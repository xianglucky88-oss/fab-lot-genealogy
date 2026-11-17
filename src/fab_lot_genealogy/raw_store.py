"""Raw layer: append-only, idempotent, recoverable landing zone.

Layout under ``base_dir``::

    raw/
      <source>.jsonl          one JSON object per line, append only
      seen_index.json         raw_id -> {file, offset} for O(1) dedupe
      batches/
        <batch_id>.json       batch manifest + per-record ack state

Writes are atomic (tmp file + os.replace) so a crash mid-batch leaves
either the old or the new state, never a torn one.  Replaying the same
payload twice appends nothing and reports the same raw ids.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .ids import stable_hash
from .models import RawRecord
from .timeutil import iso, parse_ts

RAW_VERSION = 1
VALID_SOURCES = {"mes", "split", "merge", "equipment", "inspection", "legacy"}


class RawStore:
    def __init__(self, base_dir: str | Path):
        self.base = Path(base_dir)
        self.raw_dir = self.base / "raw"
        self.batch_dir = self.raw_dir / "batches"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.batch_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.raw_dir / "seen_index.json"
        self._index = self._load_index()

    # ------------------------------------------------------------------ index
    def _load_index(self) -> Dict[str, dict]:
        if self.index_path.exists():
            return json.loads(self.index_path.read_text())
        return {"version": RAW_VERSION, "raw_ids": {}}

    def _save_index(self) -> None:
        tmp = self.index_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._index, sort_keys=True, indent=1))
        os.replace(tmp, self.index_path)

    # --------------------------------------------------------------- batches
    def batch_state(self, batch_id: str) -> Optional[dict]:
        path = self.batch_dir / f"{batch_id}.json"
        return json.loads(path.read_text()) if path.exists() else None

    def _save_batch(self, state: dict) -> None:
        path = self.batch_dir / f"{state['batch_id']}.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, sort_keys=True, indent=1))
        os.replace(tmp, path)

    # ---------------------------------------------------------------- append
    def append_batch(
        self,
        source: str,
        records: List[dict[str, Any]],
        batch_id: str,
        received_at: Optional[datetime] = None,
    ) -> dict:
        """Land one ingestion batch idempotently.

        Returns a manifest with ``accepted`` / ``duplicate`` raw ids and a
        ``failed`` list.  A partially failed batch keeps its successful
        records and records the failing offsets so a retry can resume.
        """
        if source not in VALID_SOURCES:
            raise ValueError(f"unknown source {source!r}; valid: {sorted(VALID_SOURCES)}")
        received_at = received_at or datetime.now(timezone.utc)

        state = self.batch_state(batch_id)
        if state is None:
            state = {
                "version": RAW_VERSION,
                "batch_id": batch_id,
                "source": source,
                "created_at": iso(received_at),
                "total": len(records),
                "acked": [],          # offsets durably appended
                "failed": [],         # offsets that failed validation + error
                "status": "open",
            }
        else:
            if state["source"] != source:
                raise ValueError(
                    f"batch {batch_id} already exists with source "
                    f"{state['source']!r}, cannot reuse for {source!r}"
                )

        accepted, duplicate, failed = [], [], []
        for seq, payload in enumerate(records):
            if seq in {f["offset"] for f in state["failed"]}:
                # retries get one more chance; previous failure is replaced
                state["failed"] = [f for f in state["failed"] if f["offset"] != seq]
            if seq in state["acked"]:
                continue
            if not isinstance(payload, dict):
                problem = {"offset": seq, "error": "record is not a JSON object"}
                state["failed"].append(problem)
                failed.append(problem)
                continue
            raw_id = self._raw_id(source, seq, payload)
            if raw_id in self._index["raw_ids"]:
                # identical verbatim record already landed somewhere -> idempotent skip
                state["acked"].append(seq)
                duplicate.append(raw_id)
                continue
            record = RawRecord(
                raw_id=raw_id,
                source=source,
                source_seq=seq,
                payload=payload,
                received_at=received_at,
                batch_id=batch_id,
            )
            self._append_line(source, record)
            self._index["raw_ids"][raw_id] = {
                "source": source,
                "batch_id": batch_id,
                "offset": seq,
            }
            state["acked"].append(seq)
            accepted.append(raw_id)

        state["status"] = "complete" if not state["failed"] else "partial"
        self._save_batch(state)
        self._save_index()
        return {
            "batch_id": batch_id,
            "source": source,
            "accepted": accepted,
            "duplicate": duplicate,
            "failed": failed,
            "status": state["status"],
        }

    def retry_failed(
        self, batch_id: str, offset: int, payload: dict
    ) -> dict:
        """Re-land a corrected payload for a previously failed offset.

        The failure entry is cleared only when the replacement is a valid
        object; the raw id stays content-derived, so an identical replacement
        remains idempotent.
        """
        state = self.batch_state(batch_id)
        if state is None:
            raise KeyError(f"unknown batch {batch_id}")
        if not isinstance(payload, dict):
            return {"batch_id": batch_id, "offset": offset, "status": "still_failing",
                    "error": "replacement record is not a JSON object"}
        raw_id = self._raw_id(state["source"], offset, payload)
        if raw_id not in self._index["raw_ids"]:
            record = RawRecord(
                raw_id=raw_id,
                source=state["source"],
                source_seq=offset,
                payload=payload,
                received_at=datetime.now(timezone.utc),
                batch_id=batch_id,
            )
            self._append_line(state["source"], record)
            self._index["raw_ids"][raw_id] = {
                "source": state["source"],
                "batch_id": batch_id,
                "offset": offset,
            }
        if offset not in state["acked"]:
            state["acked"].append(offset)
        state["failed"] = [f for f in state["failed"] if f["offset"] != offset]
        state["status"] = "complete" if not state["failed"] else "partial"
        self._save_batch(state)
        self._save_index()
        return {"batch_id": batch_id, "offset": offset, "status": "re-landed",
                "raw_id": raw_id}

    @staticmethod
    def _raw_id(source: str, seq: int, payload: dict) -> str:
        # Content of the record defines identity; arrival metadata does not,
        # so replaying an identical file yields identical raw ids.
        return f"raw_{stable_hash((source, payload))[:20]}"

    def _append_line(self, source: str, record: RawRecord) -> None:
        path = self.raw_dir / f"{source}.jsonl"
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record.to_dict(), sort_keys=True, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    # ----------------------------------------------------------------- reads
    def read_raw(self, source: str) -> List[RawRecord]:
        path = self.raw_dir / f"{source}.jsonl"
        if not path.exists():
            return []
        out: List[RawRecord] = []
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            out.append(
                RawRecord(
                    raw_id=obj["raw_id"],
                    source=obj["source"],
                    source_seq=obj["source_seq"],
                    payload=obj["payload"],
                    received_at=parse_ts(obj["received_at"]),
                    batch_id=obj["batch_id"],
                )
            )
        return out

    def read_all(self) -> List[RawRecord]:
        rows: List[RawRecord] = []
        for source in sorted(VALID_SOURCES):
            rows.extend(self.read_raw(source))
        return rows

    def batches(self) -> List[dict]:
        return [
            json.loads(p.read_text())
            for p in sorted(self.batch_dir.glob("*.json"))
        ]
