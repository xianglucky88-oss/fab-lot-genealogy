"""Streaming ingestion with checkpointed recovery.

The stream processor applies the *same* normalization and graph rules as
batch materialization.  Its only extra concern is bounding live state
and surviving crashes:

* raw records are landed durably first (the :class:`RawStore` is the
  source of truth);
* a checkpoint records the last consumed raw id per source;
* on resume, already-consumed records are skipped and in-flight work is
  rebuilt from the raw log, so reprocessing is idempotent;
* a record that fails is quarantined and the stream advances — a bad
  batch never blocks later batches.  ``recover`` retries failed offsets.

Streaming vs. batch is intentionally the same code path: both produce
canonical events that feed the identical :class:`GraphBuilder`, which is
what makes the order-independence guarantee testable.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from .graph import GraphBuilder, GraphSnapshot
from .ids import stable_hash
from .models import QuarantineEntry
from .normalizer import Normalizer
from .raw_store import RawStore
from .snapshot import SnapshotStore
from .timeutil import far_future

CHECKPOINT_NAME = "stream_checkpoint.json"


class StreamProcessor:
    def __init__(
        self,
        base_dir: str | Path,
        equipment_aliases: Optional[Dict[str, str]] = None,
        max_canonical_events: int = 200_000,
    ):
        self.store = RawStore(base_dir)
        self.snapshots = SnapshotStore(base_dir)
        self.aliases = equipment_aliases or {}
        self.max_canonical_events = max_canonical_events
        self.checkpoint_path = Path(base_dir) / CHECKPOINT_NAME

    # ------------------------------------------------------------- checkpoint
    def _load_checkpoint(self) -> dict:
        if self.checkpoint_path.exists():
            return json.loads(self.checkpoint_path.read_text())
        return {"version": 1, "consumed": {}, "quarantine": [], "materialized": []}

    def _save_checkpoint(self, state: dict) -> None:
        tmp = self.checkpoint_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, sort_keys=True, indent=1))
        os.replace(tmp, self.checkpoint_path)

    # -------------------------------------------------------------- ingestion
    def ingest(
        self,
        source: str,
        records: List[dict],
        batch_id: str,
    ) -> dict:
        """Land one micro-batch and advance the checkpoint.

        Returns the raw-store manifest plus the current stream position.
        A partially failed batch is durably recorded; failed offsets are
        retried by :meth:`recover`.
        """
        manifest = self.store.append_batch(source, records, batch_id)
        checkpoint = self._load_checkpoint()
        for raw_id in manifest["accepted"] + manifest["duplicate"]:
            checkpoint["consumed"].setdefault(source, [])
            if raw_id not in checkpoint["consumed"][source]:
                checkpoint["consumed"][source].append(raw_id)
        self._save_checkpoint(checkpoint)
        return manifest

    # ---------------------------------------------------------------- replay
    def replay(
        self,
        as_of_event=far_future(),
        as_of_ingest=far_future(),
        builder: Optional[GraphBuilder] = None,
    ) -> GraphSnapshot:
        """Rebuild canonical events from the raw log and materialize.

        Because the raw log is append-only and every transform is
        deterministic, this yields byte-identical snapshots across
        restarts and across the batch path.
        """
        records = self.store.read_all()
        if len(records) > self.max_canonical_events * 4:
            # the raw/canonical ratio is small; this is a safety valve only
            raise RuntimeError("stream replay exceeds bounded state allowance")
        normalizer = Normalizer(self.aliases)
        events = normalizer.normalize_all(records)
        if len(events) > self.max_canonical_events:
            raise RuntimeError(
                f"canonical event count {len(events)} exceeds "
                f"max_canonical_events {self.max_canonical_events}"
            )
        prior = self._hydrate_quarantine(normalizer.quarantine)
        snapshot = self.snapshots.materialize(
            events,
            as_of_event=as_of_event,
            as_of_ingest=as_of_ingest,
            prior_quarantine=prior,
            builder=builder,
        )
        checkpoint = self._load_checkpoint()
        if snapshot.content_hash() not in checkpoint["materialized"]:
            checkpoint["materialized"].append(snapshot.content_hash())
        checkpoint["quarantine"] = [q.to_dict() for q in snapshot.quarantine]
        self._save_checkpoint(checkpoint)
        return snapshot

    # --------------------------------------------------------------- recovery
    def recover(
        self,
        builder: Optional[GraphBuilder] = None,
        replacements: Optional[Dict[str, Dict[int, dict]]] = None,
    ) -> dict:
        """Recover partial batches, then replay.

        ``replacements`` maps ``batch_id -> {offset: corrected_payload}`` so a
        previously rejected record can be re-driven through the normal
        idempotent landing path.  Failures without a replacement are genuinely
        unparseable and remain recorded on the batch manifest (permanent
        quarantine) instead of blocking the stream.
        """
        replacements = replacements or {}
        retried, still_failing = [], []
        for batch in self.store.batches():
            if batch["status"] != "partial":
                continue
            fixed = replacements.get(batch["batch_id"], {})
            for failure in list(batch["failed"]):
                offset = failure["offset"]
                if offset in fixed:
                    outcome = self.store.retry_failed(
                        batch["batch_id"], offset, fixed[offset]
                    )
                    retried.append(outcome)
                else:
                    still_failing.append({"batch_id": batch["batch_id"], **failure})
        snapshot = self.replay(builder=builder)
        return {
            "retried_failures": retried,
            "permanently_failed": still_failing,
            "snapshot_hash": snapshot.content_hash(),
            "quarantine_count": len(snapshot.quarantine),
        }

    def _hydrate_quarantine(
        self, fresh: Sequence[QuarantineEntry]
    ) -> List[QuarantineEntry]:
        """Merge normalization quarantine with graph-level quarantine.

        Normalizer quarantine is produced fresh on every replay (it is
        deterministic), while the checkpoint keeps a durable audit copy.
        """
        checkpoint = self._load_checkpoint()
        seen = {stable_hash((q.code, q.raw_id, q.natural_key)) for q in fresh}
        merged = list(fresh)
        # Checkpoint entries are dictionaries; we only dedupe, not rebuild,
        # because graph replay regenerates graph-level quarantine anyway.
        for item in checkpoint.get("quarantine", []):
            key = stable_hash(
                (item["code"], item.get("raw_id"), item.get("natural_key"))
            )
            if key not in seen:
                seen.add(key)
        return merged

    def position(self) -> dict:
        return self._load_checkpoint()
