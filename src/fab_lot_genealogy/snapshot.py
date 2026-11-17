"""Frozen snapshot materialization and catalog.

A snapshot is an immutable, self-describing artifact: once written it can
never observe data ingested afterwards, because its bitemporal
``as_of_ingest`` ceiling and full content hash are baked into the
manifest.  Writes are atomic and snapshots are content-addressed, so
re-materializing the same inputs overwrites the same file with the same
bytes (idempotent replay).
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence

from .graph import GraphBuilder, GraphSnapshot
from .ids import stable_hash
from .models import CanonicalEvent, QuarantineEntry
from .timeutil import far_future, iso

SNAPSHOT_VERSION = 1


class SnapshotStore:
    def __init__(self, base_dir: str | Path):
        self.base = Path(base_dir)
        self.dir = self.base / "snapshots"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.catalog_path = self.dir / "catalog.json"

    # ----------------------------------------------------------- materialize
    def materialize(
        self,
        events: Sequence[CanonicalEvent],
        as_of_event: datetime = far_future(),
        as_of_ingest: datetime = far_future(),
        prior_quarantine: Optional[Sequence[QuarantineEntry]] = None,
        builder: Optional[GraphBuilder] = None,
    ) -> GraphSnapshot:
        builder = builder or GraphBuilder()
        snapshot = builder.build(
            events,
            as_of_event=as_of_event,
            as_of_ingest=as_of_ingest,
            prior_quarantine=prior_quarantine,
        )
        self._write(snapshot)
        self._update_catalog(snapshot)
        return snapshot

    def _write(self, snapshot: GraphSnapshot) -> None:
        body = {"version": SNAPSHOT_VERSION, **snapshot.to_dict()}
        name = f"{snapshot.content_hash()}.json"
        path = self.dir / name
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(body, sort_keys=True, indent=1, ensure_ascii=False))
        os.replace(tmp, path)

    def _update_catalog(self, snapshot: GraphSnapshot) -> None:
        catalog = self.catalog()
        entry = {
            "content_hash": snapshot.content_hash(),
            "as_of_event": iso(snapshot.as_of_event),
            "as_of_ingest": iso(snapshot.as_of_ingest),
            "node_count": len(snapshot.nodes),
            "edge_count": len(snapshot.edges),
            "quarantine_count": len(snapshot.quarantine),
        }
        by_hash = {c["content_hash"]: c for c in catalog}
        by_hash[entry["content_hash"]] = entry
        tmp = self.catalog_path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps({"version": SNAPSHOT_VERSION,
                        "snapshots": sorted(by_hash.values(), key=lambda c: c["content_hash"])},
                       sort_keys=True, indent=1)
        )
        os.replace(tmp, self.catalog_path)

    # ------------------------------------------------------------------ read
    def catalog(self) -> List[dict]:
        if self.catalog_path.exists():
            return json.loads(self.catalog_path.read_text()).get("snapshots", [])
        return []

    def load(self, content_hash: str) -> dict:
        path = self.dir / f"{content_hash}.json"
        if not path.exists():
            raise KeyError(f"snapshot {content_hash} not found")
        body = json.loads(path.read_text())
        if self._content_hash_of(body) != content_hash:
            raise RuntimeError(f"snapshot {content_hash} failed integrity check")
        return body

    @staticmethod
    def _content_hash_of(body: dict) -> str:
        # Must match GraphSnapshot.content_hash: ingest-time fields and batch
        # names are excluded so arrival regrouping cannot change lineage.
        def evidence_ids(refs):
            return tuple(sorted((r["raw_id"], r["source"]) for r in refs))

        def node_key(n):
            return (
                n["node_id"], n["node_type"], n["label"],
                n["created_event_time"],
                tuple(n["event_ids"]),
                evidence_ids(n["evidence"]),
                tuple(sorted((k, str(v)) for k, v in n.get("detail", {}).items())),
            )

        def edge_key(e):
            return (
                e["edge_id"], e["edge_type"], e["src_id"], e["dst_id"],
                e["event_time"], tuple(sorted(e["wafers"])), e["event_id"],
                evidence_ids(e["evidence"]),
            )

        payload = {
            "nodes": sorted(node_key(n) for n in body["nodes"]),
            "edges": sorted(edge_key(e) for e in body["edges"]),
            "quarantine": sorted(
                (q["code"], q.get("raw_id") or "",
                 q.get("natural_key") or "", q["reason"])
                for q in body["quarantine"]
            ),
        }
        return stable_hash(payload)
