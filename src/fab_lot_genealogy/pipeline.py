"""Orchestration boundaries.

Two entry points live here:

* :func:`run` — the original flat-record pipeline, preserved byte-for-byte
  in behaviour so legacy callers and fixtures keep working;
* :class:`GenealogyPipeline` — the three-layer (raw / normalized /
  queryable) replayable genealogy pipeline used by the new CLI and tests.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .graph import GraphBuilder, GraphSnapshot
from .models import CanonicalEvent, QuarantineEntry
from .normalizer import Normalizer
from .parser import load_records
from .engine import calculate
from .report import build_result
from .query import LineageQuery, LineageResult
from .raw_store import RawStore
from .snapshot import SnapshotStore
from .timeutil import far_future, parse_ts


# --------------------------------------------------------------------- legacy
def run(payload: dict) -> dict:
    """Original behaviour: flat operations -> simple ancestor summary."""
    records = load_records(payload)
    return build_result("fab-lot-genealogy", calculate(records, payload))


# --------------------------------------------------------------- new pipeline
class GenealogyPipeline:
    """Batch materialization over a directory-backed medallion store."""

    def __init__(
        self,
        base_dir: str | Path,
        equipment_aliases: Optional[Dict[str, str]] = None,
        builder: Optional[GraphBuilder] = None,
    ):
        self.raw = RawStore(base_dir)
        self.snapshots = SnapshotStore(base_dir)
        self.aliases = equipment_aliases or {}
        self.builder = builder or GraphBuilder()
        self.normalizer = Normalizer(self.aliases)

    # ------------------------------------------------------------- ingestion
    def ingest(
        self,
        source: str,
        records: List[dict],
        batch_id: str,
    ) -> dict:
        return self.raw.append_batch(source, records, batch_id)

    def ingest_legacy(self, payload: dict, batch_id: str = "legacy-0001") -> dict:
        """Adapt the original flat ``operations`` fixture into the raw layer."""
        return self.raw.append_batch("legacy", payload.get("operations", []), batch_id)

    # -------------------------------------------------------- normalization
    def normalize(self) -> tuple[List[CanonicalEvent], List[QuarantineEntry]]:
        events = self.normalizer.normalize_all(self.raw.read_all())
        return events, list(self.normalizer.quarantine)

    # -------------------------------------------------------- materialization
    def materialize(
        self,
        as_of_event: datetime | str = far_future(),
        as_of_ingest: datetime | str = far_future(),
    ) -> GraphSnapshot:
        if isinstance(as_of_event, str):
            as_of_event = parse_ts(as_of_event)
        if isinstance(as_of_ingest, str):
            as_of_ingest = parse_ts(as_of_ingest)
        events, norm_quarantine = self.normalize()
        return self.snapshots.materialize(
            events,
            as_of_event=as_of_event,
            as_of_ingest=as_of_ingest,
            prior_quarantine=norm_quarantine,
            builder=self.builder,
        )

    # ----------------------------------------------------------------- query
    def query(
        self,
        target: str,
        snapshot: Optional[GraphSnapshot] = None,
        direction: str = "ancestors",
        max_nodes: int = 10_000,
        wafer: Optional[str] = None,
    ) -> LineageResult:
        snapshot = snapshot or self.materialize()
        return LineageQuery(snapshot).lineage(
            target, direction=direction, max_nodes=max_nodes, wafer=wafer
        )

    def frozen_query(
        self,
        content_hash: str,
        target: str,
        direction: str = "ancestors",
        max_nodes: int = 10_000,
    ) -> dict:
        """Query a previously frozen snapshot by content hash.

        The snapshot is loaded from disk and re-checked for integrity,
        guaranteeing the answer cannot reflect anything written after
        materialization.
        """
        body = self.snapshots.load(content_hash)
        frozen = _HydratedSnapshot(body)
        return LineageQuery(frozen).lineage(target, direction, max_nodes).to_dict()


class _HydratedSnapshot(GraphSnapshot):
    """Minimal GraphSnapshot view reconstructed from a persisted JSON body."""

    def __init__(self, body: dict):
        from .models import GraphEdge, GraphNode, QuarantineEntry, RawRef
        from .timeutil import parse_ts as _parse

        def refs(items):
            return tuple(RawRef(**r) for r in items)

        nodes = {}
        for n in body["nodes"]:
            nodes[n["node_id"]] = GraphNode(
                node_id=n["node_id"],
                node_type=n["node_type"],
                label=n["label"],
                created_event_time=_parse(n["created_event_time"]),
                created_ingest_time=_parse(n["created_ingest_time"]),
                event_ids=tuple(n["event_ids"]),
                evidence=refs(n["evidence"]),
                detail=n.get("detail", {}),
            )
        edges = [
            GraphEdge(
                edge_id=e["edge_id"],
                edge_type=e["edge_type"],
                src_id=e["src_id"],
                dst_id=e["dst_id"],
                event_time=_parse(e["event_time"]),
                ingest_time=_parse(e["ingest_time"]),
                wafers=frozenset(e["wafers"]),
                event_id=e["event_id"],
                evidence=refs(e["evidence"]),
            )
            for e in body["edges"]
        ]
        quarantine = [
            QuarantineEntry(
                code=q["code"],
                reason=q["reason"],
                detected_at=_parse(q["detected_at"]),
                raw_id=q.get("raw_id"),
                natural_key=q.get("natural_key"),
                event_id=q.get("event_id"),
                detail=q.get("detail", {}),
                evidence=refs(q.get("evidence", [])),
            )
            for q in body["quarantine"]
        ]
        super().__init__(
            nodes=nodes,
            edges=edges,
            quarantine=quarantine,
            ledger=[],
            as_of_event=_parse(body["as_of_event"]),
            as_of_ingest=_parse(body["as_of_ingest"]),
            input_event_count=body["input_event_count"],
            budget=body.get("budget", {}),
        )
