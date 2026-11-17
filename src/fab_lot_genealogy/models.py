"""Immutable domain contracts shared by every layer."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, FrozenSet, Optional, Tuple

from .timeutil import iso

# --- legacy contract (kept for the original CLI / tests) -------------------


@dataclass(frozen=True)
class PipelineResult:
    project: str
    data: dict[str, Any]


# --- raw layer -------------------------------------------------------------


@dataclass(frozen=True)
class RawRecord:
    """One verbatim source record as it arrived."""

    raw_id: str                       # hash of verbatim payload -> idempotent append
    source: str                       # mes | split | merge | equipment | inspection | legacy
    source_seq: int                   # monotonic position inside the source batch
    payload: dict[str, Any]           # verbatim payload (deep-frozen by construction)
    received_at: datetime             # ingestion time
    batch_id: str                     # ingestion batch / file run id

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_id": self.raw_id,
            "source": self.source,
            "source_seq": self.source_seq,
            "payload": self.payload,
            "received_at": iso(self.received_at),
            "batch_id": self.batch_id,
        }


# --- normalization layer ---------------------------------------------------


@dataclass(frozen=True)
class RawRef:
    """Pointer back to the exact raw record that produced a conclusion."""

    raw_id: str
    source: str
    batch_id: str

    def to_dict(self) -> dict[str, Any]:
        return {"raw_id": self.raw_id, "source": self.source, "batch_id": self.batch_id}


@dataclass(frozen=True)
class CanonicalEvent:
    """A normalized, source-agnostic manufacturing event."""

    event_id: str                     # hash of (natural key + correction sequence)
    natural_key: str                  # business identity: same operation re-reported
    kind: str                         # batch_create | split | merge | rework |
                                      # equipment_process | inspection | correction
    event_time: datetime
    ingest_time: datetime
    batch: Optional[str]              # owning batch of the event
    from_batches: Tuple[str, ...]
    to_batches: Tuple[str, ...]
    wafers: FrozenSet[str]
    equipment: Optional[str]          # *resolved* equipment id (post alias registry)
    equipment_ref: Optional[str]      # equipment id exactly as the source sent it
    operation: Optional[str]
    inspection: Optional[dict[str, Any]]
    valid_from: datetime              # correction validity window (event-time axis)
    valid_to: datetime
    supersedes: Optional[str]         # natural_key of the event this correction replaces
    schema_version: int
    evidence: Tuple[RawRef, ...]
    wafer_routing: Tuple[Tuple[str, Tuple[str, ...]], ...] = field(default_factory=tuple)
    extra: Tuple[Tuple[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "natural_key": self.natural_key,
            "kind": self.kind,
            "event_time": iso(self.event_time),
            "ingest_time": iso(self.ingest_time),
            "batch": self.batch,
            "from_batches": list(self.from_batches),
            "to_batches": list(self.to_batches),
            "wafers": sorted(self.wafers),
            "equipment": self.equipment,
            "equipment_ref": self.equipment_ref,
            "operation": self.operation,
            "inspection": self.inspection,
            "valid_from": iso(self.valid_from),
            "valid_to": iso(self.valid_to),
            "supersedes": self.supersedes,
            "schema_version": self.schema_version,
            "evidence": [r.to_dict() for r in self.evidence],
            "wafer_routing": {child: sorted(wafers) for child, wafers in self.wafer_routing},
            "extra": dict(self.extra),
        }


# --- graph layer -----------------------------------------------------------

NODE_TYPES = ("batch", "equipment", "inspection", "wafer")


@dataclass(frozen=True)
class GraphNode:
    node_id: str                      # stable, content-derived
    node_type: str                    # one of NODE_TYPES
    label: str                        # batch id / equipment id / inspection id
    created_event_time: datetime
    created_ingest_time: datetime
    event_ids: Tuple[str, ...]        # every event that shaped this node
    evidence: Tuple[RawRef, ...]
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "label": self.label,
            "created_event_time": iso(self.created_event_time),
            "created_ingest_time": iso(self.created_ingest_time),
            "event_ids": list(self.event_ids),
            "evidence": [r.to_dict() for r in self.evidence],
            "detail": self.detail,
        }


@dataclass(frozen=True)
class GraphEdge:
    edge_id: str
    edge_type: str                    # split | merge | rework | process | inspect
    src_id: str
    dst_id: str
    event_time: datetime
    ingest_time: datetime
    wafers: FrozenSet[str]
    event_id: str
    evidence: Tuple[RawRef, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "edge_type": self.edge_type,
            "src_id": self.src_id,
            "dst_id": self.dst_id,
            "event_time": iso(self.event_time),
            "ingest_time": iso(self.ingest_time),
            "wafers": sorted(self.wafers),
            "event_id": self.event_id,
            "evidence": [r.to_dict() for r in self.evidence],
        }


# --- quarantine ------------------------------------------------------------

QUARANTINE_CODES = (
    "missing_parent",          # parent batch was never created / observed
    "cyclic_relation",         # edge would create a cycle in the batch DAG
    "conflicting_operation",   # same operation step reports incompatible facts
    "conservation_violation",  # split/merge wafer sets do not conserve across edge
    "unknown_equipment",       # equipment id cannot be resolved through the alias registry
    "invalid_record",          # structurally unparseable source record
    "duplicate_conflict",      # same natural key re-reported with a different payload
)


@dataclass(frozen=True)
class QuarantineEntry:
    code: str
    reason: str
    detected_at: datetime
    raw_id: Optional[str]
    natural_key: Optional[str]
    event_id: Optional[str]
    detail: dict[str, Any] = field(default_factory=dict)
    evidence: Tuple[RawRef, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "reason": self.reason,
            "detected_at": iso(self.detected_at),
            "raw_id": self.raw_id,
            "natural_key": self.natural_key,
            "event_id": self.event_id,
            "detail": self.detail,
            "evidence": [r.to_dict() for r in self.evidence],
        }
