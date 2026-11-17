"""Shared fixture factory: a full split / merge / rework / inspection chain."""
from __future__ import annotations

from fab_lot_genealogy.models import RawRecord
from fab_lot_genealogy.timeutil import parse_ts


def raw(raw_id, source, payload, received=None, batch="b") -> RawRecord:
    received = received or payload.get("ingest_at") or payload["at"]
    if not raw_id.startswith("raw_"):
        raw_id = f"raw_{raw_id}"
    return RawRecord(
        raw_id=raw_id, source=source, source_seq=0, payload=payload,
        received_at=parse_ts(received), batch_id=batch,
    )


def healthy_chain_records():
    """B0(w1..w4) -> split -> B1(w1,w2), B2(w3,w4)
    B1 rework -> B3(w1,w2); B2 merge B3 -> B4(all four)
    equipment step + inspection on B4."""
    return [
        raw("c0", "mes", {
            "type": "batch_create", "at": "2026-01-01T08:00Z",
            "batch_id": "B0", "wafers": ["w1", "w2", "w3", "w4"]}),
        raw("c1", "split", {
            "type": "split", "at": "2026-01-02T08:00Z",
            "from_batch": "B0", "to_batches": ["B1", "B2"],
            "wafers_by_child": {"B1": ["w1", "w2"], "B2": ["w3", "w4"]},
            "wafers": ["w1", "w2", "w3", "w4"]}),
        raw("c2", "mes", {
            "type": "rework", "at": "2026-01-03T08:00Z",
            "from_batch": "B1", "to_batch": "B3", "wafers": ["w1", "w2"],
            "operation": "RW-CLEAN"}),
        raw("c3", "merge", {
            "type": "merge", "at": "2026-01-04T08:00Z",
            "from_batches": ["B2", "B3"], "to_batch": "B4",
            "wafers": ["w1", "w2", "w3", "w4"]}),
        raw("c4", "equipment", {
            "type": "equipment_process", "at": "2026-01-05T08:00Z",
            "batch": "B4", "equipment_id": "EQ-7", "operation": "ETCH-1"}),
        raw("c5", "inspection", {
            "type": "inspection", "at": "2026-01-06T08:00Z", "batch": "B4",
            "inspection_id": "INSP-1", "result": "fail",
            "measurements": {"defects": 3}}),
    ]


EQUIPMENT_ALIASES = {"OLD-TOOL-7": "EQ-7"}
