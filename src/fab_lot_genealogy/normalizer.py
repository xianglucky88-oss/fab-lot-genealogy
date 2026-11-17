"""Normalization layer.

Turns verbatim :class:`RawRecord` payloads from five MES-adjacent sources
(plus the legacy fixture format) into source-agnostic
:class:`CanonicalEvent` objects.

Responsibilities
----------------
* accept old and new schema versions without touching old batches;
* resolve equipment ids through an alias registry while keeping the raw
  reference for evidence;
* derive a business *natural key* so repeated reports of the same
  operation are detectable (dedupe vs. conflicting duplicate);
* represent historical corrections as first-class events carrying an
  event-time validity window;
* send anything unexplainable to quarantine instead of dropping it
  silently.
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

from .ids import content_id, natural_key, stable_hash
from .models import (
    CanonicalEvent,
    QuarantineEntry,
    RawRecord,
    RawRef,
)
from .timeutil import far_future, iso, parse_ts

CURRENT_SCHEMA = 2
STRUCTURAL_KINDS = {"batch_create", "split", "merge", "rework"}


class NormalizeError(ValueError):
    """Raised for records that cannot be interpreted at all."""


def _ref(record: RawRecord) -> RawRef:
    return RawRef(raw_id=record.raw_id, source=record.source, batch_id=record.batch_id)


def _wafers(payload: dict) -> frozenset:
    if "wafers" in payload:
        value = payload["wafers"]
        if not isinstance(value, list):
            raise NormalizeError("wafers must be a list")
        return frozenset(str(w) for w in value)
    if "wafer" in payload:
        return frozenset({str(payload["wafer"])})
    if "wafers_by_child" in payload:
        wbc = payload["wafers_by_child"]
        if not isinstance(wbc, dict):
            raise NormalizeError("wafers_by_child must be an object")
        out = set()
        for ws in wbc.values():
            if not isinstance(ws, list):
                raise NormalizeError("wafers_by_child values must be lists")
            out.update(str(w) for w in ws)
        return frozenset(out)
    return frozenset()


def _wafer_routing(
    payload: dict, to_batches: Tuple[str, ...], wafers: frozenset
) -> Tuple[Tuple[str, Tuple[str, ...]], ...]:
    """Per-destination wafer assignment used by the ownership ledger."""
    if isinstance(payload.get("wafers_by_child"), dict):
        routing = tuple(
            (str(child), tuple(str(w) for w in ws))
            for child, ws in sorted(payload["wafers_by_child"].items())
        )
    elif len(to_batches) == 1:
        # single-child split, rework, merge: everything goes to that child
        routing = ((to_batches[0], tuple(sorted(wafers))),)
    elif to_batches:
        # No explicit routing: each destination receives the whole set
        # (legal for rework-style fan-out; conservation is still enforced).
        routing = tuple((child, tuple(sorted(wafers))) for child in to_batches)
    else:
        routing = ()
    return routing


def _batches(payload: dict) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Return (from_batches, to_batches) for a structural payload."""
    froms, tos = [], []
    if "from_batch" in payload and payload["from_batch"] is not None:
        froms.append(str(payload["from_batch"]))
    if "from_batches" in payload and payload["from_batches"] is not None:
        if not isinstance(payload["from_batches"], list):
            raise NormalizeError("from_batches must be a list")
        froms.extend(str(b) for b in payload["from_batches"])
    if "to_batch" in payload and payload["to_batch"] is not None:
        tos.append(str(payload["to_batch"]))
    if "to_batches" in payload and payload["to_batches"] is not None:
        if not isinstance(payload["to_batches"], list):
            raise NormalizeError("to_batches must be a list")
        tos.extend(str(b) for b in payload["to_batches"])
    return tuple(froms), tuple(tos)


def key_fields(kind: str, payload: dict, event_time: datetime) -> tuple:
    """Business identity of an operation.

    Re-reported copies carry the same key; genuinely different operations
    differ in at least one component.  Event time is part of the key so
    that the same step repeated later in the day stays distinct.
    """
    if kind == "batch_create":
        return ("create", str(payload["batch_id"]), iso(event_time))
    if kind == "split":
        froms, tos = _batches(payload)
        return ("split", tuple(sorted(froms)), tuple(sorted(tos)), iso(event_time))
    if kind == "merge":
        froms, tos = _batches(payload)
        return ("merge", tuple(sorted(froms)), tuple(sorted(tos)), iso(event_time))
    if kind == "rework":
        froms, tos = _batches(payload)
        return ("rework", tuple(sorted(froms)), tuple(sorted(tos)), iso(event_time))
    if kind == "equipment_process":
        return (
            "equip",
            str(payload.get("batch")),
            payload.get("operation"),
            payload.get("equipment_ref"),
            iso(event_time),
        )
    if kind == "inspection":
        return (
            "inspect",
            str(payload.get("batch")),
            payload.get("inspection_id"),
            iso(event_time),
        )
    raise NormalizeError(f"cannot build natural key for kind {kind!r}")


# --------------------------------------------------------------------- kind


def _detect_kind(source: str, payload: dict) -> str:
    ptype = payload.get("type")
    if ptype in {
        "batch_create",
        "split",
        "merge",
        "rework",
        "equipment_process",
        "inspection",
        "correction",
    }:
        return ptype
    # legacy fixture shape: {"kind": "split", ...}
    legacy = payload.get("kind")
    if legacy in {"split", "merge", "rework"}:
        return legacy
    if source == "mes":
        return "batch_create"
    if source == "equipment":
        return "equipment_process"
    if source == "inspection":
        return "inspection"
    if source == "split":
        return "split"
    if source == "merge":
        return "merge"
    raise NormalizeError(f"cannot determine event kind from payload {payload!r}")


# ------------------------------------------------------------- equipment id


def resolve_equipment(
    payload: dict, schema_version: int, aliases: Dict[str, str]
) -> Tuple[Optional[str], Optional[str]]:
    """Return (resolved canonical id, raw reference).

    Schema evolution: v1 used ``equipment``; v2 renamed it to
    ``equipment_id`` and may additionally carry ``tool_ref``.  Both must
    keep resolving for old batches.
    """
    ref = None
    for field_name in ("equipment_id", "equipment", "tool_ref"):
        if payload.get(field_name):
            ref = str(payload[field_name])
            break
    if ref is None:
        return None, None
    return aliases.get(ref, ref), ref


# ----------------------------------------------------------------- builder


class Normalizer:
    def __init__(self, equipment_aliases: Optional[Dict[str, str]] = None):
        self.aliases = dict(equipment_aliases or {})
        self.quarantine: List[QuarantineEntry] = []

    def normalize_all(
        self, records: Sequence[RawRecord]
    ) -> List[CanonicalEvent]:
        # A Normalizer may be reused across materializations; quarantine must
        # describe exactly this run, never accumulate from previous calls.
        self.quarantine = []
        events: List[CanonicalEvent] = []
        # Deterministic ingestion order independent of filesystem listing.
        for record in sorted(
            records, key=lambda r: (r.received_at, r.source, r.batch_id, r.source_seq, r.raw_id)
        ):
            try:
                events.extend(self.normalize_one(record))
            except NormalizeError as exc:
                self.quarantine.append(
                    QuarantineEntry(
                        code="invalid_record",
                        reason=str(exc),
                        detected_at=record.received_at,
                        raw_id=record.raw_id,
                        natural_key=None,
                        event_id=None,
                        detail={"source": record.source},
                        evidence=(_ref(record),),
                    )
                )
        return self._dedupe(events)

    # ------------------------------------------------------------- one record
    def normalize_one(self, record: RawRecord) -> List[CanonicalEvent]:
        payload = record.payload
        schema_version = int(payload.get("schema_version", 1))
        if schema_version > CURRENT_SCHEMA:
            # Forward compatibility: unknown fields survive in `extra`.
            pass
        kind = _detect_kind(record.source, payload)
        event_time = parse_ts(payload["at"])
        ingest_time = (
            parse_ts(payload["ingest_at"]) if payload.get("ingest_at") else record.received_at
        )

        if kind == "correction":
            return [self._normalize_correction(record, event_time, ingest_time, schema_version)]

        nkey = natural_key(key_fields(kind, payload, event_time))
        equipment, equipment_ref = resolve_equipment(payload, schema_version, self.aliases)
        froms, tos = _batches(payload)
        batch = payload.get("batch_id") or payload.get("batch")
        if batch is not None:
            batch = str(batch)
        inspection = None
        if kind == "inspection":
            inspection = {
                "inspection_id": payload.get("inspection_id"),
                "result": payload.get("result"),
                "measurements": payload.get("measurements", {}),
            }

        known_fields = {
            "type", "kind", "at", "ingest_at", "schema_version",
            "from_batch", "from_batches", "to_batch", "to_batches",
            "wafer", "wafers", "wafers_by_child", "batch_id", "batch",
            "equipment", "equipment_id", "tool_ref", "operation",
            "inspection_id", "result", "measurements",
        }
        extra = tuple(sorted((k, v) for k, v in payload.items() if k not in known_fields))

        event = CanonicalEvent(
            event_id=content_id("evt", (nkey, None)),
            natural_key=nkey,
            kind=kind,
            event_time=event_time,
            ingest_time=ingest_time,
            batch=batch,
            from_batches=froms,
            to_batches=tos,
            wafers=_wafers(payload),
            equipment=equipment,
            equipment_ref=equipment_ref,
            operation=payload.get("operation"),
            inspection=inspection,
            valid_from=event_time,
            valid_to=far_future(),
            supersedes=None,
            schema_version=schema_version,
            evidence=(_ref(record),),
            wafer_routing=_wafer_routing(payload, tos, _wafers(payload)),
            extra=extra,
        )
        self._validate(event, record, equipment_ref)
        return [event]

    def _normalize_correction(
        self,
        record: RawRecord,
        event_time: datetime,
        ingest_time: datetime,
        schema_version: int,
    ) -> CanonicalEvent:
        payload = record.payload
        body = payload.get("event")
        if not isinstance(body, dict):
            raise NormalizeError("correction requires a full corrected 'event' body")
        target_key_fields = payload.get("corrects")
        if not isinstance(target_key_fields, dict):
            raise NormalizeError("correction requires a 'corrects' natural-key object")
        original_time = parse_ts(target_key_fields.get("at", body["at"]))
        body_kind = _detect_kind(record.source, body)
        # The target natural key is rebuilt from the *original* operation's
        # business identity. Resolution order:
        #   1. an explicit `key` tuple in `corrects`;
        #   2. an `original_identity` object carrying the old from/to/step
        #      fields (the corrected body itself has the *new* fields);
        #   3. the corrected body, which works when the identity fields did
        #      not change (e.g. an inspection result revision).
        if "key" in target_key_fields:
            supersedes = natural_key(tuple(target_key_fields["key"]))
        else:
            identity = dict(body)
            identity.update(
                {k: v for k, v in target_key_fields.items() if k != "at"}
            )
            original_identity = target_key_fields.get("original_identity")
            if isinstance(original_identity, dict):
                identity.update(original_identity)
            supersedes = natural_key(
                key_fields(body_kind, identity, original_time)
            )

        valid_from = parse_ts(payload["valid_from"]) if payload.get("valid_from") else parse_ts(body["at"])
        valid_to = parse_ts(payload["valid_to"]) if payload.get("valid_to") else far_future()

        nkey = natural_key(
            ("correction", supersedes, iso(valid_from), iso(valid_to), iso(ingest_time))
        )
        equipment, equipment_ref = resolve_equipment(body, schema_version, self.aliases)
        froms, tos = _batches(body)
        corrected = CanonicalEvent(
            event_id=content_id("evt", (nkey, None)),
            natural_key=nkey,
            kind=body_kind,
            event_time=original_time,
            ingest_time=ingest_time,
            batch=str(body["batch_id"]) if body.get("batch_id") is not None else body.get("batch"),
            from_batches=froms,
            to_batches=tos,
            wafers=_wafers(body),
            equipment=equipment,
            equipment_ref=equipment_ref,
            operation=body.get("operation"),
            inspection=(
                {
                    "inspection_id": body.get("inspection_id"),
                    "result": body.get("result"),
                    "measurements": body.get("measurements", {}),
                }
                if body_kind == "inspection"
                else None
            ),
            valid_from=valid_from,
            valid_to=valid_to,
            supersedes=supersedes,
            schema_version=schema_version,
            evidence=(_ref(record),),
            wafer_routing=_wafer_routing(body, tos, _wafers(body)),
        )
        self._validate(corrected, record, equipment_ref)
        return corrected

    # ------------------------------------------------------------- validation
    def _validate(
        self, event: CanonicalEvent, record: RawRecord, equipment_ref: Optional[str]
    ) -> None:
        if event.kind in STRUCTURAL_KINDS:
            if not event.from_batches and event.kind != "batch_create":
                raise NormalizeError(f"{event.kind} event needs at least one from_batch")
            if not event.to_batches and event.kind != "batch_create":
                raise NormalizeError(f"{event.kind} event needs at least one to_batch")
            if not event.wafers:
                raise NormalizeError(f"{event.kind} event needs wafers")
        if event.kind == "batch_create" and not event.batch:
            raise NormalizeError("batch_create needs batch_id")
        if event.kind == "equipment_process" and not event.batch:
            raise NormalizeError("equipment_process needs a batch")
        if event.kind == "inspection" and (not event.batch or event.inspection is None):
            raise NormalizeError("inspection needs a batch and inspection payload")
        if equipment_ref is not None and event.equipment is None:
            self.quarantine.append(
                QuarantineEntry(
                    code="unknown_equipment",
                    reason=f"equipment reference {equipment_ref!r} has no alias and no direct id",
                    detected_at=record.received_at,
                    raw_id=record.raw_id,
                    natural_key=event.natural_key,
                    event_id=event.event_id,
                    detail={"equipment_ref": equipment_ref},
                    evidence=(_ref(record),),
                )
            )
        elif equipment_ref is not None and self.aliases.get(equipment_ref) is None and event.equipment == equipment_ref:
            # Direct, un-aliased id is fine; only unresolved names are quarantined above.
            pass

    # ----------------------------------------------------------------- dedupe
    def _dedupe(
        self, events: List[CanonicalEvent]
    ) -> List[CanonicalEvent]:
        """Collapse repeated reports of the same natural key.

        Identical content -> one event (evidence refs are merged so every
        raw report remains traceable).  Different content -> the earliest
        deterministic report wins, the loser goes to quarantine.
        """
        chosen: Dict[str, CanonicalEvent] = {}
        order = sorted(
            events, key=lambda e: (e.ingest_time, e.evidence[0].raw_id)
        )
        for event in order:
            existing = chosen.get(event.natural_key)
            if existing is None:
                chosen[event.natural_key] = event
                continue
            if _same_facts(existing, event):
                merged = CanonicalEvent(
                    **{**existing.__dict__,
                       "evidence": tuple(sorted(
                           {*existing.evidence, *event.evidence},
                           key=lambda r: r.raw_id,
                       ))}
                )
                chosen[event.natural_key] = merged
            else:
                self.quarantine.append(
                    QuarantineEntry(
                        code="duplicate_conflict",
                        reason=(
                            f"natural key {event.natural_key} re-reported with "
                            "different payload; keeping earliest report"
                        ),
                        detected_at=event.ingest_time,
                        raw_id=event.evidence[0].raw_id,
                        natural_key=event.natural_key,
                        event_id=event.event_id,
                        detail={
                            "kept_event_id": existing.event_id,
                            "rejected_event_id": event.event_id,
                            "kept_facts": _fact_hash(existing),
                            "rejected_facts": _fact_hash(event),
                        },
                        evidence=event.evidence,
                    )
                )
        # Deterministic canonical ordering: event time first, then natural key,
        # so callers always observe the same sequence regardless of arrival.
        return sorted(
            chosen.values(),
            key=lambda e: (e.event_time, e.natural_key),
        )


def _fact_hash(event: CanonicalEvent) -> str:
    return stable_hash(
        (
            event.kind,
            iso(event.event_time),
            event.from_batches,
            event.to_batches,
            sorted(event.wafers),
            event.equipment,
            event.operation,
            event.inspection,
        )
    )


def _same_facts(a: CanonicalEvent, b: CanonicalEvent) -> bool:
    return _fact_hash(a) == _fact_hash(b)
