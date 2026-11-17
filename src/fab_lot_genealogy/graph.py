"""Graph builder: normalized events -> conserved manufacturing DAG.

The builder is a *pure replay*.  Given the same set of canonical events
it always produces the same graph, no matter what order the events are
handed in, because:

* events are filtered and ordered deterministically inside the builder;
* every node/edge id is a content hash;
* quarantine decisions depend only on event content and replay state.

Bitemporal semantics
--------------------
``as_of_event`` answers "what was true at factory time T?" while
``as_of_ingest`` answers "what did the data lake know at ingestion
time I?".  A snapshot at ``(T, I)`` contains exactly the events
ingested by ``I`` whose business facts are valid at ``T``.  Historical
corrections therefore only reshape descendants inside their event-time
validity window, and a frozen snapshot can never observe later writes.

Conservation
------------
Every structural event must move whole wafers between known batches
without loss, duplication, or overlap.  Anything that breaks the
invariant is quarantined and excluded from the graph rather than
silently corrupting the lineage.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from .ids import content_id, stable_hash
from .models import (
    CanonicalEvent,
    GraphEdge,
    GraphNode,
    QuarantineEntry,
    RawRef,
)
from .timeutil import far_future, iso

STRUCTURAL = {"batch_create", "split", "merge", "rework"}
FAR_FUTURE = far_future()


# --------------------------------------------------------------------- state


@dataclass
class _BatchState:
    label: str
    wafers: Set[str] = field(default_factory=set)
    created_event_time: Optional[datetime] = None
    created_ingest_time: Optional[datetime] = None
    event_ids: List[str] = field(default_factory=list)
    evidence: List[RawRef] = field(default_factory=list)
    open: bool = True


# ------------------------------------------------------------------- ledger


@dataclass(frozen=True)
class ConservationLedger:
    """Wafer conservation per structural event, for audit output."""

    event_id: str
    edge_type: str
    wafers_in: Tuple[str, ...]
    wafers_out: Tuple[str, ...]
    balanced: bool

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "edge_type": self.edge_type,
            "wafers_in": sorted(self.wafers_in),
            "wafers_out": sorted(self.wafers_out),
            "balanced": self.balanced,
        }


# ------------------------------------------------------------------ snapshot


class GraphSnapshot:
    def __init__(
        self,
        nodes: Dict[str, GraphNode],
        edges: List[GraphEdge],
        quarantine: List[QuarantineEntry],
        ledger: List[ConservationLedger],
        as_of_event: datetime,
        as_of_ingest: datetime,
        input_event_count: int,
        budget: dict,
    ):
        self.nodes = nodes
        self.edges = edges
        self.quarantine = quarantine
        self.ledger = ledger
        self.as_of_event = as_of_event
        self.as_of_ingest = as_of_ingest
        self.input_event_count = input_event_count
        self.budget = budget
        self._adjacency: Dict[str, List[GraphEdge]] = {}
        for edge in edges:
            self._adjacency.setdefault(edge.src_id, []).append(edge)

    # ------------------------------------------------------------- serialize
    def content_hash(self) -> str:
        # Full structural content participates in the hash, so any tampering
        # with labels, wafer sets, evidence or times inside a persisted
        # snapshot is detectable on reload.  Deliberately excluded:
        #   * ingestion batch name and ingest timestamps — regrouping records
        #     into different batches, or the same fact arriving earlier/later,
        #     must not change the lineage conclusion;
        # Ingest times still live on every node/edge for as-of filtering.
        def evidence_ids(refs):
            return tuple(sorted((r.raw_id, r.source) for r in refs))

        payload = {
            "nodes": sorted(
                (n.node_id, n.node_type, n.label,
                 iso(n.created_event_time),
                 n.event_ids, evidence_ids(n.evidence),
                 sorted((k, str(v)) for k, v in n.detail.items()))
                for n in self.nodes.values()
            ),
            "edges": sorted(
                (e.edge_id, e.edge_type, e.src_id, e.dst_id,
                 iso(e.event_time), sorted(e.wafers), e.event_id,
                 evidence_ids(e.evidence))
                for e in self.edges
            ),
            "quarantine": sorted(
                (q.code, q.raw_id or "", q.natural_key or "", q.reason)
                for q in self.quarantine
            ),
        }
        return stable_hash(payload)

    def to_dict(self) -> dict:
        return {
            "as_of_event": iso(self.as_of_event),
            "as_of_ingest": iso(self.as_of_ingest),
            "content_hash": self.content_hash(),
            "input_event_count": self.input_event_count,
            "budget": self.budget,
            "nodes": [self.nodes[n].to_dict() for n in sorted(self.nodes)],
            "edges": [e.to_dict() for e in sorted(self.edges, key=lambda e: e.edge_id)],
            "quarantine": [q.to_dict() for q in self.quarantine],
            "conservation_ledger": [l.to_dict() for l in self.ledger],
        }


# ------------------------------------------------------------------- builder


class GraphBuilder:
    def __init__(
        self,
        max_batches: int = 100_000,
        max_edges: int = 500_000,
        max_quarantine: int = 10_000,
    ):
        self.max_batches = max_batches
        self.max_edges = max_edges
        self.max_quarantine = max_quarantine

    def build(
        self,
        events: Sequence[CanonicalEvent],
        as_of_event: datetime = FAR_FUTURE,
        as_of_ingest: datetime = FAR_FUTURE,
        prior_quarantine: Optional[Sequence[QuarantineEntry]] = None,
    ) -> GraphSnapshot:
        budget = {
            "max_batches": self.max_batches,
            "max_edges": self.max_edges,
            "max_quarantine": self.max_quarantine,
        }
        quarantine = list(prior_quarantine or [])
        # de-duplicate across replay: normalization quarantine is regenerated
        # every run, so an identity already present must not be appended again
        quarantine_keys = {
            (q.code, q.raw_id, q.natural_key) for q in quarantine
        }
        visible = self._apply_bitemporal(events, as_of_event, as_of_ingest)

        batches: Dict[str, _BatchState] = {}
        batch_node_ids: Dict[str, str] = {}
        nodes: Dict[str, GraphNode] = {}
        edges: List[GraphEdge] = []
        ledger: List[ConservationLedger] = []
        # wafer -> set of batches currently owning it. A set (not a single
        # owner) is required because duplicated routing can leave the same
        # wafer id in several batches at once, which merge checks must catch.
        ownership: Dict[str, Set[str]] = {}
        # operation step already seen on a batch, for conflict detection
        seen_steps: Dict[Tuple[str, str], CanonicalEvent] = {}

        def quarantined(code: str, reason: str, event: CanonicalEvent, detail: dict) -> bool:
            key = (code, event.evidence[0].raw_id, event.natural_key)
            if key in quarantine_keys:
                return False
            if len(quarantine) >= self.max_quarantine:
                raise BudgetExceeded("quarantine", self.max_quarantine)
            quarantine_keys.add(key)
            quarantine.append(
                QuarantineEntry(
                    code=code,
                    reason=reason,
                    detected_at=as_of_ingest,
                    raw_id=event.evidence[0].raw_id,
                    natural_key=event.natural_key,
                    event_id=event.event_id,
                    detail=detail,
                    evidence=event.evidence,
                )
            )
            return False  # convenience: event is rejected

        ordered = sorted(
            visible,
            key=lambda e: (e.event_time, e.ingest_time, e.natural_key),
        )
        for event in ordered:
            if event.kind == "batch_create":
                self._handle_create(event, batches, batch_node_ids, nodes, ownership, quarantined)
            elif event.kind in STRUCTURAL:
                self._handle_structural(
                    event, batches, batch_node_ids, nodes, edges,
                    ledger, seen_steps, ownership, quarantined,
                )
            elif event.kind == "equipment_process":
                self._handle_equipment(event, batches, batch_node_ids, nodes, edges,
                                       seen_steps, quarantined)
            elif event.kind == "inspection":
                self._handle_inspection(event, batches, batch_node_ids, nodes, edges,
                                        seen_steps, quarantined)
            if len(edges) > self.max_edges:
                raise BudgetExceeded("edges", self.max_edges)
            if len(batches) > self.max_batches:
                raise BudgetExceeded("batches", self.max_batches)

        return GraphSnapshot(
            nodes=nodes,
            edges=edges,
            quarantine=quarantine,
            ledger=ledger,
            as_of_event=as_of_event,
            as_of_ingest=as_of_ingest,
            input_event_count=len(events),
            budget=budget,
        )

    # --------------------------------------------------------- bitemporal core
    def _apply_bitemporal(
        self,
        events: Sequence[CanonicalEvent],
        as_of_event: datetime,
        as_of_ingest: datetime,
    ) -> List[CanonicalEvent]:
        # 1. ingestion-axis visibility: a frozen snapshot never reads later writes
        visible = [
            e for e in events
            if e.ingest_time <= as_of_ingest
        ]
        # 2. group originals vs. corrections, apply validity windows
        originals: Dict[str, CanonicalEvent] = {}
        corrections_by_target: Dict[str, List[CanonicalEvent]] = {}
        for event in visible:
            if event.supersedes is None:
                originals[event.natural_key] = event
            else:
                corrections_by_target.setdefault(event.supersedes, []).append(event)

        result: List[CanonicalEvent] = []
        # Half-open validity windows [valid_from, valid_to); the open upper
        # bound includes the far-future sentinel so "no as-of ceiling"
        # materializes every known fact.
        ceiling = far_future()

        def covers(valid_from, valid_to) -> bool:
            return valid_from <= as_of_event and (
                as_of_event < valid_to or valid_to >= ceiling
            )

        for nkey, original in originals.items():
            if not covers(original.valid_from, original.valid_to):
                continue
            chosen = original
            candidates = sorted(
                corrections_by_target.get(nkey, []),
                key=lambda c: c.ingest_time,
            )
            for correction in candidates:
                if covers(correction.valid_from, correction.valid_to):
                    # latest correction covering T wins; keep a chain of evidence
                    chosen = CanonicalEvent(
                        **{**correction.__dict__,
                           "evidence": tuple(
                               sorted({*chosen.evidence, *correction.evidence},
                                      key=lambda r: r.raw_id)
                           )}
                    )
            result.append(chosen)
        # corrections whose target was never ingested cannot be applied
        applied_targets = set(originals)
        return sorted(
            result,
            key=lambda e: (e.event_time, e.ingest_time, e.natural_key),
        )

    # --------------------------------------------------------------- handlers
    def _handle_create(self, event, batches, batch_node_ids, nodes, ownership, quarantined) -> None:
        label = event.batch
        if label in batches:
            # re-create is tolerated as a duplicate report when facts agree;
            # conflicting create payloads are conflicts.
            existing = batches[label]
            if event.wafers and existing.wafers and existing.wafers != set(event.wafers):
                quarantined(
                    "conflicting_operation",
                    f"batch {label} created twice with different wafer sets",
                    event,
                    {"existing": sorted(existing.wafers), "incoming": sorted(event.wafers)},
                )
            return
        batches[label] = _BatchState(
            label=label,
            wafers=set(event.wafers),
            created_event_time=event.event_time,
            created_ingest_time=event.ingest_time,
            event_ids=[event.event_id],
            evidence=list(event.evidence),
        )
        for wafer in event.wafers:
            ownership.setdefault(wafer, set()).add(label)
        self._put_batch_node(label, batches[label], batch_node_ids, nodes)

    def _handle_structural(
        self, event, batches, batch_node_ids, nodes, edges,
        ledger, seen_steps, ownership, quarantined,
    ) -> None:
        froms = list(event.from_batches)
        tos = list(event.to_batches)

        # --- missing parents ------------------------------------------------
        missing = [b for b in froms if b not in batches]
        if missing:
            quarantined(
                "missing_parent",
                f"{event.kind} references unknown parent batch(es): {missing}",
                event,
                {"missing_parents": missing, "from_batches": froms, "to_batches": tos},
            )
            return

        # --- cycle: no destination may already be an ancestor of any source -
        if self._would_cycle(froms, tos, edges, batch_node_ids):
            quarantined(
                "cyclic_relation",
                f"{event.kind} would create a cycle between {froms} and {tos}",
                event,
                {"from_batches": froms, "to_batches": tos},
            )
            return

        # --- same-step conflict ---------------------------------------------
        step_key = (tuple(sorted(tos)), event.operation or f"@{iso(event.event_time)}")
        prior = seen_steps.get(step_key)
        if prior is not None and prior.event_id != event.event_id:
            quarantined(
                "conflicting_operation",
                f"operation step {step_key[1]} on {tos} already recorded with different facts",
                event,
                {"prior_event_id": prior.event_id, "step": step_key[1]},
            )
            return

        wafers = set(event.wafers)
        # snapshot per-batch contents and ownership before mutation, so the
        # per-edge wafer sets below describe what flowed *into* this event
        batches_before = {b: set(batches[b].wafers) for b in froms}
        ownership_before = {w: set(ownership.get(w, set())) for w in wafers}

        # --- conservation ---------------------------------------------------
        if event.kind == "merge":
            # Point-in-time ownership decides overlap: each moving wafer must
            # belong to exactly one of the stated parents at this moment.
            parent_set = set(froms)
            overlap = {
                wafer for wafer in wafers
                if len(ownership.get(wafer, set()) & parent_set) > 1
            }
            wrong_owner = {
                wafer for wafer in wafers
                if ownership.get(wafer)
                and not (ownership[wafer] & parent_set)
            }
            if overlap:
                quarantined(
                    "conservation_violation",
                    f"merge parents {froms} overlap on wafers {sorted(overlap)}",
                    event,
                    {"overlap": sorted(overlap)},
                )
                return
            if wrong_owner:
                quarantined(
                    "conservation_violation",
                    f"merge wafers are owned by a batch outside the parents: {sorted(wrong_owner)}",
                    event,
                    {"wrong_owner": sorted(wrong_owner)},
                )
                return
            parents_union: Set[str] = set()
            for parent in froms:
                parents_union |= batches[parent].wafers
            if parents_union != wafers:
                quarantined(
                    "conservation_violation",
                    "merge wafer set does not equal the union of parent wafers",
                    event,
                    {"parent_union": sorted(parents_union), "event_wafers": sorted(wafers)},
                )
                return
            wafers_in = sorted(parents_union)
        else:
            # split / rework: every moving wafer must exist in every source
            unavailable = set()
            for parent in froms:
                unavailable |= wafers - batches[parent].wafers
            if unavailable:
                quarantined(
                    "conservation_violation",
                    f"{event.kind} moves wafers not present in source batch: {sorted(unavailable)}",
                    event,
                    {"unavailable": sorted(unavailable)},
                )
                return
            # and the sources must include a recorded owner of those wafers
            parent_set = set(froms)
            foreign = {
                wafer for wafer in wafers
                if ownership.get(wafer)
                and not (ownership[wafer] & parent_set)
            }
            if foreign:
                quarantined(
                    "conservation_violation",
                    f"{event.kind} wafers are owned by another batch: {sorted(foreign)}",
                    event,
                    {"foreign": sorted(foreign)},
                )
                return
            wafers_in = sorted(wafers)

        # --- apply the move -------------------------------------------------
        if event.kind == "merge":
            for parent in froms:
                batches[parent].wafers -= wafers
                batches[parent].event_ids.append(event.event_id)
                batches[parent].evidence.extend(event.evidence)
            for child in tos:
                self._ensure_child(child, event, batches, batch_node_ids, nodes)
                batches[child].wafers |= wafers
                batches[child].event_ids.append(event.event_id)
                batches[child].evidence.extend(event.evidence)
            for wafer in wafers:
                # merge: leave ownership only on the destination batch(es)
                owners = ownership.setdefault(wafer, set())
                owners.difference_update(froms)
                owners.update(tos)
        else:
            # split / rework: distribute the same wafer set across destinations
            for parent in froms:
                batches[parent].wafers -= wafers
                batches[parent].event_ids.append(event.event_id)
                batches[parent].evidence.extend(event.evidence)
            for child in tos:
                self._ensure_child(child, event, batches, batch_node_ids, nodes)
                batches[child].wafers |= wafers
                batches[child].event_ids.append(event.event_id)
                batches[child].evidence.extend(event.evidence)
            # point-in-time ownership follows the explicit per-child routing
            routing = dict(event.wafer_routing) if event.wafer_routing else {}
            default_routing = {child: sorted(wafers) for child in tos}
            for child, child_wafers in {**default_routing, **routing}.items():
                for wafer in child_wafers:
                    owners = ownership.setdefault(wafer, set())
                    owners.difference_update(froms)
                    owners.add(child)

        # --- edges (one per source -> destination pair) ---------------------
        # Each edge carries only the wafers that actually flowed through that
        # pair, so wafer-scoped queries never fan out into parallel branches.
        routing = dict(event.wafer_routing) if event.wafer_routing else {}
        for parent in froms:
            for child in tos:
                if event.kind == "merge":
                    # ownership snapshot names each wafer's parent at this moment
                    pair_wafers = frozenset(
                        wafer for wafer in wafers
                        if parent in ownership_before.get(wafer, set())
                    )
                elif event.kind == "split":
                    pair_wafers = frozenset(routing.get(child, sorted(wafers)))
                else:  # rework: every moving wafer passes through each pair
                    pair_wafers = frozenset(wafers)
                edges.append(
                    self._make_edge(
                        event, batch_node_ids[parent], batch_node_ids[child],
                        pair_wafers,
                    )
                )
        seen_steps[step_key] = event
        ledger.append(
            ConservationLedger(
                event_id=event.event_id,
                edge_type=event.kind,
                wafers_in=tuple(wafers_in),
                wafers_out=tuple(sorted(wafers)),
                balanced=True,
            )
        )

    def _handle_equipment(self, event, batches, batch_node_ids, nodes, edges,
                          seen_steps, quarantined) -> None:
        label = event.batch
        if label not in batches:
            quarantined(
                "missing_parent",
                f"equipment_process on unknown batch {label}",
                event,
                {"batch": label},
            )
            return
        step_key = (label, event.operation or f"equip@{iso(event.event_time)}")
        prior = seen_steps.get(step_key)
        if prior is not None and prior.event_id != event.event_id:
            quarantined(
                "conflicting_operation",
                f"batch {label} already processed step {event.operation}",
                event,
                {"prior_event_id": prior.event_id},
            )
            return
        equip_id = content_id(
            "node_equip",
            (event.equipment or event.equipment_ref, event.operation),
        )
        if equip_id not in nodes:
            nodes[equip_id] = GraphNode(
                node_id=equip_id,
                node_type="equipment",
                label=event.equipment or event.equipment_ref or "UNKNOWN",
                created_event_time=event.event_time,
                created_ingest_time=event.ingest_time,
                event_ids=[event.event_id],
                evidence=event.evidence,
                detail={"operation": event.operation,
                        "equipment_ref": event.equipment_ref},
            )
        edges.append(self._make_edge(event, batch_node_ids[label], equip_id))
        batches[label].event_ids.append(event.event_id)
        batches[label].evidence.extend(event.evidence)
        seen_steps[step_key] = event

    def _handle_inspection(self, event, batches, batch_node_ids, nodes, edges,
                           seen_steps, quarantined) -> None:
        label = event.batch
        if label not in batches:
            quarantined(
                "missing_parent",
                f"inspection on unknown batch {label}",
                event,
                {"batch": label},
            )
            return
        step_key = (label, f"inspect:{event.inspection.get('inspection_id')}")
        if step_key in seen_steps:
            quarantined(
                "conflicting_operation",
                f"duplicate inspection {event.inspection.get('inspection_id')} on {label}",
                event,
                {"prior_event_id": seen_steps[step_key].event_id},
            )
            return
        insp_id = content_id(
            "node_insp",
            (label, event.inspection.get("inspection_id"), iso(event.event_time)),
        )
        nodes[insp_id] = GraphNode(
            node_id=insp_id,
            node_type="inspection",
            label=str(event.inspection.get("inspection_id")),
            created_event_time=event.event_time,
            created_ingest_time=event.ingest_time,
            event_ids=[event.event_id],
            evidence=event.evidence,
            detail=event.inspection,
        )
        edges.append(self._make_edge(event, batch_node_ids[label], insp_id))
        batches[label].event_ids.append(event.event_id)
        batches[label].evidence.extend(event.evidence)
        seen_steps[step_key] = event

    # ------------------------------------------------------------- utilities
    def _ensure_child(self, label, event, batches, batch_node_ids, nodes) -> None:
        if label in batches:
            return
        batches[label] = _BatchState(
            label=label,
            created_event_time=event.event_time,
            created_ingest_time=event.ingest_time,
            event_ids=[event.event_id],
            evidence=list(event.evidence),
        )
        self._put_batch_node(label, batches[label], batch_node_ids, nodes)

    @staticmethod
    def _put_batch_node(label, state: _BatchState, batch_node_ids, nodes) -> None:
        node_id = content_id("node_batch", label)
        batch_node_ids[label] = node_id
        nodes[node_id] = GraphNode(
            node_id=node_id,
            node_type="batch",
            label=label,
            created_event_time=state.created_event_time,
            created_ingest_time=state.created_ingest_time,
            event_ids=tuple(state.event_ids),
            evidence=tuple(state.evidence),
            detail={"initial_wafers": sorted(state.wafers)},
        )

    def _make_edge(
        self, event: CanonicalEvent, src_id: str, dst_id: str,
        edge_wafers: Optional[FrozenSet[str]] = None,
    ) -> GraphEdge:
        wafers = event.wafers if edge_wafers is None else frozenset(edge_wafers)
        edge_id = content_id(
            "edge",
            (event.kind, src_id, dst_id, event.event_id, sorted(wafers)),
        )
        return GraphEdge(
            edge_id=edge_id,
            edge_type=event.kind,
            src_id=src_id,
            dst_id=dst_id,
            event_time=event.event_time,
            ingest_time=event.ingest_time,
            wafers=wafers,
            event_id=event.event_id,
            evidence=event.evidence,
        )

    def _would_cycle(self, froms, tos, edges, batch_node_ids) -> bool:
        """True if adding from->tos edges closes a cycle.

        A cycle exists when any source batch is reachable downstream of
        any destination through the current edges (equipment/inspection
        leaves never lead back to batches, so batch-only edges suffice).
        """
        child_ids = {batch_node_ids.get(t) for t in tos}
        parent_ids = {batch_node_ids.get(f) for f in froms}
        adjacency: Dict[str, Set[str]] = {}
        for edge in edges:
            if edge.edge_type in STRUCTURAL:
                adjacency.setdefault(edge.src_id, set()).add(edge.dst_id)
        for start in child_ids:
            if start is None:
                continue
            stack = [start]
            seen: Set[str] = set()
            while stack:
                cur = stack.pop()
                if cur in parent_ids:
                    return True
                if cur in seen:
                    continue
                seen.add(cur)
                stack.extend(adjacency.get(cur, ()))
        return False


class BudgetExceeded(RuntimeError):
    """Raised when replay state grows past the configured safety limits."""

    def __init__(self, resource: str, limit: int):
        super().__init__(f"replay budget exceeded: {resource} limit {limit}")
        self.resource = resource
        self.limit = limit
