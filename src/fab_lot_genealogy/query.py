"""Query layer: bounded lineage traversal over an immutable snapshot.

Every query runs against a *frozen* snapshot, so materializing a
snapshot at ingestion time ``I`` guarantees later writes can never
change its answers.  Traversals carry an explicit node-expansion budget
and a temporary-state budget; when the budget is hit the result is
returned with ``truncated=True`` instead of exhausting memory.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from .graph import GraphSnapshot, STRUCTURAL
from .ids import content_id
from .models import GraphEdge, RawRef
from .timeutil import iso


@dataclass
class LineageResult:
    target: str
    target_node_id: Optional[str]
    direction: str
    as_of_event: str
    as_of_ingest: str
    node_count: int
    edge_count: int
    max_nodes: int
    truncated: bool
    nodes: List[dict]
    edges: List[dict]
    equipment_steps: List[dict]
    inspections: List[dict]
    evidence: List[dict]

    def to_dict(self) -> dict:
        return self.__dict__.copy()


class LineageQuery:
    def __init__(self, snapshot: GraphSnapshot):
        self.s = snapshot
        self._children: Dict[str, List[GraphEdge]] = {}
        self._parents: Dict[str, List[GraphEdge]] = {}
        # equipment/inspection leaves attached to a batch node; these hang off
        # the batch and must be reachable in either traversal direction
        self._attachments: Dict[str, List[GraphEdge]] = {}
        for edge in snapshot.edges:
            if edge.edge_type in STRUCTURAL:
                self._children.setdefault(edge.src_id, []).append(edge)
                self._parents.setdefault(edge.dst_id, []).append(edge)
            else:
                self._attachments.setdefault(edge.src_id, []).append(edge)

    # ------------------------------------------------------------- resolution
    def _resolve_batch_node(self, label: str) -> Optional[str]:
        return content_id("node_batch", label) if content_id("node_batch", label) in self.s.nodes else None

    def _resolve(self, target: str) -> Tuple[Optional[str], Optional[str]]:
        """Return (node_id, wafer). Target may be a node id, batch label or wafer."""
        if target in self.s.nodes:
            return target, None
        node_id = self._resolve_batch_node(target)
        if node_id is not None:
            return node_id, None
        # wafer id: find any batch node that currently holds / carried it
        for edge in self.s.edges:
            if target in edge.wafers:
                return edge.src_id, target
        return None, target

    # ---------------------------------------------------------------- lineage
    def lineage(
        self,
        target: str,
        direction: str = "ancestors",
        max_nodes: int = 10_000,
        wafer: Optional[str] = None,
    ) -> LineageResult:
        if direction not in {"ancestors", "descendants", "full"}:
            raise ValueError("direction must be ancestors, descendants or full")
        start_id, wafer_guess = self._resolve(target)
        wafer = wafer or wafer_guess
        if start_id is None:
            return self._empty(target, None, direction, max_nodes)

        visited_nodes: Set[str] = {start_id}
        visited_edges: List[GraphEdge] = []
        truncated = False

        def take(edge: GraphEdge) -> bool:
            if wafer is not None and edge.edge_type in STRUCTURAL and wafer not in edge.wafers:
                return False
            return True

        def walk(adjacency: Dict[str, List[GraphEdge]], roots: List[str]) -> None:
            nonlocal truncated
            frontier = list(roots)

            def note_edge(edge: GraphEdge, other: str) -> None:
                nonlocal truncated
                if edge.edge_id not in {e.edge_id for e in visited_edges}:
                    visited_edges.append(edge)
                if other not in visited_nodes:
                    if len(visited_nodes) >= max_nodes:
                        truncated = True
                        return
                    visited_nodes.add(other)
                    frontier.append(other)

            while frontier:
                cur = frontier.pop()
                # equipment/inspection leaves attached to this batch always
                # travel with it (they describe how the batch was produced)
                for leaf_edge in self._attachments.get(cur, ()):
                    if take(leaf_edge):
                        note_edge(leaf_edge, leaf_edge.dst_id)
                    if truncated:
                        return
                for edge in adjacency.get(cur, ()):
                    if not take(edge):
                        continue
                    other = edge.dst_id if cur == edge.src_id else edge.src_id
                    note_edge(edge, other)
                    if truncated:
                        return

        if direction in ("ancestors", "full"):
            walk(self._parents, [start_id])
        if direction in ("descendants", "full"):
            walk(self._children, [start_id])

        nodes = [self.s.nodes[n] for n in sorted(visited_nodes)]
        edges = sorted(
            (e for e in visited_edges if e.src_id in visited_nodes and e.dst_id in visited_nodes),
            key=lambda e: (iso(e.event_time), e.edge_id),
        )
        equipment_steps = [
            n.to_dict() for n in nodes if n.node_type == "equipment"
        ]
        inspections = [n.to_dict() for n in nodes if n.node_type == "inspection"]

        evidence: Dict[str, RawRef] = {}
        for node in nodes:
            for ref in node.evidence:
                evidence[ref.raw_id] = ref
        for edge in edges:
            for ref in edge.evidence:
                evidence[ref.raw_id] = ref

        return LineageResult(
            target=target,
            target_node_id=start_id,
            direction=direction,
            as_of_event=iso(self.s.as_of_event),
            as_of_ingest=iso(self.s.as_of_ingest),
            node_count=len(nodes),
            edge_count=len(edges),
            max_nodes=max_nodes,
            truncated=truncated,
            nodes=[n.to_dict() for n in nodes],
            edges=[e.to_dict() for e in edges],
            equipment_steps=equipment_steps,
            inspections=inspections,
            evidence=[r.to_dict() for r in (evidence[k] for k in sorted(evidence))],
        )

    def _empty(self, target, node_id, direction, max_nodes) -> LineageResult:
        return LineageResult(
            target=target,
            target_node_id=node_id,
            direction=direction,
            as_of_event=iso(self.s.as_of_event),
            as_of_ingest=iso(self.s.as_of_ingest),
            node_count=0,
            edge_count=0,
            max_nodes=max_nodes,
            truncated=False,
            nodes=[],
            edges=[],
            equipment_steps=[],
            inspections=[],
            evidence=[],
        )
