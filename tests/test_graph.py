"""Graph layer: conservation, quarantine classes, order independence."""
import unittest

from fab_lot_genealogy.graph import GraphBuilder
from fab_lot_genealogy.normalizer import Normalizer

from .fixtures import healthy_chain_records, raw


def build(records, **kw):
    nz = Normalizer()
    events = nz.normalize_all(records)
    snap = GraphBuilder().build(events, prior_quarantine=nz.quarantine, **kw)
    return events, snap


class TestHealthyChain(unittest.TestCase):
    def test_all_batches_present_and_typed(self):
        _, snap = build(healthy_chain_records())
        batch_labels = {n.label for n in snap.nodes.values() if n.node_type == "batch"}
        self.assertEqual(batch_labels, {"B0", "B1", "B2", "B3", "B4"})
        kinds = {n.node_type for n in snap.nodes.values()}
        self.assertEqual(kinds, {"batch", "equipment", "inspection"})

    def test_edges_follow_split_rework_merge_sequence(self):
        _, snap = build(healthy_chain_records())
        pairs = {(e.edge_type, e.src_id, e.dst_id) for e in snap.edges}
        def node(label):
            return next(n.node_id for n in snap.nodes.values()
                        if n.node_type == "batch" and n.label == label)
        self.assertIn(("split", node("B0"), node("B1")), pairs)
        self.assertIn(("split", node("B0"), node("B2")), pairs)
        self.assertIn(("rework", node("B1"), node("B3")), pairs)
        self.assertIn(("merge", node("B2"), node("B4")), pairs)
        self.assertIn(("merge", node("B3"), node("B4")), pairs)
        # structural edges carry the exact wafer set they moved
        rework_edge = next(e for e in snap.edges if e.edge_type == "rework")
        self.assertEqual(rework_edge.wafers, frozenset({"w1", "w2"}))

    def test_conservation_ledger_is_balanced(self):
        _, snap = build(healthy_chain_records())
        self.assertTrue(snap.ledger)
        self.assertTrue(all(entry.balanced for entry in snap.ledger))
        merge = next(e for e in snap.ledger if e.edge_type == "merge")
        self.assertEqual(set(merge.wafers_in), {"w1", "w2", "w3", "w4"})

    def test_every_graph_object_points_back_to_raw_evidence(self):
        _, snap = build(healthy_chain_records())
        for node in snap.nodes.values():
            self.assertTrue(node.evidence, node.label)
            self.assertTrue(all(r.raw_id.startswith("raw_") for r in node.evidence))
        for edge in snap.edges:
            self.assertTrue(edge.evidence)

    def test_no_quarantine_on_healthy_chain(self):
        _, snap = build(healthy_chain_records())
        self.assertEqual(snap.quarantine, [])


class TestConservationViolations(unittest.TestCase):
    def test_split_moving_unknown_wafer_is_quarantined(self):
        records = [
            raw("r0", "mes", {"type": "batch_create", "at": "2026-01-01T08:00Z",
                              "batch_id": "B0", "wafers": ["w1"]}),
            raw("r1", "split", {"type": "split", "at": "2026-01-02T08:00Z",
                                "from_batch": "B0", "to_batch": "B1",
                                "wafers": ["w9"]}),  # w9 never existed
        ]
        _, snap = build(records)
        codes = {q.code for q in snap.quarantine}
        self.assertIn("conservation_violation", codes)
        # the rejected event must not have created B1
        labels = {n.label for n in snap.nodes.values() if n.node_type == "batch"}
        self.assertNotIn("B1", labels)

    def test_merge_with_overlapping_parents_is_quarantined(self):
        # w2 is routed into BOTH children by the split (a duplicated wafer),
        # so the later merge has overlapping parents and must be rejected.
        records = [
            raw("r0", "mes", {"type": "batch_create", "at": "2026-01-01T08:00Z",
                              "batch_id": "ROOT", "wafers": ["w1", "w2", "w3"]}),
            raw("r1", "split", {
                "type": "split", "at": "2026-01-02T08:00Z",
                "from_batch": "ROOT", "to_batches": ["P1", "P2"],
                "wafers_by_child": {"P1": ["w1", "w2"], "P2": ["w2", "w3"]},
                "wafers": ["w1", "w2", "w3"]}),
            raw("r2", "merge", {"type": "merge", "at": "2026-01-03T08:00Z",
                                "from_batches": ["P1", "P2"], "to_batch": "C",
                                "wafers": ["w1", "w2", "w3"]}),
        ]
        _, snap = build(records)
        self.assertTrue(
            any(q.code == "conservation_violation" for q in snap.quarantine)
        )
        self.assertIn(
            "overlap",
            " ".join(q.reason for q in snap.quarantine),
        )

    def test_merge_wafer_set_must_equal_parent_union(self):
        records = [
            raw("r0", "mes", {"type": "batch_create", "at": "2026-01-01T08:00Z",
                              "batch_id": "P1", "wafers": ["w1"]}),
            raw("r1", "mes", {"type": "batch_create", "at": "2026-01-01T09:00Z",
                              "batch_id": "P2", "wafers": ["w2"]}),
            raw("r2", "merge", {"type": "merge", "at": "2026-01-03T08:00Z",
                                "from_batches": ["P1", "P2"], "to_batch": "C",
                                "wafers": ["w1", "w2", "wX"]}),
        ]
        _, snap = build(records)
        self.assertTrue(
            any(q.code == "conservation_violation" for q in snap.quarantine)
        )


class TestQuarantineClasses(unittest.TestCase):
    def test_missing_parent_batch(self):
        records = [
            raw("r1", "split", {"type": "split", "at": "2026-01-02T08:00Z",
                                "from_batch": "GHOST", "to_batch": "B1",
                                "wafers": ["w1"]}),
        ]
        _, snap = build(records)
        self.assertEqual({q.code for q in snap.quarantine}, {"missing_parent"})
        self.assertEqual(snap.quarantine[0].detail["missing_parents"], ["GHOST"])

    def test_cyclic_relation_is_detected_and_isolated(self):
        records = [
            raw("r0", "mes", {"type": "batch_create", "at": "2026-01-01T08:00Z",
                              "batch_id": "A", "wafers": ["w1"]}),
            raw("r1", "split", {"type": "split", "at": "2026-01-02T08:00Z",
                                "from_batch": "A", "to_batch": "B", "wafers": ["w1"]}),
            # trying to move the wafer back into its own ancestor
            raw("r2", "split", {"type": "split", "at": "2026-01-03T08:00Z",
                                "from_batch": "B", "to_batch": "A", "wafers": ["w1"]}),
        ]
        _, snap = build(records)
        self.assertTrue(any(q.code == "cyclic_relation" for q in snap.quarantine))
        # the valid A->B edge survives; the cyclic one is absent
        structural = [e for e in snap.edges if e.edge_type == "split"]
        self.assertEqual(len(structural), 1)

    def test_conflicting_same_operation_step(self):
        records = [
            raw("r0", "mes", {"type": "batch_create", "at": "2026-01-01T08:00Z",
                              "batch_id": "B", "wafers": ["w1"]}),
            raw("r1", "equipment", {
                "type": "equipment_process", "at": "2026-01-02T08:00Z",
                "batch": "B", "equipment_id": "EQ-1", "operation": "ETCH"}),
            raw("r2", "equipment", {
                "type": "equipment_process", "at": "2026-01-02T09:00Z",
                "batch": "B", "equipment_id": "EQ-9", "operation": "ETCH"}),
        ]
        _, snap = build(records)
        self.assertTrue(
            any(q.code == "conflicting_operation" for q in snap.quarantine)
        )

    def test_quarantine_entries_are_explainable(self):
        records = [
            raw("r1", "split", {"type": "split", "at": "2026-01-02T08:00Z",
                                "from_batch": "GHOST", "to_batch": "B1",
                                "wafers": ["w1"]}),
        ]
        _, snap = build(records)
        q = snap.quarantine[0]
        self.assertTrue(q.reason)
        self.assertEqual(q.evidence[0].raw_id, "raw_r1")
        self.assertIn("missing_parents", q.detail)


class TestOrderIndependence(unittest.TestCase):
    def test_shuffled_and_reversed_inputs_produce_identical_snapshot(self):
        base = healthy_chain_records()
        _, forward = build(base)
        _, reversed_snap = build(list(reversed(base)))
        shuffled = [base[i] for i in (3, 0, 5, 1, 4, 2)]
        _, shuffled_snap = build(shuffled)
        self.assertEqual(forward.content_hash(), reversed_snap.content_hash())
        self.assertEqual(forward.content_hash(), shuffled_snap.content_hash())
        self.assertEqual(
            sorted(e.edge_id for e in forward.edges),
            sorted(e.edge_id for e in shuffled_snap.edges),
        )


if __name__ == "__main__":
    unittest.main()
