"""Bitemporal as-of semantics and correction-window scoping.

Two independent clocks:

* event time   — when things happened on the factory floor;
* ingest time  — when the data lake learned about them.

A snapshot at ``(T, I)`` must contain exactly the facts ingested by ``I``
and valid at ``T``.  Corrections only reshape descendants inside their
event-time validity window, and a frozen snapshot can never see writes
that arrived later.
"""
import unittest

from fab_lot_genealogy.graph import GraphBuilder
from fab_lot_genealogy.normalizer import Normalizer
from fab_lot_genealogy.timeutil import parse_ts

from .fixtures import raw


def build(records, as_of_event=None, as_of_ingest=None):
    nz = Normalizer()
    events = nz.normalize_all(records)
    snap = GraphBuilder().build(
        events,
        prior_quarantine=nz.quarantine,
        as_of_event=as_of_event or parse_ts("9999-12-31T00:00Z"),
        as_of_ingest=as_of_ingest or parse_ts("9999-12-31T00:00Z"),
    )
    return events, snap


def inspection_chain(corrected_result=None):
    records = [
        raw("r0", "mes", {"type": "batch_create", "at": "2026-03-01T08:00Z",
                          "batch_id": "B", "wafers": ["w1"]},
            received="2026-03-01T09:00Z"),
        raw("r1", "inspection", {
            "type": "inspection", "at": "2026-03-02T08:00Z", "batch": "B",
            "inspection_id": "I1", "result": "pass"},
            received="2026-03-02T09:00Z"),
    ]
    if corrected_result:
        records.append(raw("r2", "inspection", {
            "type": "correction",
            "at": "2026-03-10T12:00Z",
            "ingest_at": "2026-03-10T12:00Z",
            "valid_from": "2026-03-02T00:00Z",
            "valid_to": "9999-12-31T00:00Z",
            "corrects": {"at": "2026-03-02T08:00Z", "batch": "B",
                         "inspection_id": "I1"},
            "event": {
                "type": "inspection", "at": "2026-03-02T08:00Z", "batch": "B",
                "inspection_id": "I1", "result": corrected_result,
                "measurements": {"defect": 7},
            },
        }, received="2026-03-10T12:00Z"))
    return records


def inspection_result(snap):
    insp = next(n for n in snap.nodes.values() if n.node_type == "inspection")
    return insp.detail["result"]


class TestIngestAxisFreeze(unittest.TestCase):
    def test_snapshot_before_ingestion_cannot_see_later_writes(self):
        records = inspection_chain(corrected_result="fail")
        # freeze at ingest time before the correction (2026-03-10)
        _, frozen = build(records, as_of_ingest=parse_ts("2026-03-05T00:00Z"))
        self.assertEqual(inspection_result(frozen), "pass")
        # the later materialization does see the correction
        _, current = build(records)
        self.assertEqual(inspection_result(current), "fail")

    def test_freeze_excludes_events_ingested_after_ceiling(self):
        records = [
            raw("r0", "mes", {"type": "batch_create", "at": "2026-03-01T08:00Z",
                              "batch_id": "B", "wafers": ["w1"]},
                received="2026-03-01T09:00Z"),
            raw("r1", "equipment", {
                "type": "equipment_process", "at": "2026-03-02T08:00Z",
                "batch": "B", "equipment_id": "EQ-1", "operation": "OP1"},
                received="2026-03-09T09:00Z"),  # late arrival
        ]
        _, early = build(records, as_of_ingest=parse_ts("2026-03-05T00:00Z"))
        self.assertFalse(
            any(n.node_type == "equipment" for n in early.nodes.values())
        )
        _, late = build(records)
        self.assertTrue(
            any(n.node_type == "equipment" for n in late.nodes.values())
        )

    def test_late_arriving_old_event_appears_in_current_but_not_frozen_view(self):
        # out-of-order log: an old event arrives last
        records = [
            raw("r1", "split", {"type": "split", "at": "2026-03-02T08:00Z",
                                "from_batch": "B", "to_batch": "C", "wafers": ["w1"]},
                received="2026-03-08T00:00Z"),
            raw("r0", "mes", {"type": "batch_create", "at": "2026-03-01T08:00Z",
                              "batch_id": "B", "wafers": ["w1"]},
                received="2026-03-01T09:00Z"),
        ]
        _, frozen = build(records, as_of_ingest=parse_ts("2026-03-05T00:00Z"))
        labels = {n.label for n in frozen.nodes.values() if n.node_type == "batch"}
        self.assertEqual(labels, {"B"})
        _, current = build(records)
        self.assertEqual(
            {n.label for n in current.nodes.values() if n.node_type == "batch"},
            {"B", "C"},
        )


class TestEventAxisWindow(unittest.TestCase):
    def test_correction_only_applies_inside_its_validity_window(self):
        records = [
            raw("r0", "mes", {"type": "batch_create", "at": "2026-03-01T08:00Z",
                              "batch_id": "B", "wafers": ["w1"]}),
            raw("r1", "inspection", {
                "type": "inspection", "at": "2026-03-02T08:00Z", "batch": "B",
                "inspection_id": "I1", "result": "pass"}),
            raw("r2", "inspection", {
                "type": "correction",
                "at": "2026-03-10T12:00Z",
                "ingest_at": "2026-03-10T12:00Z",
                # correction only valid for a historical sub-window
                "valid_from": "2026-03-02T00:00Z",
                "valid_to": "2026-03-05T00:00Z",
                "corrects": {"at": "2026-03-02T08:00Z", "batch": "B",
                             "inspection_id": "I1"},
                "event": {
                    "type": "inspection", "at": "2026-03-02T08:00Z", "batch": "B",
                    "inspection_id": "I1", "result": "fail",
                    "measurements": {"defect": 7},
                },
            }),
        ]
        # as-of inside the correction window -> corrected fact
        _, inside = build(records, as_of_event=parse_ts("2026-03-03T00:00Z"))
        self.assertEqual(inspection_result(inside), "fail")
        # as-of after the correction window -> original fact again
        _, outside = build(records, as_of_event=parse_ts("2026-03-06T00:00Z"))
        self.assertEqual(inspection_result(outside), "pass")

    def test_correction_evidence_chain_keeps_both_raw_refs(self):
        records = inspection_chain(corrected_result="fail")
        _, snap = build(records)
        insp = next(n for n in snap.nodes.values() if n.node_type == "inspection")
        raw_ids = {r.raw_id for r in insp.evidence}
        self.assertIn("raw_r1", raw_ids)
        self.assertIn("raw_r2", raw_ids)

    def test_descendant_only_affected_within_revision_time_range(self):
        # A correction changes a split at 2026-03-02; batches created before
        # the revision's valid_from keep their original shape, descendants
        # inside the window reflect the revision.
        records = [
            raw("r0", "mes", {"type": "batch_create", "at": "2026-03-01T08:00Z",
                              "batch_id": "ROOT", "wafers": ["w1", "w2"]}),
            raw("r1", "split", {
                "type": "split", "at": "2026-03-02T08:00Z", "from_batch": "ROOT",
                "to_batches": ["A"], "wafers": ["w1", "w2"],
                "wafers_by_child": {"A": ["w1", "w2"]}}),
            raw("r2", "split", {
                "type": "correction",
                "at": "2026-03-20T08:00Z", "ingest_at": "2026-03-20T08:00Z",
                "valid_from": "2026-03-02T00:00Z",
                "corrects": {
                    "at": "2026-03-02T08:00Z",
                    "original_identity": {
                        "from_batch": "ROOT", "to_batches": ["A"],
                    },
                },
                "event": {
                    "type": "split", "at": "2026-03-02T08:00Z", "from_batch": "ROOT",
                    "to_batches": ["A", "A2"],
                    "wafers": ["w1", "w2"],
                    "wafers_by_child": {"A": ["w1"], "A2": ["w2"]},
                },
            }),
        ]
        # before the revision window: only the original A exists
        _, before = build(records, as_of_event=parse_ts("2026-03-01T12:00Z"))
        before_labels = {n.label for n in before.nodes.values() if n.node_type == "batch"}
        self.assertEqual(before_labels, {"ROOT"})
        # inside the window: both revised children exist
        _, after = build(records)
        after_labels = {n.label for n in after.nodes.values() if n.node_type == "batch"}
        self.assertEqual(after_labels, {"ROOT", "A", "A2"})


class TestBitemporalCombinations(unittest.TestCase):
    def test_four_quadrants_of_event_and_ingest_time(self):
        records = inspection_chain(corrected_result="fail")
        cases = [
            # (as_of_event, as_of_ingest, expected result)
            ("2026-03-01T00:00Z", "2026-03-11T00:00Z", None),      # before inspection happened
            ("2026-03-03T00:00Z", "2026-03-05T00:00Z", "pass"),    # after event, before correction
            ("2026-03-03T00:00Z", "2026-03-11T00:00Z", "fail"),    # after correction ingest
            ("9999-12-31T00:00Z", "2026-03-05T00:00Z", "pass"),    # future event view, frozen ingest
        ]
        for at, it, expected in cases:
            with self.subTest(at=at, it=it):
                _, snap = build(
                    records,
                    as_of_event=parse_ts(at),
                    as_of_ingest=parse_ts(it),
                )
                insp_nodes = [n for n in snap.nodes.values() if n.node_type == "inspection"]
                if expected is None:
                    self.assertEqual(insp_nodes, [])
                else:
                    self.assertEqual(insp_nodes[0].detail["result"], expected)


if __name__ == "__main__":
    unittest.main()
