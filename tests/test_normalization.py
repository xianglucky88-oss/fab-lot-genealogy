"""Normalization: multi-source schemas, evolution, aliases, dedupe, corrections."""
import unittest

from fab_lot_genealogy.models import RawRecord
from fab_lot_genealogy.normalizer import Normalizer
from fab_lot_genealogy.timeutil import parse_ts


def raw(raw_id, source, payload, received="2026-02-01T00:00:00Z", batch="b"):
    return RawRecord(
        raw_id=raw_id, source=source, source_seq=0, payload=payload,
        received_at=parse_ts(received), batch_id=batch,
    )


class TestNormalizer(unittest.TestCase):
    def test_legacy_operations_shape_is_accepted(self):
        events = Normalizer().normalize_all([
            raw("r1", "legacy", {
                "at": "2026-01-01T10:00:00Z",
                "from_batch": "B0", "to_batch": "B1",
                "wafer": "W1", "kind": "split",
            })
        ])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].kind, "split")
        self.assertEqual(events[0].from_batches, ("B0",))
        self.assertEqual(events[0].to_batches, ("B1",))
        self.assertEqual(events[0].schema_version, 1)

    def test_five_sources_normalize_to_one_event_vocabulary(self):
        records = [
            raw("r1", "mes", {"type": "batch_create", "at": "2026-01-01T08:00Z",
                              "batch_id": "B1", "wafers": ["W1", "W2"]}),
            raw("r2", "split", {"type": "split", "at": "2026-01-02T08:00Z",
                                "from_batch": "B1", "to_batches": ["B2", "B3"],
                                "wafers_by_child": {"B2": ["W1"], "B3": ["W2"]}}),
            raw("r3", "merge", {"type": "merge", "at": "2026-01-03T08:00Z",
                                "from_batches": ["B2", "B3"], "to_batch": "B4",
                                "wafers": ["W1", "W2"]}),
            raw("r4", "equipment", {"type": "equipment_process",
                                    "at": "2026-01-04T08:00Z", "batch": "B4",
                                    "equipment_id": "EQ-7", "operation": "ETCH-1"}),
            raw("r5", "inspection", {"type": "inspection",
                                     "at": "2026-01-05T08:00Z", "batch": "B4",
                                     "inspection_id": "INSP-1", "result": "fail",
                                     "measurements": {"defect": 3}}),
        ]
        events = Normalizer().normalize_all(records)
        kinds = [e.kind for e in events]
        self.assertEqual(
            kinds,
            ["batch_create", "split", "merge", "equipment_process", "inspection"],
        )
        insp = next(e for e in events if e.kind == "inspection")
        self.assertEqual(insp.inspection["result"], "fail")

    def test_equipment_alias_resolution_keeps_raw_ref(self):
        nz = Normalizer(equipment_aliases={"TOOL-A": "EQ-100"})
        events = nz.normalize_all([
            raw("r1", "equipment", {
                "at": "2026-01-04T08:00Z", "batch": "B1",
                "equipment": "TOOL-A", "operation": "OP1"})
        ])
        self.assertEqual(events[0].equipment, "EQ-100")
        self.assertEqual(events[0].equipment_ref, "TOOL-A")

    def test_schema_v1_equipment_field_still_resolves_on_old_batches(self):
        # old batch used `equipment`; new schema v2 uses `equipment_id`
        old = Normalizer().normalize_all([raw("r1", "equipment", {
            "schema_version": 1, "at": "2020-01-01T00:00Z", "batch": "B1",
            "equipment": "EQ-1", "operation": "OP"})])
        new = Normalizer().normalize_all([raw("r2", "equipment", {
            "schema_version": 2, "at": "2026-01-01T00:00Z", "batch": "B2",
            "equipment_id": "EQ-1", "operation": "OP"})])
        self.assertEqual(old[0].equipment, "EQ-1")
        self.assertEqual(new[0].equipment, "EQ-1")
        self.assertEqual(old[0].schema_version, 1)
        self.assertEqual(new[0].schema_version, 2)

    def test_duplicate_report_merges_evidence(self):
        payload = {"type": "batch_create", "at": "2026-01-01T08:00Z",
                   "batch_id": "B1", "wafers": ["W1"]}
        nz = Normalizer()
        events = nz.normalize_all([
            raw("r1", "mes", dict(payload), received="2026-01-01T09:00Z"),
            raw("r2", "mes", dict(payload), received="2026-01-02T09:00Z"),
        ])
        self.assertEqual(len(events), 1)
        self.assertEqual({r.raw_id for r in events[0].evidence}, {"r1", "r2"})
        self.assertEqual(nz.quarantine, [])

    def test_conflicting_duplicate_is_quarantined(self):
        nz = Normalizer()
        base = {"type": "batch_create", "at": "2026-01-01T08:00Z", "batch_id": "B1"}
        events = nz.normalize_all([
            raw("r1", "mes", {**base, "wafers": ["W1"]}, received="2026-01-01T09:00Z"),
            raw("r2", "mes", {**base, "wafers": ["W2"]}, received="2026-01-02T09:00Z"),
        ])
        self.assertEqual(len(events), 1)
        self.assertEqual(sorted(events[0].wafers), ["W1"])  # earliest kept
        self.assertEqual(nz.quarantine[0].code, "duplicate_conflict")

    def test_correction_carries_validity_window_and_supersedes_chain(self):
        original = raw("r1", "inspection", {
            "type": "inspection", "at": "2026-01-05T08:00Z", "batch": "B4",
            "inspection_id": "INSP-1", "result": "pass"})
        correction = raw("r2", "inspection", {
            "type": "correction",
            "at": "2026-01-06T08:00Z",
            "ingest_at": "2026-01-10T08:00Z",
            "valid_from": "2026-01-05T00:00Z",
            "valid_to": "9999-12-31T00:00Z",
            "corrects": {"at": "2026-01-05T08:00Z", "batch": "B4",
                         "inspection_id": "INSP-1"},
            "event": {
                "type": "inspection", "at": "2026-01-05T08:00Z", "batch": "B4",
                "inspection_id": "INSP-1", "result": "fail",
                "measurements": {"defect": 9},
            },
        }, received="2026-01-10T08:00Z")
        nz = Normalizer()
        events = nz.normalize_all([original, correction])
        corr = next(e for e in events if e.supersedes is not None)
        self.assertEqual(corr.inspection["result"], "fail")
        self.assertEqual(corr.valid_from, parse_ts("2026-01-05T00:00Z"))
        self.assertTrue(corr.natural_key != corr.supersedes)

    def test_invalid_record_goes_to_quarantine_not_crash(self):
        nz = Normalizer()
        events = nz.normalize_all([
            raw("r1", "split", {"at": "2026-01-01T08:00Z"})  # no kind/source hints
        ])
        self.assertEqual(events, [])
        self.assertEqual(nz.quarantine[0].code, "invalid_record")

    def test_forward_compatible_extra_fields_are_retained(self):
        events = Normalizer().normalize_all([raw("r1", "mes", {
            "schema_version": 99,
            "type": "batch_create", "at": "2026-01-01T08:00Z",
            "batch_id": "B1", "wafers": ["W1"],
            "new_future_field": {"x": 1},
        })])
        self.assertEqual(dict(events[0].extra)["new_future_field"], {"x": 1})


if __name__ == "__main__":
    unittest.main()
