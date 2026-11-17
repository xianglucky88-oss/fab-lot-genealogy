"""Query layer: bounded traversal, wafer filtering, frozen snapshots."""
import tempfile
import unittest

from fab_lot_genealogy.pipeline import GenealogyPipeline
from fab_lot_genealogy.timeutil import parse_ts

from .fixtures import healthy_chain_records, EQUIPMENT_ALIASES


def populated_pipeline(tmp):
    pipe = GenealogyPipeline(tmp, equipment_aliases=EQUIPMENT_ALIASES)
    # rework is an operation reported through MES, not a separate source feed
    source_override = {"rework": "mes"}
    for source, group in _group_by_source(healthy_chain_records()):
        raw_source = source_override.get(source, source)
        pipe.ingest(raw_source, [r.payload for r in group], batch_id=f"b-{source}")
    return pipe


def _group_by_source(records):
    groups = {}
    for record in records:
        groups.setdefault(record.source, []).append(record)
    return list(groups.items())


class TestLineageQuery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.pipe = populated_pipeline(self.tmp.name)
        self.snap = self.pipe.materialize()

    def tearDown(self):
        self.tmp.cleanup()

    def test_ancestor_lineage_of_final_batch(self):
        result = self.pipe.query("B4", direction="ancestors").to_dict()
        labels = {n["label"] for n in result["nodes"] if n["node_type"] == "batch"}
        self.assertEqual(labels, {"B0", "B1", "B2", "B3", "B4"})

    def test_descendant_lineage_from_root(self):
        result = self.pipe.query("B0", direction="descendants").to_dict()
        labels = {n["label"] for n in result["nodes"] if n["node_type"] == "batch"}
        self.assertIn("B4", labels)
        self.assertIn("B3", labels)

    def test_equipment_and_rework_steps_are_listed(self):
        result = self.pipe.query("B4").to_dict()
        equip_labels = [n["label"] for n in result["equipment_steps"]]
        self.assertIn("EQ-7", equip_labels)
        edge_types = {e["edge_type"] for e in result["edges"]}
        self.assertIn("rework", edge_types)
        self.assertIn("merge", edge_types)

    def test_inspection_result_is_reachable(self):
        result = self.pipe.query("B4").to_dict()
        self.assertEqual(len(result["inspections"]), 1)
        self.assertEqual(result["inspections"][0]["detail"]["result"], "fail")

    def test_wafer_level_filtering_follows_only_that_wafer(self):
        # w1 travels B0 -> B1 -> B3 -> B4; it never touches B2
        result = self.pipe.query("B4", wafer="w1").to_dict()
        labels = {n["label"] for n in result["nodes"] if n["node_type"] == "batch"}
        self.assertNotIn("B2", labels)
        self.assertIn("B3", labels)
        result_w2 = self.pipe.query("B4", wafer="w3").to_dict()
        labels_w2 = {n["label"] for n in result_w2["nodes"] if n["node_type"] == "batch"}
        self.assertIn("B2", labels_w2)
        self.assertNotIn("B1", labels_w2)

    def test_node_budget_truncates_instead_of_exploding(self):
        result = self.pipe.query("B4", max_nodes=2).to_dict()
        self.assertTrue(result["truncated"])
        self.assertLessEqual(result["node_count"], 2)

    def test_unknown_target_returns_empty_not_error(self):
        result = self.pipe.query("B-DOES-NOT-EXIST").to_dict()
        self.assertEqual(result["node_count"], 0)
        self.assertFalse(result["truncated"])

    def test_every_conclusion_carries_raw_evidence(self):
        result = self.pipe.query("B4").to_dict()
        self.assertTrue(result["evidence"])
        raw_ids = {e["raw_id"] for e in result["evidence"]}
        # every evidence ref must resolve to a record actually in the raw layer
        landed = {r.raw_id for r in self.pipe.raw.read_all()}
        self.assertTrue(raw_ids)
        self.assertTrue(raw_ids.issubset(landed))
        # the root batch-create record is among the evidence chain from B4
        root_raw_id = self.pipe.raw.read_raw("mes")[0].raw_id
        self.assertIn(root_raw_id, raw_ids)


class TestFrozenSnapshot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.pipe = populated_pipeline(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_snapshot_is_persisted_and_cataloged(self):
        snap = self.pipe.materialize()
        catalog = self.pipe.snapshots.catalog()
        self.assertEqual(len(catalog), 1)
        self.assertEqual(catalog[0]["content_hash"], snap.content_hash())

    def test_frozen_query_does_not_see_later_ingestion(self):
        frozen = self.pipe.materialize(as_of_ingest=parse_ts("2026-01-04T00:00Z"))
        frozen_hash = frozen.content_hash()
        # new data arrives afterwards
        self.pipe.ingest(
            "equipment",
            [{"type": "equipment_process", "at": "2026-01-09T08:00Z",
              "batch": "B4", "equipment_id": "EQ-NEW", "operation": "OP-NEW"}],
            batch_id="b-late",
        )
        answer = self.pipe.frozen_query(frozen_hash, "B4")
        equip_labels = {n["label"] for n in answer["equipment_steps"]}
        self.assertNotIn("EQ-NEW", equip_labels)

    def test_rematerialization_is_byte_idempotent(self):
        first = self.pipe.materialize()
        second = self.pipe.materialize()
        self.assertEqual(first.content_hash(), second.content_hash())
        snapshot_files = list(self.pipe.snapshots.dir.glob("*.json"))
        snapshot_files = [p for p in snapshot_files if p.name != "catalog.json"]
        self.assertEqual(len(snapshot_files), 1)

    def test_tampered_snapshot_fails_integrity_check(self):
        snap = self.pipe.materialize()
        path = self.pipe.snapshots.dir / f"{snap.content_hash()}.json"
        body = path.read_text()
        path.write_text(body.replace("B0", "B0-EVIL"))
        with self.assertRaises(RuntimeError):
            self.pipe.frozen_query(snap.content_hash(), "B4")


if __name__ == "__main__":
    unittest.main()
