"""Raw layer: idempotent landing, partial-failure recovery, replay identity."""
import tempfile
import unittest
from pathlib import Path

from fab_lot_genealogy.raw_store import RawStore


def mes_batch(n=3):
    return [
        {
            "type": "batch_create",
            "at": f"2026-01-0{i+1}T08:00:00Z",
            "batch_id": f"B{i}",
            "wafers": [f"W{i}-{k}" for k in range(n)],
        }
        for i in range(3)
    ]


class TestRawStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = RawStore(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_append_and_read_roundtrip(self):
        manifest = self.store.append_batch("mes", mes_batch(), "b1")
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(len(manifest["accepted"]), 3)
        rows = self.store.read_raw("mes")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].source, "mes")
        self.assertEqual(rows[0].batch_id, "b1")

    def test_replay_is_idempotent(self):
        first = self.store.append_batch("mes", mes_batch(), "b1")
        second = self.store.append_batch("mes", mes_batch(), "b1-retry")
        self.assertEqual(len(first["accepted"]), 3)
        self.assertEqual(second["accepted"], [])
        self.assertEqual(len(second["duplicate"]), 3)
        # the raw log itself does not grow on replay
        self.assertEqual(len(self.store.read_raw("mes")), 3)
        # raw ids are content-derived and stable
        self.assertEqual(
            sorted(first["accepted"]),
            sorted(second["duplicate"]),
        )

    def test_partial_batch_failure_is_recoverable(self):
        bad = mes_batch()
        bad.insert(1, "not-an-object")
        manifest = self.store.append_batch("split", bad, "p1")
        self.assertEqual(manifest["status"], "partial")
        self.assertEqual(len(manifest["accepted"]), 3)
        self.assertEqual(manifest["failed"][0]["offset"], 1)

        state = self.store.batch_state("p1")
        self.assertEqual(state["status"], "partial")
        self.assertEqual(len(state["acked"]), 3)

        # a retry batch containing distinct good records lands normally;
        # the failed offset is isolated and never blocks later batches
        follow_up = [
            {
                "type": "split",
                "at": f"2026-02-0{i+1}T08:00:00Z",
                "from_batch": f"S{i}", "to_batch": f"T{i}",
                "wafers": [f"SW{i}"],
            }
            for i in range(2)
        ]
        good = self.store.append_batch("split", follow_up, "p2")
        self.assertEqual(good["status"], "complete")
        # 3 good records from the partial batch + 2 new ones
        self.assertEqual(len(self.store.read_raw("split")), 5)
        # the partial manifest still records exactly one failed offset
        self.assertEqual(len(self.store.batch_state("p1")["failed"]), 1)

    def test_same_batch_id_different_source_rejected(self):
        self.store.append_batch("mes", mes_batch(), "x")
        with self.assertRaises(ValueError):
            self.store.append_batch("equipment", mes_batch(), "x")

    def test_index_and_manifest_are_durable_files(self):
        self.store.append_batch("mes", mes_batch(), "b1")
        files = {p.name for p in Path(self.tmp.name).rglob("*") if p.is_file()}
        self.assertIn("mes.jsonl", files)
        self.assertIn("seen_index.json", files)
        self.assertIn("b1.json", files)
        # reopening the store reconstructs dedupe state from disk
        reopened = RawStore(self.tmp.name)
        again = reopened.append_batch("mes", mes_batch(), "b2")
        self.assertEqual(len(again["duplicate"]), 3)


if __name__ == "__main__":
    unittest.main()
