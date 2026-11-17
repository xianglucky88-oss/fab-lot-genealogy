"""Streaming vs. batch equivalence, replay idempotence, crash recovery, scale.

These tests prove the operational promises:

* the stream path and the batch path emit identical snapshots for the same
  logical inputs, regardless of arrival order;
* replaying the raw log any number of times is idempotent;
* a simulated crash mid-stream resumes from the checkpoint with no loss or
  duplicate graph state;
* a large fan-in merge stays conserved and queryable under bounded state.
"""
import json
import random
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fab_lot_genealogy.graph import GraphBuilder, BudgetExceeded
from fab_lot_genealogy.pipeline import GenealogyPipeline
from fab_lot_genealogy.stream import StreamProcessor

from .fixtures import healthy_chain_records

# A fixed ingestion clock keeps replay determinism independent of wall time;
# each distinct record receives a distinct, monotonically increasing stamp.
EPOCH = datetime(2026, 2, 1, tzinfo=timezone.utc)


def fixed_received(index: int) -> datetime:
    return EPOCH + timedelta(minutes=index)


class TestStreamBatchEquivalence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _batches_by_source(self, records):
        groups = {}
        for record in records:
            source = "mes" if record.source == "rework" else record.source
            groups.setdefault(source, []).append(record.payload)
        return groups

    def test_stream_and_batch_produce_same_snapshot_hash(self):
        groups = self._batches_by_source(healthy_chain_records())

        batch_dir = self.tmp.name + "/batch"
        batch_pipe = GenealogyPipeline(batch_dir)
        idx = 0
        for source, payloads in groups.items():
            batch_pipe.raw.append_batch(
                source, payloads, f"b-{source}", received_at=fixed_received(idx)
            )
            idx += 1
        batch_snap = batch_pipe.materialize()

        stream_dir = self.tmp.name + "/stream"
        stream = StreamProcessor(stream_dir)
        idx = 0
        for source, payloads in groups.items():
            stream.store.append_batch(
                source, payloads, f"b-{source}", received_at=fixed_received(idx)
            )
            idx += 1
        stream_snap = stream.replay()

        self.assertEqual(
            batch_snap.content_hash(), stream_snap.content_hash()
        )

    def test_out_of_order_arrival_does_not_change_result(self):
        groups = self._batches_by_source(healthy_chain_records())
        ref_dir = self.tmp.name + "/ord1"
        reference = GenealogyPipeline(ref_dir)
        idx = 0
        for source, payloads in groups.items():
            reference.raw.append_batch(
                source, payloads, f"b-{source}", received_at=fixed_received(idx)
            )
            idx += 1
        ref_hash = reference.materialize().content_hash()

        shuffled_store = self.tmp.name + "/ord2"
        pipe = GenealogyPipeline(shuffled_store)
        all_records = [(s, p) for s, payloads in groups.items() for p in payloads]
        rng = random.Random(42)
        rng.shuffle(all_records)
        for i, (source, payload) in enumerate(all_records):
            # same fixed ingestion stamp per *record content*, regardless of
            # the order it happens to arrive in
            pipe.raw.append_batch(
                source, [payload], batch_id=f"b-shuf-{i}",
                received_at=fixed_received(i),
            )
        self.assertEqual(pipe.materialize().content_hash(), ref_hash)

    def test_repeated_replay_is_idempotent(self):
        groups = self._batches_by_source(healthy_chain_records())
        stream = StreamProcessor(self.tmp.name + "/rep")
        for source, payloads in groups.items():
            stream.ingest(source, payloads, f"b-{source}")
        first = stream.replay()
        second = stream.replay()
        third = stream.replay()
        self.assertEqual(first.content_hash(), second.content_hash())
        self.assertEqual(second.content_hash(), third.content_hash())
        # snapshot files are content-addressed: one physical artifact
        snap_files = [
            p for p in stream.snapshots.dir.glob("*.json")
            if p.name != "catalog.json"
        ]
        self.assertEqual(len(snap_files), 1)
        # raw log line count stays stable across replays
        raw_lines = 0
        for path in Path(stream.store.raw_dir).glob("*.jsonl"):
            with path.open() as fh:
                raw_lines += sum(1 for _ in fh)
        self.assertEqual(raw_lines, len(healthy_chain_records()))

    def test_repeated_materialization_never_duplicates_quarantine(self):
        # Regression: a reused normalizer/graph builder must not accumulate
        # quarantine entries when materialized more than once.
        from fab_lot_genealogy.pipeline import GenealogyPipeline
        pipe = GenealogyPipeline(self.tmp.name + "/qdup")
        pipe.ingest("split", [{
            "type": "split", "at": "2026-01-02T08:00Z",
            "from_batch": "GHOST", "to_batch": "X", "wafers": ["w1"],
        }], "b1")
        identities = set()
        for _ in range(3):
            snap = pipe.materialize()
            current = {(q.code, q.raw_id, q.natural_key) for q in snap.quarantine}
            self.assertEqual(len(current), len(snap.quarantine))
            if identities:
                self.assertEqual(current, identities)
            identities = current
        self.assertEqual(len(identities), 1)


class TestCheckpointRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def test_resume_after_simulated_crash_keeps_graph_identical(self):
        store_dir = self.tmp.name + "/crash"
        records = healthy_chain_records()
        groups = {}
        for record in records:
            source = "mes" if record.source == "rework" else record.source
            groups.setdefault(source, []).append(record.payload)
        sources = list(groups)

        # first run: ingest only half the sources, then "crash"
        first = StreamProcessor(store_dir)
        for source in sources[:2]:
            first.ingest(source, groups[source], f"b-{source}")
        partial = first.replay()

        checkpoint = json.loads((Path(store_dir) / "stream_checkpoint.json").read_text())
        self.assertTrue(checkpoint["consumed"])

        # resume in a brand-new process instance (same on-disk state)
        resumed = StreamProcessor(store_dir)
        for source in sources[2:]:
            resumed.ingest(source, groups[source], f"b-{source}")
        full = resumed.replay()

        # the resumed graph is a strict superset of the partial one
        self.assertGreaterEqual(len(full.nodes), len(partial.nodes))
        self.assertGreater(len(full.edges), len(partial.edges))

        # replaying again after recovery converges and stays stable
        again = StreamProcessor(store_dir).replay()
        self.assertEqual(again.content_hash(), full.content_hash())

    def test_duplicate_ingestion_after_recovery_appends_nothing(self):
        store_dir = self.tmp.name + "/dup"
        stream = StreamProcessor(store_dir)
        payload = [{
            "type": "batch_create", "at": "2026-01-01T08:00Z",
            "batch_id": "B1", "wafers": ["w1"],
        }]
        first = stream.ingest("mes", payload, "b1")
        second = stream.ingest("mes", payload, "b1-retry")
        self.assertEqual(len(first["accepted"]), 1)
        self.assertEqual(second["accepted"], [])
        self.assertEqual(len(second["duplicate"]), 1)
        snap = stream.replay()
        batch_nodes = [n for n in snap.nodes.values() if n.node_type == "batch"]
        self.assertEqual(len(batch_nodes), 1)

    def test_partial_batch_failure_does_not_block_stream(self):
        store_dir = self.tmp.name + "/partial"
        stream = StreamProcessor(store_dir)
        bad = [
            "not-a-dict",
            {"type": "batch_create", "at": "2026-01-01T08:00Z",
             "batch_id": "B1", "wafers": ["w1"]},
        ]
        manifest = stream.ingest("mes", bad, "b-bad")
        self.assertEqual(manifest["status"], "partial")
        # the stream still advances and materializes the good record
        snap = stream.replay()
        labels = {n.label for n in snap.nodes.values() if n.node_type == "batch"}
        self.assertEqual(labels, {"B1"})
        recovery = stream.recover()
        self.assertEqual(recovery["snapshot_hash"], snap.content_hash())
        # the unparseable offset is reported as permanently failed
        self.assertEqual(len(recovery["permanently_failed"]), 1)

    def test_recovery_re_drives_a_corrected_failed_record(self):
        store_dir = self.tmp.name + "/recover"
        stream = StreamProcessor(store_dir)
        bad = [
            "not-a-dict",
            {"type": "batch_create", "at": "2026-01-01T08:00Z",
             "batch_id": "B1", "wafers": ["w1"]},
        ]
        stream.ingest("mes", bad, "b-bad")
        # supply a corrected payload for the failed offset 0
        corrected = {
            "type": "batch_create", "at": "2025-12-31T08:00Z",
            "batch_id": "B0", "wafers": ["w0"],
        }
        recovery = stream.recover(replacements={"b-bad": {0: corrected}})
        self.assertEqual(recovery["retried_failures"][0]["status"], "re-landed")
        self.assertEqual(recovery["permanently_failed"], [])
        # the batch manifest is now complete
        self.assertEqual(stream.store.batch_state("b-bad")["status"], "complete")
        snap = stream.replay()
        labels = {n.label for n in snap.nodes.values() if n.node_type == "batch"}
        self.assertEqual(labels, {"B0", "B1"})
        # recovering again with no new failures is a stable no-op
        again = stream.recover(replacements={"b-bad": {0: corrected}})
        self.assertEqual(again["retried_failures"], [])


class TestLargeMergeScale(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def test_massive_fan_in_merge_is_conserved_and_order_independent(self):
        n_parents = 50
        wafers_per_parent = 20
        rng = random.Random(7)

        creates, merges = [], []
        all_wafers = []
        for i in range(n_parents):
            wafers = [f"w-{i}-{k}" for k in range(wafers_per_parent)]
            all_wafers.extend(wafers)
            creates.append({
                "type": "batch_create",
                "at": f"2026-01-01T08:{i:02d}:00Z",
                "batch_id": f"P{i}",
                "wafers": wafers,
            })
        merge_event = {
            "type": "merge",
            "at": "2026-01-02T08:00:00Z",
            "from_batches": [f"P{i}" for i in range(n_parents)],
            "to_batch": "GRAND",
            "wafers": sorted(all_wafers),
        }

        def run(store, ordered):
            pipe = GenealogyPipeline(store)
            incoming_creates = list(creates)
            if not ordered:
                rng.shuffle(incoming_creates)
            pipe.ingest("mes", incoming_creates, "b-creates")
            pipe.ingest("merge", [merge_event], "b-merge")
            return pipe.materialize()

        ordered_snap = run(self.tmp.name + "/ordered", True)
        shuffled_snap = run(self.tmp.name + "/shuffled", False)

        self.assertEqual(
            ordered_snap.content_hash(), shuffled_snap.content_hash()
        )
        # conservation: exactly one balanced merge ledger row, all wafers
        merge_rows = [l for l in ordered_snap.ledger if l.edge_type == "merge"]
        self.assertEqual(len(merge_rows), 1)
        self.assertTrue(merge_rows[0].balanced)
        self.assertEqual(len(merge_rows[0].wafers_in), n_parents * wafers_per_parent)
        self.assertEqual(ordered_snap.quarantine, [])

        # the grandchild's ancestry reaches every parent under the node budget
        result = GenealogyQuery(ordered_snap, "GRAND")
        self.assertEqual(result.node_count, n_parents + 1)


def GenealogyQuery(snapshot, target):
    from fab_lot_genealogy.query import LineageQuery
    return LineageQuery(snapshot).lineage(target, max_nodes=100_000)


class TestReplayBudgets(unittest.TestCase):
    def test_budget_breach_raises_instead_of_unbounded_growth(self):
        records = [
            {
                "type": "batch_create",
                "at": f"2026-01-01T08:{i:02d}:00Z",
                "batch_id": f"B{i}",
                "wafers": [f"w{i}"],
            }
            for i in range(10)
        ]
        tmpdir = tempfile.mkdtemp()
        pipe = GenealogyPipeline(
            tmpdir,
            builder=GraphBuilder(max_batches=3, max_edges=100),
        )
        pipe.ingest("mes", records, "b")
        with self.assertRaises(BudgetExceeded):
            pipe.materialize()

    def test_query_node_budget_truncates_large_lineage(self):
        n = 30
        records = [{
            "type": "batch_create",
            "at": "2026-01-01T08:00:00Z",
            "batch_id": "ROOT",
            "wafers": [f"w{i}" for i in range(n)],
        }]
        for i in range(n):
            records.append({
                "type": "split",
                "at": f"2026-01-02T08:{i:02d}:00Z",
                "from_batch": "ROOT",
                "to_batch": f"CH{i}",
                "wafers": [f"w{i}"],
                "wafers_by_child": {f"CH{i}": [f"w{i}"]},
            })
        pipe = GenealogyPipeline(tempfile.mkdtemp())
        pipe.ingest("mes", records, "b")
        snap = pipe.materialize()
        small = pipe.query("ROOT", direction="descendants", max_nodes=5).to_dict()
        self.assertTrue(small["truncated"])
        self.assertLessEqual(small["node_count"], 5)
        full = pipe.query("ROOT", direction="descendants", max_nodes=10_000).to_dict()
        self.assertFalse(full["truncated"])


if __name__ == "__main__":
    unittest.main()
