"""Domain-computation layer vs the independent oracle (领域计算层)."""
import random
import unittest

import oracle_gen
import oracle_graph as oracle
from fab_lot_genealogy import engine

OP = oracle_gen.make_op
T = oracle_gen.stamp


def calculate(rows, payload=None):
    return engine.calculate(rows, payload if payload is not None else {})


class TestEngineOracle(unittest.TestCase):
    def test_repeated_identical_operation_counted_once(self):
        rows = [OP(T(0), "B0", "B1") for _ in range(5)]
        data = calculate(rows)
        self.assertEqual(data["operation_count"], 1)
        self.assertEqual(data["ancestors"], ["B0"])
        self.assertEqual(data["layers"], {"B0": [], "B1": ["B0"]})

    def test_input_reorder_invariance(self):
        rows = [
            OP(T(0), "B0", "B1"), OP(T(1), "B0", "B2"),
            OP(T(2), "B1", "B3"), OP(T(3), "B2", "B3"),
            OP(T(4), "B3", "B4"), OP(T(4), "B4", "B5"),
        ]
        baseline = calculate(rows)
        rng = random.Random(21)
        for _ in range(30):
            permuted = list(rows)
            rng.shuffle(permuted)
            self.assertEqual(calculate(permuted), baseline)

    def test_split_fanout(self):
        data = calculate([OP(T(0), "B0", "B1"), OP(T(1), "B0", "B2")])
        self.assertEqual(data["layers"]["B1"], ["B0"])
        self.assertEqual(data["layers"]["B2"], ["B0"])
        self.assertEqual(data["layers"]["B0"], [])

    def test_merge_fanin(self):
        data = calculate([OP(T(0), "B1", "B3"), OP(T(1), "B2", "B3")])
        self.assertEqual(sorted(data["layers"]["B3"]), ["B1", "B2"])

    def test_diamond_split_then_merge(self):
        data = calculate([
            OP(T(0), "B0", "B1"), OP(T(0), "B0", "B2"),
            OP(T(1), "B1", "B3"), OP(T(1), "B2", "B3"),
        ])
        self.assertEqual(data["layers"]["B3"], ["B0", "B1", "B2"])

    def test_unknown_source_has_empty_ancestry_and_appears_in_layers(self):
        data = calculate([OP(T(0), "B9", "B1"), OP(T(1), "B1", "B2")])
        self.assertIn("B9", data["layers"])
        self.assertEqual(data["layers"]["B9"], [])

    def test_cycle_terminates_and_excludes_self(self):
        rows = [OP(T(0), "B0", "B1", kind="rework"),
                OP(T(1), "B1", "B2", kind="rework"),
                OP(T(2), "B2", "B1", kind="rework")]
        data = calculate(rows)
        self.assertEqual(data["layers"]["B1"], ["B0", "B2"])
        self.assertEqual(data["layers"]["B2"], ["B0", "B1"])
        self.assertNotIn("B1", data["layers"]["B1"])
        self.assertNotIn("B2", data["layers"]["B2"])

    def test_self_loop_is_not_its_own_ancestor(self):
        data = calculate([OP(T(0), "B1", "B1", kind="rework")])
        self.assertEqual(data["layers"], {"B1": []})
        self.assertEqual(data["operation_count"], 1)

    def test_empty_operations(self):
        data = calculate([])
        self.assertEqual(data, {"latest_batch": None, "ancestors": [],
                                "operation_count": 0, "layers": {}})

    def test_wafer_filter_scopes_the_graph(self):
        rows = [OP(T(0), "B0", "B1", wafer="W1"),
                OP(T(1), "B1", "B2", wafer="W2")]
        scoped = calculate(rows, {"wafer": "W1"})
        self.assertEqual(scoped["latest_batch"], "B1")
        self.assertEqual(scoped["ancestors"], ["B0"])
        self.assertEqual(scoped["operation_count"], 1)
        self.assertNotIn("B2", scoped["layers"])
        full = calculate(rows)
        self.assertEqual(full["ancestors"], ["B0", "B1"])

    def test_cross_wafer_merge_filtered(self):
        rows = [OP(T(0), "B0", "B2", wafer="W1", kind="merge"),
                OP(T(0), "B1", "B2", wafer="W2", kind="merge")]
        scoped = calculate(rows, {"wafer": "W2"})
        self.assertEqual(scoped["layers"]["B2"], ["B1"])
        self.assertEqual(calculate(rows)["layers"]["B2"], ["B0", "B1"])

    def test_equal_timestamps_are_deterministic(self):
        rows = [OP(T(7), "B0", "B3"), OP(T(7), "B1", "B3"),
                OP(T(7), "B2", "B3"), OP(T(7), "B0", "B1")]
        expected = oracle.oracle_calculate(rows)
        rng = random.Random(22)
        for _ in range(20):
            permuted = list(rows)
            rng.shuffle(permuted)
            self.assertEqual(calculate(permuted), expected)

    def test_same_instant_different_offsets(self):
        rows = [OP("2026-01-01T10:00:00Z", "B0", "B1"),
                OP("2026-01-01T18:00:00+08:00", "B1", "B2"),
                OP("2026-01-01T08:00:00Z", "B2", "B3")]
        expected = oracle.oracle_calculate(rows)
        rng = random.Random(23)
        for _ in range(10):
            permuted = list(rows)
            rng.shuffle(permuted)
            self.assertEqual(calculate(permuted), expected)
        self.assertIn(calculate(rows)["latest_batch"], {"B1", "B2"})

    def test_latest_uses_time_not_input_order(self):
        rows = [OP(T(2), "B2", "B3"), OP(T(0), "B0", "B1"), OP(T(1), "B1", "B2")]
        data = calculate(rows)
        self.assertEqual(data["latest_batch"], "B3")
        self.assertEqual(data["ancestors"], ["B0", "B1", "B2"])

    def test_no_successor_misreported_as_ancestor_on_dags(self):
        rng = random.Random(24)
        for _ in range(60):
            rows = oracle_gen.gen_rows(rng, n_batches=7, n_ops=12,
                                       wafers=("W1", "W2"), dag=True)
            graph = oracle.OracleGraph(rows)
            data = calculate(rows)
            for batch, ancestors in data["layers"].items():
                descendants = graph.descendants_of(batch)
                self.assertTrue(set(ancestors).isdisjoint(descendants),
                                (batch, ancestors, descendants))

    def test_ancestor_sets_are_sorted_and_typed(self):
        rng = random.Random(25)
        for _ in range(40):
            rows = oracle_gen.gen_rows(rng, n_batches=6, n_ops=10,
                                       self_loops=True)
            data = calculate(rows)
            for ancestors in data["layers"].values():
                self.assertEqual(ancestors, sorted(ancestors))
            self.assertEqual(data["ancestors"], sorted(data["ancestors"]))

    def test_long_chain_does_not_overflow_recursion(self):
        n = 3000
        rows = [OP(T(i), "B%d" % i, "B%d" % (i + 1)) for i in range(n)]
        data = calculate(rows)
        self.assertEqual(len(data["layers"]["B%d" % n]), n)
        self.assertEqual(data["operation_count"], n)

    def test_malformed_rows_raise_explainable_error(self):
        with self.assertRaises(ValueError) as ctx:
            calculate([{"at": T(0), "from_batch": "B0"}])
        self.assertIn("record 0", str(ctx.exception))
        with self.assertRaises(ValueError):
            calculate([OP(T(0), "B0", "B1"), "not-a-row"])
        with self.assertRaises(ValueError):
            calculate([dict(OP(T(0), "B0", "B1"), at="not-a-time")])

    def test_layers_cover_every_known_batch(self):
        rng = random.Random(26)
        for _ in range(40):
            rows = oracle_gen.gen_rows(rng, n_batches=6, n_ops=10,
                                       self_loops=True)
            data = calculate(rows)
            touched = {b for row in rows for b in
                       (row["from_batch"], row["to_batch"])}
            self.assertEqual(set(data["layers"]), touched)


if __name__ == "__main__":
    unittest.main()
