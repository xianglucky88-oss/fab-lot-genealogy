"""Graph property tests driven by the shared oracle battery (图性质测试)."""
import random
import unittest

import oracle_battery
import oracle_diff
import oracle_gen
import oracle_graph as oracle
import oracle_shrink
from fab_lot_genealogy import engine


class TestOracleBattery(unittest.TestCase):
    def test_real_implementation_passes_full_battery(self):
        failures = oracle_battery.run_battery(oracle_battery.real_impl())
        self.assertEqual([], [f.check for f in failures],
                         msg="\n\n".join(f.report for f in failures[:3]))


class TestGraphInvariants(unittest.TestCase):
    def test_no_successor_misreported_as_ancestor_on_dags(self):
        rng = random.Random(41)
        for _ in range(150):
            rows = oracle_gen.gen_rows(
                rng, n_batches=rng.randrange(2, 9),
                n_ops=rng.randrange(0, 15),
                wafers=("W1", "W2"), dag=True, duplicate_rate=0.2)
            graph = oracle.OracleGraph(rows)
            data = engine.calculate(rows, {})
            for batch, ancestors in data["layers"].items():
                self.assertTrue(
                    set(ancestors).isdisjoint(graph.descendants_of(batch)),
                    "batch %s: ancestors %s overlap descendants" % (batch, ancestors))

    def test_ancestor_descendant_duality(self):
        rng = random.Random(42)
        for _ in range(150):
            rows = oracle_gen.gen_rows(
                rng, n_batches=rng.randrange(2, 8), n_ops=rng.randrange(0, 14),
                wafers=("W1", "W2"), self_loops=True)
            graph = oracle.OracleGraph(rows)
            data = engine.calculate(rows, {})
            for batch, ancestors in data["layers"].items():
                for other in data["layers"]:
                    self.assertEqual(
                        other in ancestors,
                        batch in graph.descendants_of(other),
                        "duality broken between %s and %s" % (batch, other))

    def test_ancestors_are_subset_of_known_batches_minus_self(self):
        rng = random.Random(43)
        for _ in range(150):
            rows = oracle_gen.gen_rows(
                rng, n_batches=rng.randrange(2, 8), n_ops=rng.randrange(0, 14),
                wafers=("W1", "W2"), self_loops=True)
            data = engine.calculate(rows, {})
            known = set(data["layers"])
            for batch, ancestors in data["layers"].items():
                self.assertTrue(set(ancestors) <= known - {batch})

    def test_operation_count_equals_distinct_operations(self):
        rng = random.Random(44)
        for _ in range(150):
            rows = oracle_gen.gen_rows(
                rng, n_batches=rng.randrange(2, 8), n_ops=rng.randrange(0, 14),
                wafers=("W1", "W2"), duplicate_rate=0.5)
            data = engine.calculate(rows, {})
            self.assertEqual(data["operation_count"],
                             len({oracle.op_key(r) for r in rows}))

    def test_permutation_and_duplicate_stability_with_shrink_on_failure(self):
        rng = random.Random(45)

        def stable(payload):
            rows = payload["operations"]
            baseline = engine.calculate([dict(r) for r in rows], payload)
            local = random.Random(99)
            for _ in range(6):
                permuted = [dict(r) for r in rows]
                local.shuffle(permuted)
                permuted += [dict(r) for r in local.sample(rows, min(2, len(rows)))]
                if engine.calculate(permuted, payload) != baseline:
                    return False
            return True

        for _ in range(60):
            payload = oracle_gen.gen_payload(
                rng, n_batches=rng.randrange(2, 8), n_ops=rng.randrange(1, 14),
                wafers=("W1", "W2"), self_loops=True, duplicate_rate=0.3,
                equal_ts_rate=0.5)
            if not stable(payload):
                shrunk = oracle_shrink.shrink_payload(
                    payload, lambda p: not stable(p))
                self.fail("instability, minimal counterexample:\n"
                          + oracle_diff.format_operations(
                              shrunk["operations"]))

    def test_wafer_filter_never_leaks_other_wafers(self):
        rng = random.Random(46)
        for _ in range(150):
            rows = oracle_gen.gen_rows(
                rng, n_batches=rng.randrange(2, 8), n_ops=rng.randrange(0, 14),
                wafers=("W1", "W2", "W3"), self_loops=True)
            wafer = rng.choice(("W1", "W2", "W3"))
            data = engine.calculate(rows, {"wafer": wafer})
            scoped = [r for r in rows if r["wafer"] == wafer]
            graph = oracle.OracleGraph(scoped)
            self.assertEqual(set(data["layers"]), graph.batches)
            self.assertEqual(data["operation_count"], len(graph.rows))


if __name__ == "__main__":
    unittest.main()
