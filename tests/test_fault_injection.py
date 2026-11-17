"""Fault injection: corrupted records, hostile payloads, boundary abuse
(故障注入)."""
import json
import random
import unittest

import oracle_gen
import oracle_graph as oracle
from fab_lot_genealogy import engine, pipeline, report

OP = oracle_gen.make_op
T = oracle_gen.stamp


class TestParserFaultInjection(unittest.TestCase):
    def test_every_corrupt_category_is_rejected_with_reason(self):
        rng = random.Random(51)
        good = OP(T(0), "B0", "B1")
        for _ in range(60):
            row = oracle_gen.corrupt_row(rng, good)
            payload = {"operations": [good, row]}
            result = pipeline.run(payload)
            self.assertEqual(result["status"], "ok")
            self.assertEqual(len(result["rejected_operations"]), 1, row)
            entry = result["rejected_operations"][0]
            self.assertEqual(entry["index"], 1)
            self.assertTrue(entry["reason"], row)

    def test_all_rows_invalid_still_returns_empty_result(self):
        payload = {"operations": [None, 42, {"at": "bad"}, {"kind": "nope"}]}
        result = pipeline.run(payload)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["latest_batch"], None)
        self.assertEqual(result["ancestors"], [])
        self.assertEqual(result["operation_count"], 0)
        self.assertEqual(result["layers"], {})
        self.assertEqual(len(result["rejected_operations"]), 4)

    def test_extra_fields_are_preserved_by_parser_ignored_by_engine(self):
        row = dict(OP(T(0), "B0", "B1"), tool="etcher-7", note="ok")
        result = pipeline.run({"operations": [row]})
        self.assertEqual(result["operation_count"], 1)
        self.assertEqual(result["rejected_operations"], [])

    def test_operations_container_type_faults(self):
        for bad in (None, "ops", 3.14, {}, True):
            with self.assertRaises(ValueError):
                pipeline.run({"operations": bad})


class TestEngineFaultInjection(unittest.TestCase):
    def test_malformed_rows_raise_value_error_naming_the_record(self):
        bad_rows = [
            [{"at": T(0), "from_batch": "B0"}],                      # missing keys
            [dict(OP(T(0), "B0", "B1"), wafer=None)],               # wrong type
            [dict(OP(T(0), "B0", "B1"), at="yesterday")],           # bad instant
            [dict(OP(T(0), "B0", "B1"), from_batch="")],            # empty name
            ["not-a-row"],                                          # non-dict
        ]
        for rows in bad_rows:
            with self.assertRaises(ValueError, msg=rows):
                engine.calculate(rows, {})

    def test_deep_chain_does_not_crash(self):
        n = 5000
        rows = [OP(T(i), "B%d" % i, "B%d" % (i + 1)) for i in range(n)]
        data = engine.calculate(rows, {})
        self.assertEqual(len(data["layers"]["B%d" % n]), n)

    def test_cycle_and_duplicate_storm_terminates(self):
        rng = random.Random(52)
        rows = []
        batches = ["B%d" % i for i in range(8)]
        for a in batches:
            for b in batches:
                rows.append(OP(T(0), a, b, wafer="W1", kind="rework"))
                rows.append(OP(T(0), a, b, wafer="W1", kind="rework"))
        rng.shuffle(rows)
        data = engine.calculate(rows, {})
        self.assertEqual(data["operation_count"], 64)
        for batch, ancestors in data["layers"].items():
            self.assertNotIn(batch, ancestors)
            self.assertEqual(len(ancestors), 7)


class TestReportFaultInjection(unittest.TestCase):
    def test_hostile_data_keys_cannot_forge_envelope(self):
        result = report.build_result(
            "fab-lot-genealogy",
            {"status": "failed", "project": "other-project", "extra": 1})
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["project"], "fab-lot-genealogy")
        self.assertEqual(result["extra"], 1)

    def test_envelope_is_faithful_for_non_serializable_values(self):
        marker = object()
        result = report.build_result("proj", {"blob": marker})
        self.assertIs(result["blob"], marker)  # boundary is faithful
        with self.assertRaises(TypeError):
            json.dumps(result)  # serialization failure is explicit, not silent


class TestPipelineFaultInjection(unittest.TestCase):
    def test_half_broken_payload_still_auditable(self):
        rng = random.Random(53)
        valid = oracle_gen.gen_rows(rng, n_batches=5, n_ops=8, wafers=("W1",))
        broken = [oracle_gen.corrupt_row(rng, row) for row in valid]
        mixed = [row for pair in zip(valid, broken) for row in pair]
        rng.shuffle(mixed)
        result = pipeline.run({"operations": mixed})
        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(result["rejected_operations"]), len(broken))
        expected = oracle.oracle_calculate(valid)
        for key in ("latest_batch", "ancestors", "operation_count", "layers"):
            self.assertEqual(result[key], expected[key])

    def test_result_survives_json_round_trip(self):
        rng = random.Random(54)
        payload = oracle_gen.gen_payload(rng, n_batches=5, n_ops=10,
                                         bad_rate=0.3, duplicate_rate=0.3)
        result = pipeline.run(payload)
        self.assertEqual(json.loads(json.dumps(result, sort_keys=True)), result)


if __name__ == "__main__":
    unittest.main()
