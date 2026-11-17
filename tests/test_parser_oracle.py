"""Input-parsing layer vs the independent oracle (输入解析层)."""
import random
import unittest

import oracle_gen
import oracle_graph as oracle
from fab_lot_genealogy import parser


class TestParserOracle(unittest.TestCase):
    def test_valid_payload_matches_oracle(self):
        rng = random.Random(11)
        for _ in range(50):
            payload = oracle_gen.gen_payload(rng, n_batches=5, n_ops=8)
            expected, rejected = oracle.oracle_parse(payload)
            self.assertEqual(rejected, [])
            self.assertEqual(parser.parse_operations(payload), (expected, []))

    def test_operations_must_be_a_list(self):
        for bad in (None, "x", 1, 1.5, {}, ()):
            with self.assertRaises(ValueError):
                parser.parse_operations({"operations": bad})
            with self.assertRaises(ValueError):
                oracle.oracle_parse({"operations": bad})

    def test_missing_operations_key_is_empty(self):
        self.assertEqual(parser.parse_operations({}), ([], []))
        self.assertEqual(parser.load_records({}), [])

    def test_non_dict_rows_are_rejected_not_dropped_silently(self):
        payload = {"operations": [None, 42, "oops",
                                  oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")]}
        valid, rejected = parser.parse_operations(payload)
        self.assertEqual(len(valid), 1)
        self.assertEqual([r["index"] for r in rejected], [0, 1, 2])
        self.assertTrue(all(r["reason"] == "row is not an object" for r in rejected))

    def test_each_missing_field_is_rejected_with_reason(self):
        good = oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")
        for field in oracle.FIELDS:
            row = {k: v for k, v in good.items() if k != field}
            _, rejected = parser.parse_operations({"operations": [row]})
            self.assertEqual(len(rejected), 1)
            self.assertEqual(rejected[0]["reason"], "missing field: %s" % field)
            self.assertEqual(rejected[0]["index"], 0)

    def test_wrong_field_types_are_rejected(self):
        good = oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")
        for field in oracle.FIELDS:
            for bad_value in (1, 1.5, None, [], {}):
                row = dict(good, **{field: bad_value})
                _, rejected = parser.parse_operations({"operations": [row]})
                self.assertEqual(len(rejected), 1, (field, bad_value))
                self.assertEqual(rejected[0]["reason"],
                                 "field %s must be a string" % field)

    def test_empty_names_are_rejected(self):
        good = oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")
        for field, reason in (("from_batch", "from_batch must be non-empty"),
                              ("to_batch", "to_batch must be non-empty"),
                              ("wafer", "wafer must be non-empty")):
            _, rejected = parser.parse_operations(
                {"operations": [dict(good, **{field: ""})]})
            self.assertEqual([r["reason"] for r in rejected], [reason])

    def test_illegal_kind_is_rejected(self):
        good = oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")
        for kind in ("teleport", "SPLIT", "", "merge2", " split"):
            _, rejected = parser.parse_operations(
                {"operations": [dict(good, kind=kind)]})
            self.assertEqual(len(rejected), 1, kind)
            self.assertEqual(rejected[0]["reason"], "illegal kind: %r" % kind)

    def test_bad_timestamps_are_rejected(self):
        good = oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")
        for at in ("nope", "2026-13-01T00:00:00Z", "2026-01-01", "10:00:00",
                   "", "2026-01-01T25:00:00Z"):
            _, rejected = parser.parse_operations(
                {"operations": [dict(good, at=at)]})
            self.assertEqual(len(rejected), 1, at)
            self.assertEqual(rejected[0]["reason"],
                             "invalid timestamp: %r" % at)

    def test_equivalent_instants_are_accepted(self):
        good = oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")
        for at in ("2026-01-01T10:00:00Z", "2026-01-01T18:00:00+08:00",
                   "2026-01-01T10:00:00", "2026-01-01T10:00:00.5Z",
                   "2026-01-01 10:00:00+0000"):
            valid, rejected = parser.parse_operations(
                {"operations": [dict(good, at=at)]})
            self.assertEqual(rejected, [], at)
            self.assertEqual(len(valid), 1)

    def test_partial_failure_keeps_valid_rows(self):
        rng = random.Random(12)
        for _ in range(50):
            payload = oracle_gen.gen_payload(
                rng, n_batches=5, n_ops=10, bad_rate=0.4)
            self.assertEqual(parser.parse_operations(payload),
                             oracle.oracle_parse(payload))

    def test_parser_preserves_duplicates_and_order(self):
        row = oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")
        payload = {"operations": [dict(row), dict(row), dict(row)]}
        valid, _ = parser.parse_operations(payload)
        self.assertEqual(len(valid), 3)  # dedup is the engine's concern

    def test_input_payload_is_not_mutated_and_rows_are_copied(self):
        row = oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")
        payload = {"operations": [row]}
        valid, _ = parser.parse_operations(payload)
        valid[0]["from_batch"] = "HACKED"
        self.assertEqual(payload["operations"][0]["from_batch"], "B0")

    def test_load_records_backward_compatible(self):
        rng = random.Random(13)
        payload = oracle_gen.gen_payload(rng, n_batches=4, n_ops=8, bad_rate=0.4)
        valid, _ = oracle.oracle_parse(payload)
        self.assertEqual(parser.load_records(payload), valid)


if __name__ == "__main__":
    unittest.main()
