"""Minimal counterexample reduction and divergence reporting
(最小反例缩减与差异报告)."""
import unittest

import oracle_battery
import oracle_diff
import oracle_gen
import oracle_graph as oracle
import oracle_shrink
from fab_lot_genealogy import engine

OP = oracle_gen.make_op
T = oracle_gen.stamp


def _duplicate_counting_calculate(rows, payload):
    """Fault-injected calculate: counts redelivered operations twice."""
    data = engine.calculate(rows, payload)
    data["operation_count"] = len(rows)
    return data


def _wafer_blind_calculate(rows, payload):
    """Fault-injected calculate: ignores the wafer filter."""
    return engine.calculate(rows, {})


def _run_with(calculate):
    return oracle_battery.compose(
        lambda p: oracle.oracle_parse(p), calculate,
        oracle.oracle_envelope)


class TestShrink(unittest.TestCase):
    def test_shrinks_duplicate_count_to_two_operations(self):
        rows = [OP(T(0), "B0", "B1"), OP(T(1), "B1", "B2"), OP(T(2), "B2", "B3"),
                OP(T(3), "B3", "B4")]
        payload = {"operations": rows + [dict(rows[1]), dict(rows[1])]}
        impl = oracle_battery.Impl(
            oracle.oracle_parse, _duplicate_counting_calculate,
            oracle.oracle_envelope, _run_with(_duplicate_counting_calculate))

        def fails(p):
            return oracle_battery.compare_payload(impl, p, "shrink") is not None

        self.assertTrue(fails(payload))
        shrunk = oracle_shrink.shrink_payload(payload, fails)
        self.assertTrue(fails(shrunk))
        self.assertEqual(len(shrunk["operations"]), 2)
        self.assertEqual(shrunk["operations"][0], shrunk["operations"][1])
        self.assertTrue(oracle_shrink.is_minimal(shrunk, fails))

    def test_shrinks_wafer_leak_to_single_cross_wafer_operation(self):
        rows = [OP(T(0), "B0", "B1", wafer="W1"),
                OP(T(1), "B1", "B2", wafer="W1"),
                OP(T(2), "B9", "B8", wafer="W2"),
                OP(T(3), "B2", "B3", wafer="W1")]
        payload = {"operations": rows, "wafer": "W1"}
        impl = oracle_battery.Impl(
            oracle.oracle_parse, _wafer_blind_calculate,
            oracle.oracle_envelope, _run_with(_wafer_blind_calculate))

        def fails(p):
            return oracle_battery.compare_payload(impl, p, "shrink") is not None

        shrunk = oracle_shrink.shrink_payload(payload, fails)
        self.assertTrue(fails(shrunk))
        self.assertEqual(len(shrunk["operations"]), 1)
        self.assertEqual(shrunk["operations"][0]["wafer"], "W2")
        self.assertTrue(oracle_shrink.is_minimal(shrunk, fails))

    def test_shrinker_rejects_non_failing_initial_payload(self):
        with self.assertRaises(ValueError):
            oracle_shrink.shrink_payload({"operations": []}, lambda p: False)


class TestDivergenceReport(unittest.TestCase):
    def test_report_names_from_to_and_operation_sequence(self):
        payload = {"operations": [OP(T(0), "B0", "B1"), OP(T(1), "B1", "B2")]}
        expected = oracle.oracle_calculate(payload["operations"])
        actual = dict(expected)
        actual["layers"] = dict(expected["layers"], B2=["B0", "B1", "B1"])
        report = oracle_diff.explain_divergence(payload, expected, actual, "demo")
        self.assertIn("DIVERGENCE demo", report)
        self.assertIn("from_batch=B1", report)
        self.assertIn("to_batch=B2", report)
        self.assertIn("operation sequence", report)
        self.assertIn("[0] at=2026-01-01T00:00:00Z", report)
        self.assertIn("layers['B2']", report)

    def test_report_marks_missing_batches(self):
        payload = {"operations": [OP(T(0), "B9", "B1")]}
        expected = oracle.oracle_calculate(payload["operations"])
        actual = dict(expected)
        actual["layers"] = {"B1": ["B9"]}  # source B9 dropped
        report = oracle_diff.explain_divergence(payload, expected, actual, "src")
        self.assertIn("layers['B9']", report)
        self.assertIn("from_batch=B9", report)

    def test_parse_divergence_report_lists_rejections(self):
        payload = {"operations": [None, OP(T(0), "B0", "B1")]}
        expected = oracle.oracle_parse(payload)
        actual = ([dict(OP(T(0), "B0", "B1"))], [])  # silently dropped
        report = oracle_diff.explain_parse_divergence(
            payload, expected, actual, "parse")
        self.assertIn("rejections", report)
        self.assertIn("row is not an object", report)
        self.assertIn("operation sequence", report)


if __name__ == "__main__":
    unittest.main()
