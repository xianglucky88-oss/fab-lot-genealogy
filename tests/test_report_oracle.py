"""Result-encapsulation layer vs the independent oracle (结果封装层)."""
import dataclasses
import json
import unittest

import oracle_graph as oracle
from fab_lot_genealogy import report
from fab_lot_genealogy.models import PipelineResult


class TestReportOracle(unittest.TestCase):
    def test_envelope_matches_oracle(self):
        data = {"latest_batch": "B3", "ancestors": ["B0"], "operation_count": 2,
                "layers": {"B0": []}, "rejected_operations": []}
        self.assertEqual(report.build_result("proj", data),
                         oracle.oracle_envelope("proj", data))

    def test_envelope_fields_are_authoritative_over_data_keys(self):
        data = {"status": "corrupt", "project": "forgery", "k": 7}
        result = report.build_result("fab-lot-genealogy", data)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["project"], "fab-lot-genealogy")
        self.assertEqual(result["k"], 7)

    def test_every_data_key_is_preserved(self):
        data = {"a": 1, "b": [1, 2], "c": {"d": "e"}, "z": None}
        result = report.build_result("proj", data)
        for key, value in data.items():
            self.assertEqual(result[key], value)

    def test_input_data_is_not_mutated(self):
        data = {"status": "corrupt", "k": [1]}
        report.build_result("proj", data)
        self.assertEqual(data, {"status": "corrupt", "k": [1]})

    def test_result_is_json_serializable_with_sorted_keys(self):
        data = {"latest_batch": "B1", "ancestors": ["B0"], "operation_count": 1,
                "layers": {"B0": [], "B1": ["B0"]}, "rejected_operations": []}
        text = json.dumps(report.build_result("proj", data),
                          sort_keys=True, ensure_ascii=False)
        self.assertIn('"status": "ok"', text)

    def test_pipeline_result_is_immutable(self):
        result = PipelineResult(project="p", data={})
        self.assertTrue(isinstance(result, PipelineResult))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            result.project = "other"


if __name__ == "__main__":
    unittest.main()
