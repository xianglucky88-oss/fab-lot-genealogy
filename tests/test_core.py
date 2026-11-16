import json
import unittest
from pathlib import Path
from fab_lot_genealogy.pipeline import run
from fab_lot_genealogy.parser import load_records
from fab_lot_genealogy.engine import calculate

def fixture():
    return json.loads((Path(__file__).parents[1] / "fixtures" / "input.json").read_text())

class TestCore(unittest.TestCase):
    def test_pipeline_and_modules(self):
        payload = fixture()
        rows = load_records(payload)
        self.assertTrue(rows)
        data = calculate(rows, payload)
        self.assertIsInstance(data, dict)
        self.assertTrue(data)
        result = run(payload)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["project"], 'fab-lot-genealogy')

        assert result["ancestors"] == ["B0", "B1", "B2"]
