"""Flow-orchestration layer vs the independent oracle (流程编排层)."""
import json
import os
import random
import subprocess
import sys
import unittest
from pathlib import Path

import oracle_gen
import oracle_graph as oracle
from fab_lot_genealogy import pipeline

ROOT = Path(__file__).parents[1]


class TestPipelineOracle(unittest.TestCase):
    def test_run_matches_oracle_over_generated_corpus(self):
        rng = random.Random(31)
        for _ in range(150):
            payload = oracle_gen.gen_payload(
                rng,
                n_batches=rng.randrange(2, 8),
                n_ops=rng.randrange(0, 14),
                wafers=("W1", "W2", "W3")[: rng.randrange(1, 4)],
                dag=rng.random() < 0.5,
                self_loops=rng.random() < 0.3,
                duplicate_rate=rng.choice([0.0, 0.2, 0.4]),
                equal_ts_rate=rng.choice([0.2, 0.5, 0.8]),
                bad_rate=rng.choice([0.0, 0.15, 0.3]),
                wafer_filter=rng.choice([None, None, "W1", "W2", "W9"]),
            )
            self.assertEqual(pipeline.run(payload), oracle.oracle_run(payload))

    def test_run_is_deterministic(self):
        rng = random.Random(32)
        payload = oracle_gen.gen_payload(rng, n_batches=5, n_ops=10,
                                         duplicate_rate=0.4, bad_rate=0.3)
        self.assertEqual(pipeline.run(payload), pipeline.run(payload))

    def test_partial_record_failure_is_surfaced_not_fatal(self):
        payload = {
            "operations": [
                oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1"),
                None,
                {"at": "bad", "from_batch": "B1", "to_batch": "B2",
                 "wafer": "W1", "kind": "split"},
                oracle_gen.make_op(oracle_gen.stamp(1), "B1", "B2"),
            ],
        }
        result = pipeline.run(payload)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["ancestors"], ["B0", "B1"])
        rejected = result["rejected_operations"]
        self.assertEqual([r["index"] for r in rejected], [1, 2])
        self.assertEqual(rejected[0]["reason"], "row is not an object")
        self.assertEqual(rejected[1]["reason"], "invalid timestamp: 'bad'")

    def test_rejected_operations_key_is_always_present(self):
        payload = {"operations": [oracle_gen.make_op(oracle_gen.stamp(0), "B0", "B1")]}
        self.assertEqual(pipeline.run(payload)["rejected_operations"], [])

    def test_non_list_operations_raise_value_error(self):
        for bad in (None, "x", 1, {}):
            with self.assertRaises(ValueError):
                pipeline.run({"operations": bad})

    def test_non_string_wafer_filter_raises(self):
        with self.assertRaises(ValueError):
            pipeline.run({"operations": [], "wafer": 5})

    def test_payload_is_not_mutated(self):
        rng = random.Random(33)
        payload = oracle_gen.gen_payload(rng, n_batches=5, n_ops=9,
                                         bad_rate=0.3, duplicate_rate=0.3)
        snapshot = json.loads(json.dumps(payload))
        pipeline.run(payload)
        self.assertEqual(payload, snapshot)

    def test_cli_matches_run_on_fixture(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
        out = subprocess.run(
            [sys.executable, "-m", "fab_lot_genealogy.cli"],
            capture_output=True, text=True, env=env, check=True,
        )
        cli = json.loads(out.stdout)
        payload = json.loads((ROOT / "fixtures" / "input.json").read_text())
        self.assertEqual(cli, pipeline.run(payload))
        self.assertEqual(cli["ancestors"], ["B0", "B1", "B2"])


if __name__ == "__main__":
    unittest.main()
