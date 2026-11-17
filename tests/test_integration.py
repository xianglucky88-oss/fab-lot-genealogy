"""End-to-end integration: CLI, three layers on disk, cross-layer traceability."""
import json
import tempfile
import unittest
from pathlib import Path

from fab_lot_genealogy.cli import main as cli_main
from fab_lot_genealogy.pipeline import run as legacy_run

from .fixtures import healthy_chain_records, EQUIPMENT_ALIASES


class TestLegacyCompatibility(unittest.TestCase):
    def test_original_fixture_pipeline_unchanged(self):
        payload = json.loads(
            (Path(__file__).parents[1] / "fixtures" / "input.json").read_text()
        )
        result = legacy_run(payload)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["project"], "fab-lot-genealogy")
        self.assertEqual(result["ancestors"], ["B0", "B1", "B2"])

    def test_cli_default_mode_matches_legacy_contract(self):
        import io
        import contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli_main([])
        result = json.loads(buf.getvalue())
        self.assertEqual(result["ancestors"], ["B0", "B1", "B2"])


class TestEndToEndLayers(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.input_dir = self.root / "in"
        self.input_dir.mkdir()
        self.store_dir = str(self.root / "store")
        groups = {}
        for record in healthy_chain_records():
            source = "mes" if record.source == "rework" else record.source
            groups.setdefault(source, []).append(record.payload)
        self.groups = groups
        for source, payloads in groups.items():
            (self.input_dir / f"{source}.json").write_text(
                json.dumps({"records": payloads})
            )
        (self.root / "aliases.json").write_text(
            json.dumps({"equipment_aliases": EQUIPMENT_ALIASES})
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_three_layers_land_on_disk_and_query_resolves(self):
        import io
        import contextlib
        # 1. ingest every source through the CLI
        for source in self.groups:
            with contextlib.redirect_stdout(io.StringIO()):
                cli_main([
                    "ingest", "--store", self.store_dir,
                    "--source", source, "--batch", f"b-{source}",
                    str(self.input_dir / f"{source}.json"),
                ])
        raw_files = {p.name for p in (self.root / "store" / "raw").glob("*.jsonl")}
        self.assertTrue(raw_files)

        # 2. normalize emits canonical events and an empty quarantine
        import io
        import contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli_main(["normalize", "--store", self.store_dir])
        normalized = json.loads(buf.getvalue())
        self.assertTrue(normalized["canonical_events"])
        self.assertEqual(normalized["quarantine"], [])

        # 3. materialize a frozen snapshot
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli_main(["materialize", "--store", self.store_dir])
        snapshot = json.loads(buf.getvalue())
        self.assertTrue(snapshot["content_hash"])
        self.assertTrue(snapshot["conservation_ledger"])

        # 4. query the final batch's full genealogy
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli_main([
                "query", "--store", self.store_dir,
                "--target", "B4", "--direction", "ancestors",
            ])
        answer = json.loads(buf.getvalue())
        labels = {n["label"] for n in answer["nodes"] if n["node_type"] == "batch"}
        self.assertEqual(labels, {"B0", "B1", "B2", "B3", "B4"})
        # rework step and equipment are both represented
        edge_types = {e["edge_type"] for e in answer["edges"]}
        self.assertIn("rework", edge_types)
        self.assertTrue(answer["equipment_steps"])

        # 5. every evidence raw_id resolves to a real raw-layer record
        raw_index = json.loads(
            (self.root / "store" / "raw" / "seen_index.json").read_text()
        )
        for ref in answer["evidence"]:
            self.assertIn(ref["raw_id"], raw_index["raw_ids"])

    def test_frozen_subcommand_isolated_from_later_writes(self):
        import io
        import contextlib
        for source, payloads in self.groups.items():
            with contextlib.redirect_stdout(io.StringIO()):
                cli_main([
                    "ingest", "--store", self.store_dir,
                    "--source", source, "--batch", f"b-{source}",
                    str(self.input_dir / f"{source}.json"),
                ])
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli_main([
                "materialize", "--store", self.store_dir,
                "--as-of-ingest", "2026-01-04T00:00:00Z",
            ])
        frozen = json.loads(buf.getvalue())
        frozen_hash = frozen["content_hash"]

        # later write
        late = self.input_dir / "late.json"
        late.write_text(json.dumps({"records": [{
            "type": "equipment_process", "at": "2026-01-09T08:00Z",
            "batch": "B4", "equipment_id": "EQ-LATE", "operation": "LATE",
        }]}))
        with contextlib.redirect_stdout(io.StringIO()):
            cli_main([
                "ingest", "--store", self.store_dir,
                "--source", "equipment", "--batch", "b-late", str(late),
            ])

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli_main([
                "frozen", "--store", self.store_dir,
                "--hash", frozen_hash, "--target", "B4",
            ])
        frozen_answer = json.loads(buf.getvalue())
        labels = {n["label"] for n in frozen_answer["equipment_steps"]}
        self.assertNotIn("EQ-LATE", labels)

        # catalog lists the frozen artifact
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli_main(["catalog", "--store", self.store_dir])
        catalog = json.loads(buf.getvalue())
        self.assertTrue(any(c["content_hash"] == frozen_hash for c in catalog))


if __name__ == "__main__":
    unittest.main()
