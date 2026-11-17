# Fab Lot Genealogy

Track semiconductor wafer ancestry through split, merge, and rework operations.

## Modules

- `src/fab_lot_genealogy/models.py`: immutable result contract.
- `src/fab_lot_genealogy/instants.py`: ISO-8601 instant parsing (naive read as UTC).
- `src/fab_lot_genealogy/parser.py`: input validation and record extraction; invalid
  records are rejected with index and reason, never silently dropped.
- `src/fab_lot_genealogy/engine.py`: domain calculation only (directed ancestry graph).
- `src/fab_lot_genealogy/report.py`: stable output envelope; `project`/`status` are
  authoritative and cannot be overridden by data keys.
- `src/fab_lot_genealogy/pipeline.py`: orchestration boundary.
- `src/fab_lot_genealogy/cli.py`: deterministic JSON CLI.

## Audited semantics

- Identical records (same `at`/`from_batch`/`to_batch`/`wafer`/`kind`) are one
  operation delivered more than once: counted once, never a duplicate edge.
- Edges point `from_batch -> to_batch`; `ancestors(X)` excludes `X` itself even on
  rework cycles, so a successor is never misreported as an ancestor.
- `latest_batch` is the `to_batch` of the most recent operation ordered by
  `(instant, at, from_batch, to_batch, wafer, kind)`; equal timestamps and input
  reordering cannot change the answer.
- A `"wafer"` key in the payload scopes the whole query to that wafer.
- `layers` lists every known batch, including sources with empty ancestry.
- `rejected_operations` in the result lists every rejected record with its input
  index and reason (partial record failure stays explainable).

## Audit oracle and graph testing

`tests/` carries an independent directed-graph reference oracle that re-implements
the audited semantics for all four layers, plus generators, a counterexample
shrinker, and a divergence reporter that names the triggering
`from_batch` / `to_batch` edges and the operation sequence:

- `tests/oracle_graph.py`: per-layer reference oracle (parse / calculate / run /
  envelope) and the graph kernel.
- `tests/oracle_gen.py`: seeded generators (duplicates, reorder, split/merge,
  cycles, empty operations, cross-wafer, illegal kind, corrupted records).
- `tests/oracle_shrink.py`: greedy minimal counterexample reduction.
- `tests/oracle_diff.py`: divergence reports with implicated edges and operation
  sequence.
- `tests/oracle_battery.py`: shared check battery used by the property tests and
  the mutation harness.
- `tests/test_parser_oracle.py`, `tests/test_engine_oracle.py`,
  `tests/test_pipeline_oracle.py`, `tests/test_report_oracle.py`: per-layer oracle
  equivalence.
- `tests/test_graph_properties.py`: ancestor stability, no-successor-as-ancestor,
  ancestor/descendant duality, dedup counting, wafer scoping.
- `tests/test_fault_injection.py`: corrupted records, hostile payloads, deep
  chains, cycle/duplicate storms, envelope forgery.
- `tests/test_shrink_report.py`: minimal counterexample and report content.
- `tests/test_mutation.py`: 16 source mutants across all four layers; every
  mutant must be killed by the battery.

## Run

```bash
PYTHONPATH=src python -m fab_lot_genealogy.cli
PYTHONPATH=src python -m unittest discover -s tests -v
```

The fixture is local and deterministic; no network or credentials are required.
