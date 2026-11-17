# Fab Lot Genealogy

Replayable manufacturing **genealogy data chain** for semiconductor fabs.

It answers the quality-room question — *"at some point in history, which input
batches, equipment and rework steps produced this wafer?"* — from five MES-side
feeds (batch creation, wafer split/merge, equipment processing and inspection),
and keeps every conclusion traceable back to the exact raw record that produced
it.

The original flat-record summary (`run` / the default CLI mode) still works
unchanged for the legacy fixture.

## Three layers

```
sources (mes / split / merge / equipment / inspection / legacy)
   │  append-only, idempotent landing
   ▼
raw/        RawRecord per source JSONL + seen-index + batch manifests
   │  schema-versioned normalization, alias resolution, natural-key dedupe
   ▼
normalized  CanonicalEvent (single event vocabulary, correction windows)
   │  deterministic, order-independent graph replay
   ▼
snapshots/  frozen, content-addressed GraphSnapshot (nodes + conserved edges
            + conservation ledger + explainable quarantine)
   │  bounded traversal
   ▼
query       LineageResult (ancestors / descendants / full, wafer-scoped)
```

* **raw layer** (`raw_store.py`) — append-only JSONL.  A record's identity is a
  hash of its verbatim content, so replaying an identical file appends nothing
  and returns the same raw ids.  Writes are atomic (tmp + `os.replace`), batch
  manifests record `acked` / `failed` offsets, and a partially failed batch
  never blocks later batches.
* **normalization layer** (`normalizer.py`) — maps every source and schema
  version onto one `CanonicalEvent` vocabulary, resolves equipment ids through
  an alias registry while preserving the raw reference, derives a business
  natural key to detect repeated reports, and represents historical corrections
  as first-class events with an event-time validity window.
* **graph / query layer** (`graph.py`, `query.py`, `snapshot.py`) — builds a
  batch/equipment/inspection DAG with wafer-conserved edges, materializes
  immutable content-addressed snapshots, and serves bounded lineage traversal.

## Bitemporal as-of queries

Two independent clocks are tracked on every event:

| axis | meaning | snapshot knob |
| --- | --- | --- |
| **event time** | when it happened on the factory floor | `--as-of-event` |
| **ingest time** | when the data lake learned about it | `--as-of-ingest` |

A snapshot at `(T, I)` contains exactly the events ingested by `I` whose
business facts are valid at `T`.

* A **frozen snapshot can never read later writes** — late-arriving records are
  absent from a snapshot whose ingest ceiling predates them.
* A **historical revision only affects descendants inside its validity window**;
  outside the window the original fact is returned again.
* Corrections keep an evidence chain containing both the original and the
  correcting raw record.

## Conservation and quarantine

Every structural event must move whole wafers between *known* batches without
loss, duplication or overlap.  A point-in-time wafer-ownership ledger plus
per-batch wafer sets enforce this.  Anything that cannot be explained is
**quarantined with a reason and an evidence pointer**, never silently dropped:

| code | trigger |
| --- | --- |
| `missing_parent` | referenced parent batch was never created/observed |
| `cyclic_relation` | edge would close a cycle in the batch DAG |
| `conflicting_operation` | same operation step reports incompatible facts |
| `conservation_violation` | split/merge wafer sets do not conserve (unknown wafer, overlap, wrong owner, union mismatch) |
| `duplicate_conflict` | same natural key re-reported with a different payload |
| `unknown_equipment` | equipment reference cannot be resolved |
| `invalid_record` | structurally unparseable source record |

Each graph edge carries only the wafers that actually flowed through that
source→destination pair, so wafer-scoped queries never fan out into parallel
branches.

## Schema and identifier evolution

* Records carry an explicit `schema_version`; unknown future fields are retained
  in `extra` rather than rejected.
* Equipment id fields changed across versions (`equipment` v1 →
  `equipment_id`/`tool_ref` v2).  Both resolve for old batches, and an alias
  registry maps historical tool names to canonical ids, keeping the raw
  reference for evidence.

## Streaming vs. batch materialization

Both paths run the **same** normalization and graph rules; they differ only in
state management.

| | batch (`GenealogyPipeline`) | streaming (`StreamProcessor`) |
| --- | --- | --- |
| state | full raw log read per materialization | checkpoint of consumed raw ids per source |
| recovery | rebuild is inherently idempotent | resumes from checkpoint; failed offsets isolated |
| bound | builder budgets (`max_batches`, `max_edges`, `max_quarantine`) | same builder budgets + `max_canonical_events` |
| use case | scheduled full refresh, frozen audit snapshots | low-latency micro-batch ingestion |

Because every transform is deterministic and every id is content-derived, the
two paths emit **byte-identical snapshots for the same logical inputs**, and the
result is independent of arrival order (proven by shuffled/reversed/replay
tests and a 50-parent fan-in merge).

### Bounded work

* Replay state is capped by explicit budgets; breaching one raises
  `BudgetExceeded` instead of growing without limit.
* Each query takes a `max_nodes` expansion budget; an over-large lineage returns
  `truncated: true` with the partial answer rather than exhausting memory.

## Layout on disk

```
<store>/
  raw/
    <source>.jsonl          append-only verbatim records
    seen_index.json         raw_id -> location (O(1) idempotent dedupe)
    batches/<batch_id>.json acked/failed offsets and batch status
  snapshots/
    <content_hash>.json     immutable frozen snapshot (integrity-checked)
    catalog.json            snapshot index
  stream_checkpoint.json    consumed positions + materialized hashes
```

## CLI

```bash
# legacy flat-record behaviour (unchanged)
PYTHONPATH=src python3 -m fab_lot_genealogy.cli
PYTHONPATH=src python3 -m fab_lot_genealogy.cli legacy fixtures/input.json

# three-layer chain
PYTHONPATH=src python3 -m fab_lot_genealogy.cli ingest   --store ./ds --source mes        --batch b1 mes.json
PYTHONPATH=src python3 -m fab_lot_genealogy.cli normalize --store ./ds
PYTHONPATH=src python3 -m fab_lot_genealogy.cli materialize --store ./ds \
        --as-of-event 2026-01-05T00:00:00Z --as-of-ingest 2026-01-10T00:00:00Z
PYTHONPATH=src python3 -m fab_lot_genealogy.cli query    --store ./ds --target LOT-FINAL --direction ancestors
PYTHONPATH=src python3 -m fab_lot_genealogy.cli query    --store ./ds --target LOT-FINAL --wafer W01 --max-nodes 1000
PYTHONPATH=src python3 -m fab_lot_genealogy.cli frozen   --store ./ds --hash <content_hash> --target LOT-FINAL
PYTHONPATH=src python3 -m fab_lot_genealogy.cli stream   --store ./ds --source equipment --batch b2 eq.json
PYTHONPATH=src python3 -m fab_lot_genealogy.cli catalog  --store ./ds
PYTHONPATH=src python3 -m fab_lot_genealogy.cli quarantine --store ./ds
```

Input files are a JSON list, or an object with a top-level `"records"` list.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover
```

The suite (61 tests) covers, across the layers:

* idempotent landing, partial-failure recovery, durable dedupe index;
* five-source normalization, schema v1/v2 compatibility, alias resolution,
  duplicate merge vs. conflict, correction windows;
* conservation (unknown wafer, overlapping parents, union mismatch), cycle,
  missing-parent and same-step-conflict quarantine with explainable evidence;
* the four event-time × ingest-time quadrants, freeze isolation, correction
  scoping to descendants within its window;
* wafer-scoped traversal, node-budget truncation, frozen-snapshot integrity and
  tamper detection;
* stream/batch hash equivalence, out-of-order arrival stability, repeated replay
  idempotence, checkpoint crash recovery, and a 50-parent large fan-in merge;
* end-to-end CLI operation and unchanged legacy behaviour.
