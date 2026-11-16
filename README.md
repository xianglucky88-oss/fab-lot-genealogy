# Fab Lot Genealogy

Track semiconductor wafer ancestry through split, merge, and rework operations.

## Modules

- `src/fab_lot_genealogy/models.py`: immutable result contract.
- `src/fab_lot_genealogy/parser.py`: input validation and record extraction.
- `src/fab_lot_genealogy/engine.py`: domain calculation only.
- `src/fab_lot_genealogy/report.py`: stable output envelope.
- `src/fab_lot_genealogy/pipeline.py`: orchestration boundary.
- `src/fab_lot_genealogy/cli.py`: deterministic JSON CLI.

## Run

```bash
PYTHONPATH=src python -m fab_lot_genealogy.cli
PYTHONPATH=src python -m unittest discover -s tests -v
```

The fixture is local and deterministic; no network or credentials are required.
