"""Deterministic JSON CLI for the genealogy data chain.

Subcommands::

    legacy <input.json>                 original flat-record behaviour
    ingest   --store DIR --source S --batch B [file ...]
    normalize --store DIR
    materialize --store DIR [--as-of-event T] [--as-of-ingest T]
    query    --store DIR --target W [--direction D] [--max-nodes N]
    frozen   --store DIR --hash H --target W
    stream   --store DIR --source S --batch B [file ...]   # then replay
    catalog  --store DIR
    quarantine --store DIR [--hash H]

With no subcommand the legacy fixture run is preserved.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .pipeline import GenealogyPipeline, run as legacy_run
from .stream import StreamProcessor
from .timeutil import far_future

DEFAULT_FIXTURE = Path(__file__).parents[2] / "fixtures" / "input.json"
SOURCES = ("mes", "split", "merge", "equipment", "inspection", "legacy")


def _load(path: str) -> list:
    text = Path(path).read_text()
    payload = json.loads(text)
    if isinstance(payload, dict) and "records" in payload:
        return payload["records"]
    if isinstance(payload, list):
        return payload
    raise SystemExit(f"{path}: expected a JSON list or an object with 'records'")


def _emit(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=str))


def _aliases(path: str | None) -> dict:
    if not path:
        return {}
    payload = json.loads(Path(path).read_text())
    return payload.get("equipment_aliases", payload)


SUBCOMMANDS = (
    "legacy", "ingest", "normalize", "materialize", "query",
    "frozen", "stream", "catalog", "quarantine",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replayable manufacturing genealogy data chain"
    )
    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("legacy", help="original flat-record summary")
    p.add_argument("input", nargs="?", default=str(DEFAULT_FIXTURE))

    p = sub.add_parser("ingest", help="land a batch in the raw layer")
    p.add_argument("--store", required=True)
    p.add_argument("--source", required=True, choices=SOURCES)
    p.add_argument("--batch", required=True)
    p.add_argument("--aliases")
    p.add_argument("files", nargs="+")

    p = sub.add_parser("normalize", help="raw -> canonical events")
    p.add_argument("--store", required=True)
    p.add_argument("--aliases")

    p = sub.add_parser("materialize", help="build a bitemporal frozen snapshot")
    p.add_argument("--store", required=True)
    p.add_argument("--aliases")
    p.add_argument("--as-of-event", default=None)
    p.add_argument("--as-of-ingest", default=None)

    p = sub.add_parser("query", help="lineage over the latest snapshot")
    p.add_argument("--store", required=True)
    p.add_argument("--aliases")
    p.add_argument("--target", required=True)
    p.add_argument("--direction", default="ancestors",
                   choices=("ancestors", "descendants", "full"))
    p.add_argument("--max-nodes", type=int, default=10_000)
    p.add_argument("--wafer")

    p = sub.add_parser("frozen", help="lineage against a frozen content hash")
    p.add_argument("--store", required=True)
    p.add_argument("--hash", required=True, dest="content_hash")
    p.add_argument("--target", required=True)
    p.add_argument("--direction", default="ancestors",
                   choices=("ancestors", "descendants", "full"))
    p.add_argument("--max-nodes", type=int, default=10_000)

    p = sub.add_parser("stream", help="streaming ingest + replay")
    p.add_argument("--store", required=True)
    p.add_argument("--source", required=True, choices=SOURCES)
    p.add_argument("--batch", required=True)
    p.add_argument("--aliases")
    p.add_argument("files", nargs="+")

    p = sub.add_parser("catalog", help="list frozen snapshots")
    p.add_argument("--store", required=True)

    p = sub.add_parser("quarantine", help="show quarantine entries")
    p.add_argument("--store", required=True)
    p.add_argument("--aliases")
    p.add_argument("--hash")
    return parser


def main(argv: list[str] | None = None) -> None:
    raw_argv = list(sys.argv[1:] if argv is None else argv)

    # Legacy default mode: no arguments, or a bare fixture path that is not a
    # subcommand name. This keeps `cli.py [fixture.json]` working as before.
    if not raw_argv or raw_argv[0] not in SUBCOMMANDS:
        path = raw_argv[0] if raw_argv else str(DEFAULT_FIXTURE)
        _emit(legacy_run(json.loads(Path(path).read_text())))
        return

    parser = build_parser()
    args = parser.parse_args(raw_argv)

    if args.command == "legacy":
        _emit(legacy_run(json.loads(Path(args.input).read_text())))
        return

    if args.command == "catalog":
        _emit(GenealogyPipeline(args.store).snapshots.catalog())
        return

    aliases = _aliases(getattr(args, "aliases", None))

    if args.command == "ingest":
        pipe = GenealogyPipeline(args.store, equipment_aliases=aliases)
        manifests = []
        for file in args.files:
            manifests.append(
                pipe.ingest(args.source, _load(file), f"{args.batch}:{Path(file).name}")
            )
        _emit({"manifests": manifests})
        return

    if args.command == "normalize":
        pipe = GenealogyPipeline(args.store, equipment_aliases=aliases)
        events, quarantine = pipe.normalize()
        _emit({
            "canonical_events": [e.to_dict() for e in events],
            "quarantine": [q.to_dict() for q in quarantine],
        })
        return

    if args.command == "materialize":
        pipe = GenealogyPipeline(args.store, equipment_aliases=aliases)
        snapshot = pipe.materialize(
            as_of_event=args.as_of_event or far_future(),
            as_of_ingest=args.as_of_ingest or far_future(),
        )
        _emit(snapshot.to_dict())
        return

    if args.command == "query":
        pipe = GenealogyPipeline(args.store, equipment_aliases=aliases)
        result = pipe.query(
            args.target,
            direction=args.direction,
            max_nodes=args.max_nodes,
            wafer=args.wafer,
        )
        _emit(result.to_dict())
        return

    if args.command == "frozen":
        pipe = GenealogyPipeline(args.store)
        _emit(pipe.frozen_query(
            args.content_hash, args.target, args.direction, args.max_nodes
        ))
        return

    if args.command == "stream":
        proc = StreamProcessor(args.store, equipment_aliases=aliases)
        manifests = []
        for file in args.files:
            manifests.append(
                proc.ingest(args.source, _load(file), f"{args.batch}:{Path(file).name}")
            )
        snapshot = proc.replay()
        _emit({"manifests": manifests, "snapshot": snapshot.to_dict()})
        return

    if args.command == "quarantine":
        pipe = GenealogyPipeline(args.store)
        if args.hash:
            body = pipe.snapshots.load(args.hash)
            _emit({"quarantine": body["quarantine"]})
        else:
            _, quarantine = pipe.normalize()
            snapshot = pipe.materialize()
            _emit({"quarantine": [q.to_dict() for q in snapshot.quarantine]})
        return


if __name__ == "__main__":
    main()
