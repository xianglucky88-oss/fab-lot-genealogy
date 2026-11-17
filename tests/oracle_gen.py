"""Seeded generators for graph/property testing.  Deterministic per seed.

Only the standard library is used so the oracle stays self-contained.
"""
import random
from datetime import datetime, timedelta, timezone

KINDS = ("split", "merge", "rework")
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_op(at, from_batch, to_batch, wafer="W1", kind="split"):
    return {
        "at": at,
        "from_batch": from_batch,
        "to_batch": to_batch,
        "wafer": wafer,
        "kind": kind,
    }


def stamp(minutes):
    """Canonical UTC rendering of BASE + `minutes`."""
    return (BASE + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp_variant(rng, minutes):
    """Render one instant in different but equivalent ISO-8601 forms."""
    base = BASE + timedelta(minutes=minutes)
    form = rng.random()
    if form < 0.7:
        return base.strftime("%Y-%m-%dT%H:%M:%SZ")
    if form < 0.85:
        shifted = base.astimezone(timezone(timedelta(hours=8)))
        return shifted.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    return base.strftime("%Y-%m-%dT%H:%M:%S")  # naive, read as UTC


def gen_rows(rng, *, n_batches=6, n_ops=10, wafers=("W1", "W2"), dag=False,
             self_loops=False, duplicate_rate=0.0, equal_ts_rate=0.3):
    """Generate valid operation rows.

    ``dag=True`` guarantees an acyclic graph; otherwise cycles may occur.
    ``duplicate_rate`` re-delivers identical records (同一工序重复到达).
    ``equal_ts_rate`` reuses timestamps (时间相同).
    """
    batches = [f"B{i}" for i in range(n_batches)]
    order = list(batches)
    rng.shuffle(order)
    rank = {b: i for i, b in enumerate(order)}
    rows, stamps = [], []
    for _ in range(n_ops):
        if rows and duplicate_rate and rng.random() < duplicate_rate:
            rows.append(dict(rng.choice(rows)))
            continue
        if stamps and rng.random() < equal_ts_rate:
            at = rng.choice(stamps)
        else:
            at = stamp_variant(rng, rng.randrange(0, 60 * 24 * 30))
            stamps.append(at)
        if dag:
            a, b = rng.sample(batches, 2)
            if rank[a] > rank[b]:
                a, b = b, a
        else:
            a = rng.choice(batches)
            others = [x for x in batches if x != a]
            if others and not (self_loops and rng.random() >= 0.9):
                b = rng.choice(others)
            else:
                b = a
        rows.append(make_op(at, a, b, rng.choice(wafers), rng.choice(KINDS)))
    rng.shuffle(rows)
    return rows


def corrupt_row(rng, row=None):
    """Return a row corrupted with exactly one fault (部分记录失败)."""
    mode = rng.choice([
        "not_dict", "missing", "wrong_type", "empty_from", "empty_to",
        "empty_wafer", "bad_kind", "bad_ts",
    ])
    if mode == "not_dict":
        return rng.choice([None, 42, "oops", ["at", "from_batch"]])
    base = dict(row) if isinstance(row, dict) else make_op(
        stamp(0), "BX", "BY")
    if mode == "missing":
        del base[rng.choice(["at", "from_batch", "to_batch", "wafer", "kind"])]
    elif mode == "wrong_type":
        field = rng.choice(["at", "from_batch", "to_batch", "wafer", "kind"])
        base[field] = rng.choice([1, 1.5, None, [], {}])
    elif mode == "empty_from":
        base["from_batch"] = ""
    elif mode == "empty_to":
        base["to_batch"] = ""
    elif mode == "empty_wafer":
        base["wafer"] = ""
    elif mode == "bad_kind":
        base["kind"] = rng.choice(["teleport", "SPLIT", "", "merge2"])
    elif mode == "bad_ts":
        base["at"] = rng.choice([
            "not-a-time", "2026-13-01T00:00:00Z", "2026-01-01", "10:00:00", "",
        ])
    return base


def gen_payload(rng, *, bad_rate=0.0, wafer_filter=None, **row_kwargs):
    """Generate a full payload, optionally with corrupted rows and a wafer
    filter (跨 wafer 查询)."""
    rows = gen_rows(rng, **row_kwargs)
    if bad_rate:
        for i in range(len(rows)):
            if rng.random() < bad_rate:
                rows[i] = corrupt_row(rng, rows[i])
    payload = {"operations": rows}
    if wafer_filter is not None:
        payload["wafer"] = wafer_filter
    return payload
