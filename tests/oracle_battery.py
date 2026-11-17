"""Shared oracle battery: run the audited semantics against an implementation.

The same battery backs the graph property tests (against the real modules)
and the mutation harness (against mutated modules).  A check returns None on
success or a divergence report naming the from_batch / to_batch edges and
the operation sequence that trigger the difference.
"""
import copy
import json
import random
import traceback

import oracle_diff
import oracle_gen
import oracle_graph as oracle
import oracle_shrink

PROJECT = oracle.PROJECT
SEED = 20261117


class Impl:
    """The four layer entry points under test."""

    def __init__(self, parse_operations, calculate, build_result, run):
        self.parse_operations = parse_operations
        self.calculate = calculate
        self.build_result = build_result
        self.run = run


def real_impl():
    from fab_lot_genealogy import engine, parser, pipeline, report

    return Impl(parser.parse_operations, engine.calculate,
                report.build_result, pipeline.run)


def compose(parse_operations, calculate, build_result):
    """Recompose the orchestration layer from parts (for layer mutants)."""

    def run(payload):
        valid, rejected = parse_operations(payload)
        data = calculate(valid, payload)
        data["rejected_operations"] = rejected
        return build_result(PROJECT, data)

    return run


class Failure:
    def __init__(self, check, report):
        self.check = check
        self.report = report

    def __repr__(self):
        return "Failure(%s)\n%s" % (self.check, self.report)


# ---------------------------------------------------------------------------
# Handcrafted semantic cases
# ---------------------------------------------------------------------------

def _t(day, hour=10):
    return "2026-01-%02dT%02d:00:00Z" % (day, hour)


def handcrafted_cases():
    op = oracle_gen.make_op
    cases = {}
    # 同一工序重复到达: identical record delivered three times.
    cases["duplicate_repeated_arrival"] = {
        "operations": [op(_t(1), "B0", "B1") for _ in range(3)],
    }
    # 分流: one batch feeds two; 合流: two batches feed one.
    cases["split_fanout"] = {
        "operations": [op(_t(1), "B0", "B1"), op(_t(2), "B0", "B2")],
    }
    cases["merge_fanin"] = {
        "operations": [op(_t(1), "B1", "B3"), op(_t(2), "B2", "B3")],
    }
    cases["diamond_split_merge"] = {
        "operations": [
            op(_t(1), "B0", "B1"), op(_t(1), "B0", "B2"),
            op(_t(2), "B1", "B3"), op(_t(2), "B2", "B3"),
        ],
    }
    cases["chain"] = {
        "operations": [op(_t(i), "B%d" % i, "B%d" % (i + 1)) for i in range(4)],
    }
    # 未知来源: B9 only ever appears as a from_batch.
    cases["unknown_source"] = {
        "operations": [op(_t(1), "B9", "B1"), op(_t(2), "B1", "B2")],
    }
    # 环路: rework cycle B1 <-> B2 fed by B0.
    cases["cycle_two_batches"] = {
        "operations": [
            op(_t(1), "B0", "B1", kind="rework"),
            op(_t(2), "B1", "B2", kind="rework"),
            op(_t(3), "B2", "B1", kind="rework"),
        ],
    }
    cases["self_loop"] = {"operations": [op(_t(1), "B1", "B1", kind="rework")]}
    # 空工序.
    cases["empty_operations"] = {"operations": []}
    cases["missing_operations_key"] = {}
    # 跨 wafer: chain links carried by different wafers.
    cases["cross_wafer_chain_filtered"] = {
        "operations": [
            op(_t(1), "B0", "B1", wafer="W1"),
            op(_t(2), "B1", "B2", wafer="W2"),
        ],
        "wafer": "W1",
    }
    cases["cross_wafer_chain_unfiltered"] = {
        "operations": [
            op(_t(1), "B0", "B1", wafer="W1"),
            op(_t(2), "B1", "B2", wafer="W2"),
        ],
    }
    cases["cross_wafer_merge_filtered"] = {
        "operations": [
            op(_t(1), "B0", "B2", wafer="W1", kind="merge"),
            op(_t(1), "B1", "B2", wafer="W2", kind="merge"),
        ],
        "wafer": "W2",
    }
    cases["wafer_filter_no_match"] = {
        "operations": [op(_t(1), "B0", "B1", wafer="W1")],
        "wafer": "W9",
    }
    # 时间相同: every operation carries the same timestamp.
    cases["equal_timestamps"] = {
        "operations": [
            op(_t(1), "B0", "B3"), op(_t(1), "B1", "B3"),
            op(_t(1), "B2", "B3"), op(_t(1), "B0", "B1"),
        ],
    }
    # Same instant rendered with different offsets.
    cases["same_instant_offsets"] = {
        "operations": [
            op("2026-01-01T10:00:00Z", "B0", "B1"),
            op("2026-01-01T18:00:00+08:00", "B1", "B2"),
            op(_t(1, 8), "B2", "B3"),
        ],
    }
    # 非法 kind.
    cases["illegal_kind_rejected"] = {
        "operations": [
            op(_t(1), "B0", "B1"),
            op(_t(2), "B1", "B2", kind="teleport"),
        ],
    }
    # 部分记录失败: one fault of every category among valid rows.
    cases["partial_record_failures"] = {
        "operations": [
            op(_t(1), "B0", "B1"),
            None,
            op(_t(2), "B1", "B2"),
            {"at": _t(3), "from_batch": "B2", "wafer": "W1", "kind": "split"},
            op(_t(3), "B2", "B3"),
            {"at": "nope", "from_batch": "B3", "to_batch": "B4",
             "wafer": "W1", "kind": "merge"},
            {"at": _t(4), "from_batch": "", "to_batch": "B5",
             "wafer": "W1", "kind": "split"},
            {"at": _t(4), "from_batch": "B4", "to_batch": "B5",
             "wafer": "W1", "kind": "SPLIT"},
        ],
    }
    cases["non_dict_rows"] = {
        "operations": [None, 17, "row", op(_t(1), "B0", "B1")],
    }
    cases["wrong_field_types"] = {
        "operations": [
            op(_t(1), "B0", "B1"),
            {"at": 5, "from_batch": "B1", "to_batch": "B2",
             "wafer": "W1", "kind": "split"},
            {"at": _t(2), "from_batch": "B1", "to_batch": "B2",
             "wafer": None, "kind": "split"},
        ],
    }
    cases["duplicates_and_bad_rows"] = {
        "operations": [
            op(_t(1), "B0", "B1"), op(_t(1), "B0", "B1"),
            op(_t(2), "B1", "B2"), 42,
            op(_t(2), "B1", "B2"),
        ],
    }
    return cases


# ---------------------------------------------------------------------------
# Layer comparison
# ---------------------------------------------------------------------------

def compare_payload(impl, payload, context):
    """Compare every layer against the oracle; None when they agree."""
    expected_parse = oracle.oracle_parse(payload)
    actual_parse = impl.parse_operations(payload)
    if actual_parse != expected_parse:
        return oracle_diff.explain_parse_divergence(
            payload, expected_parse, actual_parse, context)
    valid, _ = expected_parse
    wafer = payload.get("wafer") if isinstance(payload, dict) else None
    expected_calc = oracle.oracle_calculate(valid, wafer=wafer)
    actual_calc = impl.calculate(valid, payload)
    if actual_calc != expected_calc:
        return oracle_diff.explain_divergence(
            payload, expected_calc, actual_calc, context)
    expected_run = oracle.oracle_run(payload)
    actual_run = impl.run(payload)
    if actual_run != expected_run:
        return oracle_diff.explain_divergence(
            payload, expected_run, actual_run, context)
    return None


def _still_diverges(impl):
    def predicate(payload):
        try:
            return compare_payload(impl, payload, "shrink") is not None
        except Exception:
            return True

    return predicate


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------

def _check_operations_must_be_list(impl):
    for bad in (None, "x", 1, {}, ()):
        for fn in (impl.parse_operations, impl.run):
            try:
                fn({"operations": bad})
            except ValueError:
                continue
            return "operations=%r did not raise ValueError" % (bad,)
    return None


def _check_wafer_filter_type(impl):
    try:
        impl.run({"operations": [], "wafer": 5})
    except ValueError:
        return None
    return "non-string wafer filter did not raise ValueError"


def _check_envelope_authority(impl):
    data = {"status": "corrupt", "project": "forgery", "k": 7}
    got = impl.build_result(PROJECT, data)
    expected = {"status": "ok", "project": PROJECT, "k": 7}
    if got != expected:
        return "envelope not authoritative: expected %r, got %r" % (expected, got)
    if data != {"status": "corrupt", "project": "forgery", "k": 7}:
        return "build_result mutated its input data"
    return None


def _check_envelope_preserves_data(impl):
    data = {"a": 1, "b": [1, 2], "c": {"d": "e"}}
    got = impl.build_result(PROJECT, data)
    for key, value in data.items():
        if got.get(key) != value:
            return "envelope dropped data key %r" % (key,)
    return None


def _check_determinism(impl):
    rng = random.Random(SEED + 1)
    for _ in range(5):
        payload = oracle_gen.gen_payload(
            rng, n_batches=5, n_ops=9, wafers=("W1", "W2"),
            self_loops=True, duplicate_rate=0.3, bad_rate=0.2)
        if impl.run(payload) != impl.run(payload):
            return "run() is not deterministic"
    return None


def _check_payload_immutability(impl):
    rng = random.Random(SEED + 2)
    payload = oracle_gen.gen_payload(
        rng, n_batches=5, n_ops=9, wafers=("W1", "W2"),
        self_loops=True, duplicate_rate=0.3, bad_rate=0.3)
    snapshot = copy.deepcopy(payload)
    impl.run(payload)
    if payload != snapshot:
        return "run() mutated the input payload"
    return None


def _check_json_serializable(impl):
    rng = random.Random(SEED + 3)
    payload = oracle_gen.gen_payload(
        rng, n_batches=5, n_ops=9, wafers=("W1", "W2"), bad_rate=0.3)
    try:
        json.dumps(impl.run(payload), sort_keys=True, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        return "run() result is not JSON serializable: %s" % exc
    return None


def _check_long_chain(impl):
    n = 1500
    rows = [oracle_gen.make_op(oracle_gen.stamp(i), "B%d" % i, "B%d" % (i + 1),
                               kind="rework") for i in range(n)]
    payload = {"operations": rows}
    problem = compare_payload(impl, payload, "long_chain")
    if problem:
        return problem
    return None


def _check_reorder_invariance(impl):
    rng = random.Random(SEED + 4)
    for case in range(6):
        rows = oracle_gen.gen_rows(
            rng, n_batches=5, n_ops=7, wafers=("W1", "W2"),
            dag=case % 2 == 0, self_loops=True, duplicate_rate=0.25,
            equal_ts_rate=0.5)
        payload = {"operations": rows}
        baseline = impl.calculate([dict(r) for r in rows], payload)
        for _ in range(12):
            permuted = list(rows)
            rng.shuffle(permuted)
            got = impl.calculate([dict(r) for r in permuted], payload)
            if got != baseline:
                return "input reorder changed output:\n" + \
                    oracle_diff.explain_divergence(
                        {"operations": permuted}, baseline, got, "reorder")
    return None


def _check_duplicate_invariance(impl):
    rng = random.Random(SEED + 5)
    for _ in range(6):
        rows = oracle_gen.gen_rows(
            rng, n_batches=5, n_ops=7, wafers=("W1", "W2"),
            self_loops=True, equal_ts_rate=0.5)
        payload = {"operations": rows}
        baseline = impl.calculate([dict(r) for r in rows], payload)
        injected = list(rows)
        for _ in range(4):
            injected.append(dict(rng.choice(rows)))
        rng.shuffle(injected)
        got = impl.calculate([dict(r) for r in injected], payload)
        if got != baseline:
            return "duplicate delivery changed output:\n" + \
                oracle_diff.explain_divergence(
                    {"operations": injected}, baseline, got, "duplicates")
    return None


# ---------------------------------------------------------------------------
# Battery driver
# ---------------------------------------------------------------------------

def _random_payload(rng):
    return oracle_gen.gen_payload(
        rng,
        n_batches=rng.randrange(2, 8),
        n_ops=rng.randrange(0, 14),
        wafers=("W1", "W2", "W3")[: rng.randrange(1, 4)],
        dag=rng.random() < 0.5,
        self_loops=rng.random() < 0.3,
        duplicate_rate=rng.choice([0.0, 0.2, 0.4]),
        equal_ts_rate=rng.choice([0.2, 0.5, 0.8]),
        bad_rate=rng.choice([0.0, 0.0, 0.15, 0.3]),
        wafer_filter=rng.choice([None, None, "W1", "W2", "W9"]),
    )


def run_battery(impl, *, seed=SEED, random_cases=120, include_slow=True):
    """Run every check; return a list of :class:`Failure` (empty == pass)."""
    failures = []

    def record(check, fn):
        try:
            problem = fn()
        except Exception:
            problem = "raised unexpectedly:\n" + traceback.format_exc()
        if problem:
            failures.append(Failure(check, problem))

    for name, payload in handcrafted_cases().items():
        record("semantics:%s" % name,
               lambda p=payload: compare_payload(impl, p, name))

    for name, fn in [
        ("operations_must_be_list", _check_operations_must_be_list),
        ("wafer_filter_type", _check_wafer_filter_type),
        ("envelope_authority", _check_envelope_authority),
        ("envelope_preserves_data", _check_envelope_preserves_data),
        ("determinism", _check_determinism),
        ("payload_immutability", _check_payload_immutability),
        ("json_serializable", _check_json_serializable),
        ("reorder_invariance", _check_reorder_invariance),
        ("duplicate_invariance", _check_duplicate_invariance),
    ]:
        record(name, lambda fn=fn: fn(impl))

    if include_slow:
        record("long_chain", lambda: _check_long_chain(impl))

    rng = random.Random(seed)
    for case_no in range(random_cases):
        payload = _random_payload(rng)
        try:
            problem = compare_payload(impl, payload, "random[%d]" % case_no)
        except Exception:
            problem = "raised unexpectedly:\n" + traceback.format_exc()
        if problem:
            shrunk = oracle_shrink.shrink_payload(payload, _still_diverges(impl))
            minimal = oracle_shrink.is_minimal(shrunk, _still_diverges(impl))
            try:
                report = compare_payload(impl, shrunk,
                                         "minimal counterexample")
            except Exception:
                report = "raised unexpectedly:\n" + traceback.format_exc()
            failures.append(Failure(
                "random[seed=%d case=%d]" % (seed, case_no),
                "%s\n  shrunk to %d operation(s); single-operation-removal "
                "minimal: %s" % (report, len(shrunk["operations"]), minimal)))
    return failures
