"""Divergence reporting between the implementation and the oracle.

Every report names the from_batch / to_batch edges implicated in the
difference and prints the operation sequence that triggers it, so an audit
can replay exactly what happened.
"""


def format_operations(rows):
    lines = []
    for i, row in enumerate(rows):
        if isinstance(row, dict):
            lines.append(
                "  [%d] at=%s kind=%s from_batch=%s -> to_batch=%s wafer=%s"
                % (
                    i,
                    row.get("at"),
                    row.get("kind"),
                    row.get("from_batch"),
                    row.get("to_batch"),
                    row.get("wafer"),
                )
            )
        else:
            lines.append("  [%d] <invalid row> %r" % (i, row))
    return "\n".join(lines) if lines else "  <no operations>"


def _edges_through(rows, batch):
    hits = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        if row.get("from_batch") == batch or row.get("to_batch") == batch:
            hits.append(
                "  edge[%d]: from_batch=%s to_batch=%s at=%s kind=%s wafer=%s"
                % (
                    i,
                    row.get("from_batch"),
                    row.get("to_batch"),
                    row.get("at"),
                    row.get("kind"),
                    row.get("wafer"),
                )
            )
    return hits


def _short(value, limit=6):
    if isinstance(value, list) and len(value) > limit:
        return value[:limit] + ["... (%d more)" % (len(value) - limit)]
    return value


def explain_divergence(payload, expected, actual, context=""):
    """Report how `actual` diverges from the oracle `expected`.

    Points out the implicated from_batch / to_batch edges and the full
    operation sequence of the triggering payload.
    """
    lines = ["DIVERGENCE %s" % context]
    rows = payload.get("operations", []) if isinstance(payload, dict) else []
    differing_batches = set()
    for key in sorted(set(expected) | set(actual)):
        exp = expected.get(key, "<missing>")
        act = actual.get(key, "<missing>")
        if key == "layers" and isinstance(exp, dict) and isinstance(act, dict):
            for batch in sorted(set(exp) | set(act)):
                eb, ab = exp.get(batch, "<missing>"), act.get(batch, "<missing>")
                if eb != ab:
                    differing_batches.add(batch)
                    lines.append(
                        "  layers[%r]: expected ancestors %r, actual %r"
                        % (batch, eb, ab)
                    )
        elif exp != act:
            lines.append("  %s: expected %r, actual %r"
                         % (key, _short(exp), _short(act)))
            if key == "ancestors" and isinstance(exp, list) and isinstance(act, list):
                differing_batches.update(
                    b for b in set(exp) ^ set(act) if isinstance(b, str)
                )
            if key == "latest_batch":
                differing_batches.update(
                    b for b in (exp, act) if isinstance(b, str)
                )
    if differing_batches:
        lines.append("  implicated edges (from_batch/to_batch):")
        for batch in sorted(differing_batches):
            lines.extend(_edges_through(rows, batch))
    lines.append("  operation sequence (as supplied):")
    lines.append(format_operations(rows))
    return "\n".join(lines)


def explain_parse_divergence(payload, expected, actual, context=""):
    """Report a parsing-layer divergence (valid records / rejections)."""
    exp_valid, exp_rejected = expected
    act_valid, act_rejected = actual
    lines = ["DIVERGENCE %s" % context]
    if exp_valid != act_valid:
        lines.append("  valid records: expected %d, actual %d"
                     % (len(exp_valid), len(act_valid)))
        for i, (e, a) in enumerate(zip(exp_valid, act_valid)):
            if e != a:
                lines.append("    first diff at valid[%d]: expected %r, actual %r"
                             % (i, e, a))
                break
    if exp_rejected != act_rejected:
        lines.append("  rejections: expected %r, actual %r"
                     % (_short(exp_rejected), _short(act_rejected)))
    rows = payload.get("operations", []) if isinstance(payload, dict) else []
    lines.append("  operation sequence (as supplied):")
    lines.append(format_operations(rows))
    return "\n".join(lines)
