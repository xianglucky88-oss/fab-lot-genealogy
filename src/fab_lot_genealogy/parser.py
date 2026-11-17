"""Input validation and record extraction.

Partial record failure is explicit: every invalid row is rejected with its
index and a reason while valid rows are still processed, so a batch never
aborts because of a few bad records and nothing is dropped silently.
"""
from .instants import parse_instant

LEGAL_KINDS = frozenset({"split", "merge", "rework"})
FIELDS = ("at", "from_batch", "to_batch", "wafer", "kind")


def validate_row(row):
    """Return None when `row` is a well-formed operation, else the reason."""
    if not isinstance(row, dict):
        return "row is not an object"
    for field in FIELDS:
        if field not in row:
            return f"missing field: {field}"
    for field in FIELDS:
        if not isinstance(row[field], str):
            return f"field {field} must be a string"
    if not row["from_batch"]:
        return "from_batch must be non-empty"
    if not row["to_batch"]:
        return "to_batch must be non-empty"
    if not row["wafer"]:
        return "wafer must be non-empty"
    if row["kind"] not in LEGAL_KINDS:
        return f"illegal kind: {row['kind']!r}"
    try:
        parse_instant(row["at"])
    except ValueError:
        return f"invalid timestamp: {row['at']!r}"
    return None


def parse_operations(payload):
    """Split payload operations into (valid records, rejections).

    Rejections are ``{"index", "reason", "row"}`` dicts; order and
    duplicates of valid records are preserved (dedup is the engine's job).
    """
    rows = payload.get("operations", [])
    if not isinstance(rows, list):
        raise ValueError("operations must be a list")
    valid, rejected = [], []
    for index, row in enumerate(rows):
        reason = validate_row(row)
        if reason is None:
            valid.append(dict(row))
        else:
            rejected.append({"index": index, "reason": reason, "row": row})
    return valid, rejected


def load_records(payload):
    """Return only the well-formed operation records (backwards compatible)."""
    valid, _ = parse_operations(payload)
    return valid
