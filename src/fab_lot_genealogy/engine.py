"""Domain calculation: batch ancestry as a directed graph.

Audited semantics (verified against tests/oracle_graph.py):

* identical records (same at/from_batch/to_batch/wafer/kind) are one
  operation delivered more than once: counted once, never a duplicate edge;
* edges point from_batch -> to_batch; ancestors(X) are the batches that can
  reach X, excluding X itself even on cycles, so a successor is never
  misreported as an ancestor;
* latest_batch is the to_batch of the most recent operation ordered by
  (instant, at, from_batch, to_batch, wafer, kind), so equal timestamps and
  input reordering cannot change the answer;
* a "wafer" key in the payload scopes the whole query to that wafer;
* layers lists every known batch, including sources with empty ancestry.
"""
from .instants import parse_instant

_FIELDS = ("at", "from_batch", "to_batch", "wafer", "kind")


def _validate_rows(rows):
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"record {index}: row is not an object")
        for field in _FIELDS:
            value = row.get(field)
            if not isinstance(value, str):
                raise ValueError(
                    f"record {index}: field {field} must be a string")
        for field in ("from_batch", "to_batch", "wafer"):
            if not row[field]:
                raise ValueError(f"record {index}: {field} must be non-empty")
        try:
            parse_instant(row["at"])
        except ValueError as exc:
            raise ValueError(f"record {index}: {exc}") from exc


def _dedup(rows):
    seen, unique = set(), []
    for row in rows:
        key = (row["at"], row["from_batch"], row["to_batch"],
               row["wafer"], row["kind"])
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


def calculate(rows, payload):
    _validate_rows(rows)
    wafer = payload.get("wafer") if isinstance(payload, dict) else None
    if wafer is not None and not isinstance(wafer, str):
        raise ValueError("wafer filter must be a string")
    scoped = [row for row in rows if wafer is None or row["wafer"] == wafer]
    unique = _dedup(scoped)

    parents = {}
    batches = set()
    for row in unique:
        parents.setdefault(row["to_batch"], set()).add(row["from_batch"])
        batches.add(row["from_batch"])
        batches.add(row["to_batch"])

    def ancestors(batch):
        seen = {batch}
        stack = [batch]
        while stack:
            node = stack.pop()
            for parent in parents.get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        seen.discard(batch)
        return sorted(seen)

    def order(row):
        return (
            parse_instant(row["at"]),
            row["at"],
            row["from_batch"],
            row["to_batch"],
            row["wafer"],
            row["kind"],
        )

    latest = max(unique, key=order)["to_batch"] if unique else None
    return {
        "latest_batch": latest,
        "ancestors": ancestors(latest) if latest is not None else [],
        "operation_count": len(unique),
        "layers": {batch: ancestors(batch) for batch in sorted(batches)},
    }
