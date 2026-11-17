"""Independent directed-graph reference oracle for the genealogy pipeline.

This module is written independently from the production code in
``src/fab_lot_genealogy`` and defines the audited semantics for each of the
four layers:

* input parsing     -> :func:`oracle_parse`
* domain computation -> :class:`OracleGraph` / :func:`oracle_calculate`
* flow orchestration -> :func:`oracle_run`
* result encapsulation -> :func:`oracle_envelope`

Audited semantics
-----------------
* An operation is a record ``{at, from_batch, to_batch, wafer, kind}`` where
  ``kind`` is one of :data:`LEGAL_KINDS`, every field is a string, the batch
  and wafer fields are non-empty, and ``at`` is an ISO-8601 instant (naive
  values are read as UTC).
* Identical records (same at/from_batch/to_batch/wafer/kind) are the same
  operation delivered more than once: they are counted once and never
  duplicate an edge (重复操作不能重复计数).
* Edges point ``from_batch -> to_batch`` (material flow).  ``ancestors(X)``
  is the set of batches that can reach X, excluding X itself -- even on a
  cycle a batch is never its own ancestor, and a successor of X is never
  reported as an ancestor of X unless a genuine cycle path makes it one.
* Ancestor sets are stable: identical under input reordering, duplicate
  injection and repeated evaluation, and always emitted sorted.
* ``latest_batch`` is the ``to_batch`` of the most recent operation ordered
  by ``(instant, at, from_batch, to_batch, wafer, kind)`` so equal
  timestamps and input reordering cannot change the answer.
* A ``"wafer"`` key in the payload scopes the whole query to that wafer
  (cross-wafer edges are excluded); sources unknown to the graph have an
  empty ancestor set and still appear in ``layers``.
* Invalid records are rejected with their index and a reason while valid
  records are still processed (partial record failure).
* The envelope keeps every data key; ``project`` and ``status`` are
  authoritative and cannot be overridden by data keys.
"""
import re
from datetime import datetime, timezone

PROJECT = "fab-lot-genealogy"
LEGAL_KINDS = frozenset({"split", "merge", "rework"})
FIELDS = ("at", "from_batch", "to_batch", "wafer", "kind")

_STAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:?\d{2})?$"
)
_OFFSET_NO_COLON = re.compile(r"([+-]\d{2})(\d{2})$")


def oracle_instant(text):
    """Parse an ISO-8601 instant to an aware datetime (naive means UTC)."""
    if not isinstance(text, str):
        raise ValueError(f"invalid instant: {text!r}")
    match = _STAMP.match(text.strip())
    if not match:
        raise ValueError(f"invalid instant: {text!r}")
    offset = match.group(2)
    if offset and offset != "Z":
        digits = offset[1:].replace(":", "")
        if int(digits[:2]) > 23 or int(digits[2:]) > 59:
            raise ValueError(f"invalid instant: {text!r}")
    body = text.strip()
    if body.endswith("Z"):
        body = body[:-1] + "+00:00"
    else:
        body = _OFFSET_NO_COLON.sub(r"\1:\2", body)
    try:
        stamp = datetime.fromisoformat(body)
    except ValueError as exc:
        raise ValueError(f"invalid instant: {text!r}") from exc
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp


# ---------------------------------------------------------------------------
# Input parsing layer
# ---------------------------------------------------------------------------

def oracle_validate_row(row):
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
        oracle_instant(row["at"])
    except ValueError:
        return f"invalid timestamp: {row['at']!r}"
    return None


def oracle_parse(payload):
    """Reference for the parsing layer: (valid records, rejections).

    Order and duplicates of the input are preserved; rejection entries are
    ``{"index", "reason", "row"}`` so partial record failure stays
    explainable instead of silently dropping rows.
    """
    rows = payload.get("operations", [])
    if not isinstance(rows, list):
        raise ValueError("operations must be a list")
    valid, rejected = [], []
    for index, row in enumerate(rows):
        reason = oracle_validate_row(row)
        if reason is None:
            valid.append(dict(row))
        else:
            rejected.append({"index": index, "reason": reason, "row": row})
    return valid, rejected


# ---------------------------------------------------------------------------
# Domain computation layer: the directed-graph kernel
# ---------------------------------------------------------------------------

def op_key(row):
    """Identity of an operation: the full record content."""
    return (row["at"], row["from_batch"], row["to_batch"], row["wafer"], row["kind"])


def oracle_dedup(rows):
    """Drop redelivered copies of the same operation, keeping first arrival."""
    seen, unique = set(), []
    for row in rows:
        key = op_key(row)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


class OracleGraph:
    """Directed batch-lineage graph built from unique operations."""

    def __init__(self, rows):
        self.rows = oracle_dedup(rows)
        self.children = {}
        self.parents = {}
        self.batches = set()
        for row in self.rows:
            self.children.setdefault(row["from_batch"], set()).add(row["to_batch"])
            self.parents.setdefault(row["to_batch"], set()).add(row["from_batch"])
            self.batches.add(row["from_batch"])
            self.batches.add(row["to_batch"])

    @staticmethod
    def _walk(seed, adjacency):
        seen = {seed}
        stack = [seed]
        while stack:
            node = stack.pop()
            for nxt in adjacency.get(node, ()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return seen

    def ancestors_of(self, batch):
        """Batches that can reach `batch`; never includes `batch` itself."""
        if batch not in self.batches:
            return set()
        reached = self._walk(batch, self.parents)
        reached.discard(batch)
        return reached

    def descendants_of(self, batch):
        """Batches reachable from `batch`; never includes `batch` itself."""
        if batch not in self.batches:
            return set()
        reached = self._walk(batch, self.children)
        reached.discard(batch)
        return reached


def _order_key(row):
    """Total, input-order-independent ordering for the latest operation."""
    return (
        oracle_instant(row["at"]),
        row["at"],
        row["from_batch"],
        row["to_batch"],
        row["wafer"],
        row["kind"],
    )


def oracle_calculate(rows, wafer=None):
    """Reference for the domain layer over already-validated records."""
    if wafer is not None and not isinstance(wafer, str):
        raise ValueError("wafer filter must be a string")
    scoped = [row for row in rows if wafer is None or row["wafer"] == wafer]
    graph = OracleGraph(scoped)
    latest = max(graph.rows, key=_order_key)["to_batch"] if graph.rows else None
    return {
        "latest_batch": latest,
        "ancestors": sorted(graph.ancestors_of(latest)) if latest is not None else [],
        "operation_count": len(graph.rows),
        "layers": {b: sorted(graph.ancestors_of(b)) for b in sorted(graph.batches)},
    }


# ---------------------------------------------------------------------------
# Result encapsulation layer
# ---------------------------------------------------------------------------

def oracle_envelope(project, data):
    """Reference for the encapsulation layer.

    Every data key is preserved; ``project`` and ``status`` are
    authoritative envelope fields and cannot be overridden by data.
    """
    envelope = dict(data)
    envelope["project"] = project
    envelope["status"] = "ok"
    return envelope


# ---------------------------------------------------------------------------
# Flow orchestration layer
# ---------------------------------------------------------------------------

def oracle_run(payload):
    """Reference for the orchestration layer: parse -> calculate -> envelope."""
    valid, rejected = oracle_parse(payload)
    wafer = payload.get("wafer") if isinstance(payload, dict) else None
    data = oracle_calculate(valid, wafer=wafer)
    data["rejected_operations"] = rejected
    return oracle_envelope(PROJECT, data)
