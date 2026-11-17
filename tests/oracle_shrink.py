"""Greedy counterexample reduction (最小反例缩减).

Given a failing payload, remove operations while the failure predicate keeps
holding, until no single operation can be removed without losing the
failure.  Only operations are removed (never mutated), so the result is
always a subsequence of the original operation list.
"""


def shrink_payload(payload, fails):
    """Return a minimal sub-payload that still satisfies ``fails``.

    ``fails(payload) -> bool`` must be True exactly while the payload still
    exhibits the divergence, and must be True for the initial payload.
    """
    ops = list(payload.get("operations", []))
    rest = {k: v for k, v in payload.items() if k != "operations"}

    def attempt(seq):
        return fails({**rest, "operations": list(seq)})

    if not attempt(ops):
        raise ValueError("shrink_payload: initial payload does not fail")

    # Bulk pass: remove contiguous chunks, halving the window each round.
    size = max(1, len(ops) // 2)
    while size >= 1 and ops:
        i = 0
        while i < len(ops):
            candidate = ops[:i] + ops[i + size:]
            if len(candidate) < len(ops) and attempt(candidate):
                ops = candidate
            else:
                i += size
        size //= 2

    # Fixpoint: no single operation is removable.
    changed = True
    while changed:
        changed = False
        for i in range(len(ops)):
            if attempt(ops[:i] + ops[i + 1:]):
                ops = ops[:i] + ops[i + 1:]
                changed = True
                break
    return {**rest, "operations": ops}


def is_minimal(payload, fails):
    """True when removing any single operation stops the failure."""
    ops = payload.get("operations", [])
    for i in range(len(ops)):
        if fails({**payload, "operations": ops[:i] + ops[i + 1:]}):
            return False
    return True
