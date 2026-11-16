def calculate(rows, payload):
    parents = {}
    for row in sorted(rows, key=lambda r: r["at"]):
        parents.setdefault(row["to_batch"], set()).add(row["from_batch"])
    def ancestors(batch, seen=None):
        seen = set() if seen is None else seen
        for parent in parents.get(batch, set()):
            if parent not in seen:
                seen.add(parent); ancestors(parent, seen)
        return sorted(seen)
    latest = rows[-1]["to_batch"] if rows else None
    return {"latest_batch": latest, "ancestors": ancestors(latest) if latest else [], "operation_count": len(rows), "layers": {b: ancestors(b) for b in sorted(parents)}}
