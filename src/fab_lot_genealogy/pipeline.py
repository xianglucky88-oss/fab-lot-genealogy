"""Orchestration boundary: parse -> calculate -> envelope."""
from .parser import parse_operations
from .engine import calculate
from .report import build_result


def run(payload: dict) -> dict:
    records, rejected = parse_operations(payload)
    data = calculate(records, payload)
    data["rejected_operations"] = rejected
    return build_result("fab-lot-genealogy", data)
