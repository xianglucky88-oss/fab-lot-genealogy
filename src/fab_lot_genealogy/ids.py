"""Deterministic identifiers and content hashes.

Order-independent hashing is the backbone of the replay guarantees:
the same logical content always hashes to the same id regardless of the
order records happened to arrive in.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def content_id(prefix: str, obj: Any) -> str:
    return f"{prefix}_{stable_hash(obj)[:16]}"


def natural_key(parts: tuple) -> str:
    return content_id("key", parts)
