"""UTC timestamp parsing and rendering.

All internal time keeping is timezone-aware UTC.  Naive timestamps are
assumed to be UTC so that legacy fixtures (plain ``at`` strings) keep
working without schema changes.
"""
from __future__ import annotations

from datetime import datetime, timezone

UTC = timezone.utc


def parse_ts(value) -> datetime:
    """Parse ISO-8601 into a timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid timestamp {value!r}: {exc}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso(dt: datetime) -> str:
    """Render a UTC datetime as a canonical ISO-8601 string ending in Z."""
    dt = dt.astimezone(UTC)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def far_future() -> datetime:
    return datetime(9999, 12, 31, tzinfo=UTC)
