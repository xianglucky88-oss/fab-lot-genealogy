"""ISO-8601 instant parsing shared by the parser and the engine."""
import re
from datetime import datetime, timedelta, timezone

_INSTANT_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})"
    r"(?:\.(\d{1,6}))?(Z|[+-]\d{2}:?\d{2})?$"
)


def parse_instant(text):
    """Parse an ISO-8601 instant to an aware datetime (naive means UTC)."""
    if not isinstance(text, str):
        raise ValueError(f"invalid instant: {text!r}")
    match = _INSTANT_RE.match(text.strip())
    if not match:
        raise ValueError(f"invalid instant: {text!r}")
    year, month, day, hour, minute, second = (
        int(group) for group in match.groups()[:6]
    )
    micros = int((match.group(7) or "0").ljust(6, "0"))
    offset = match.group(8)
    if offset in (None, "Z"):
        tzinfo = timezone.utc
    else:
        digits = offset[1:].replace(":", "")
        hours, minutes = int(digits[:2]), int(digits[2:])
        if hours > 23 or minutes > 59:
            raise ValueError(f"invalid instant: {text!r}")
        sign = 1 if offset.startswith("+") else -1
        tzinfo = timezone(sign * timedelta(hours=hours, minutes=minutes))
    try:
        return datetime(year, month, day, hour, minute, second, micros, tzinfo)
    except ValueError:
        raise ValueError(f"invalid instant: {text!r}") from None
