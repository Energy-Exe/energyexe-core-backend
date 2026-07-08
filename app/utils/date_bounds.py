"""Inclusive→exclusive end-date normalization for analytics endpoints."""

from datetime import datetime, time, timedelta


def exclusive_end(end_date: datetime) -> datetime:
    """Exclusive upper bound for a user-supplied inclusive end_date.

    Clients send either a date-only end (midnight, meaning "through that whole
    day" — the bound is the next midnight) or an explicit end-of-day timestamp
    (e.g. 23:59:59.999). The previous blanket ``end_date + 1 day`` overshot
    timestamped ends by a full day, leaking an extra bucket into calendar
    windows ("previous year" charted 13 months) and inflating range totals.
    """
    if end_date.time() == time.min:
        return end_date + timedelta(days=1)
    return end_date + timedelta(microseconds=1)
