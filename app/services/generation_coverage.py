"""Per-windfarm generation coverage — the last day a windfarm has a metered reading.

EPR-126: one definition shared by the Opportunity report's ``effective_window``
and the nightly detection window clip (``OpportunityDetectionService``), so the
report and the persisted findings on the client board agree on where a farm's
data ends. Norwegian (NVE) farms in particular lag by months — prices run to
today while generation stops at the previous year end — and any window that
runs past the generation feed averages price-only hours into metrics that
divide a generation-weighted quantity by a time-weighted one.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_data import MONTHLY_SOURCES, GenerationData


@dataclass(frozen=True)
class GenerationCoverage:
    """Where a windfarm's generation data runs out inside a window."""

    last_hour: datetime  # aware UTC — the latest qualifying row's ``hour``
    last_day: date  # UTC day the data runs THROUGH (month end for MONTHLY_SOURCES)
    source: str


def month_end(d: date) -> date:
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


def as_utc(dt: datetime) -> datetime:
    """A bind-safe UTC instant.

    A naive datetime is *declared* UTC (never ``astimezone``'d): asyncpg encodes
    naive datetimes for ``timestamptz`` in the process-local timezone, which
    silently shifts a window on any machine not running in UTC. The nightly
    passes naive-UTC bounds and the report passes aware ones; both must bind
    to the same instant.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def data_through_day(last_hour: datetime, source: Optional[str]) -> date:
    """The UTC day a row proves data through.

    EIA / ENERGISTYRELSEN store a whole month's MWh at a single hour (the 1st
    at 00:00 — ``MONTHLY_SOURCES``), so their latest row means "through that
    month's end", not "through that hour".
    """
    day = as_utc(last_hour).date()
    if source in MONTHLY_SOURCES:
        return month_end(day)
    return day


async def generation_data_through(
    db: AsyncSession, windfarm_id: int, start: datetime, end: datetime
) -> Optional[GenerationCoverage]:
    """Latest hour in ``[start, end)`` with a non-NULL metered/generation reading.

    ``ORDER BY hour DESC LIMIT 1`` rather than ``MAX(hour)`` so the row's
    ``source`` is available for the monthly-source branch; both plan as a
    backward scan on ``idx_gen_windfarm_hour`` for a single windfarm. Never
    aggregate over an id list here — a grouped MAX over ``generation_data``
    is planned as a full seq scan (see ``WindfarmService``).

    Returns ``None`` when the farm has no reading in the window at all.
    """
    stmt = (
        select(GenerationData.hour, GenerationData.source)
        .where(
            GenerationData.windfarm_id == windfarm_id,
            GenerationData.hour >= as_utc(start),
            GenerationData.hour < as_utc(end),
            func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh).isnot(None),
        )
        .order_by(GenerationData.hour.desc())
        .limit(1)
    )
    row = (await db.execute(stmt)).first()
    if row is None or row[0] is None:
        return None
    last_hour, source = row[0], row[1]
    return GenerationCoverage(
        last_hour=as_utc(last_hour),
        last_day=data_through_day(last_hour, source),
        source=source or "",
    )
