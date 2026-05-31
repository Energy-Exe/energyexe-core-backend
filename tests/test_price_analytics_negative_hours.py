"""Tests for ``PriceAnalyticsService.count_negative_price_hours`` (issue #105).

Powers MKT-06 (negative-price-hours exposure). The method counts DISTINCT hours
in ``[start, end)`` where the farm's NET generation is positive AND
``price_data.day_ahead_price < 0`` — non-generating hours are excluded (no
curtailment-avoided exposure when idle).

There is no Postgres in the test environment (``generation_data`` / ``price_data``
use ``JSONB`` and cannot be created on SQLite), so these tests exercise the
SQL-SHAPING logic: the session's ``execute`` is mocked (``AsyncMock``) to capture
the emitted query + bind params and to feed back a canned COUNT row. We assert
that:

  * the query restricts to generating hours (net generation > 0) AND negative
    prices (``day_ahead_price < 0``) — the two acceptance-criteria filters; and
  * the method returns the integer the DB reports (and 0, never None, for the
    empty / no-row cases).

The "excludes non-generating hours" behaviour is enforced by the SQL predicate;
the test proves that predicate is present and that a row the DB would have
filtered out (gen = 0) does not reach the count.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.price_analytics_service import PriceAnalyticsService

START = datetime(2025, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 777


def _service_with_count(count):
    """Build a service whose ``db.execute`` returns a canned COUNT row.

    The first ``execute`` call inside ``count_negative_price_hours`` is the
    ``_get_preferred_price_source`` lookup; the second is the count query. We
    record every call so the test can inspect the count SQL + params, and return
    the source row then the count row in order.
    """
    db = MagicMock()

    source_row = MagicMock()
    source_row.source = "ENTSOE"

    count_row = MagicMock()
    count_row.negative_hours = count

    source_result = MagicMock()
    source_result.fetchone.return_value = source_row

    count_result = MagicMock()
    count_result.fetchone.return_value = count_row

    db.execute = AsyncMock(side_effect=[source_result, count_result])
    return PriceAnalyticsService(db), db


def _count_call_sql_and_params(db):
    """Return (sql_text, params) of the COUNT query (the 2nd execute call)."""
    # call_args_list[0] = source lookup, [1] = the count query.
    count_call = db.execute.call_args_list[1]
    stmt = count_call.args[0]
    params = count_call.args[1]
    return str(stmt), params


@pytest.mark.asyncio
async def test_count_excludes_nongenerating_hours():
    """The count query filters to generating hours only (net generation > 0).

    Synthetic shape: the DB is told there are 3 qualifying hours. We assert the
    method returns exactly that count AND that the SQL carries BOTH guard
    predicates — net generation > 0 (so gen=0 hours are excluded) and
    day_ahead_price < 0 — so a non-generating hour can never be counted.
    """
    service, db = _service_with_count(3)

    result = await service.count_negative_price_hours(WF_ID, START, END)
    assert result == 3

    sql, params = _count_call_sql_and_params(db)
    normalized = " ".join(sql.split())

    # Generating-hours filter: net generation strictly positive (excludes gen=0).
    assert "(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0" in normalized
    # Negative-price filter.
    assert "p.day_ahead_price < 0" in normalized
    # Distinct clock-hours (multi-unit windfarms count each hour once).
    assert "COUNT(DISTINCT g.hour)" in normalized
    # Window bounds + windfarm are bound, not interpolated.
    assert params["windfarm_id"] == WF_ID
    assert params["start"] == START
    assert params["end"] == END


@pytest.mark.asyncio
async def test_count_returns_db_value():
    """The method returns the integer the DB reports for the count."""
    service, _ = _service_with_count(412)
    assert await service.count_negative_price_hours(WF_ID, START, END) == 412


@pytest.mark.asyncio
async def test_count_zero_when_no_negative_hours():
    """A 0 count comes back as 0 (never None)."""
    service, _ = _service_with_count(0)
    assert await service.count_negative_price_hours(WF_ID, START, END) == 0


@pytest.mark.asyncio
async def test_count_zero_when_null_count():
    """A NULL count (no rows aggregated) is normalised to 0, not None."""
    service, _ = _service_with_count(None)
    assert await service.count_negative_price_hours(WF_ID, START, END) == 0


@pytest.mark.asyncio
async def test_count_zero_when_no_row():
    """No COUNT row at all → 0 (defensive)."""
    db = MagicMock()
    source_row = MagicMock()
    source_row.source = "ENTSOE"
    source_result = MagicMock()
    source_result.fetchone.return_value = source_row
    empty_result = MagicMock()
    empty_result.fetchone.return_value = None
    db.execute = AsyncMock(side_effect=[source_result, empty_result])

    service = PriceAnalyticsService(db)
    assert await service.count_negative_price_hours(WF_ID, START, END) == 0


@pytest.mark.asyncio
async def test_count_joins_on_preferred_price_source():
    """The count query joins price_data on the farm's preferred source.

    ``_get_preferred_price_source`` resolves ENTSOE here; the count query must
    bind that as ``price_source`` so it only counts prices from the right feed.
    """
    service, db = _service_with_count(5)
    await service.count_negative_price_hours(WF_ID, START, END)

    _, params = _count_call_sql_and_params(db)
    assert params["price_source"] == "ENTSOE"
