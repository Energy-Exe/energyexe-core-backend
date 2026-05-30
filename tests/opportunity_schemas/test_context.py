"""Tests for DetectionContext + DetectorResult.

All tests are DB-free: ``db`` is an AsyncMock and any query result is faked via
``execute(...)`` return values. These tests also pin the test-injection contract
(``prefetched=...``) that every downstream detector test (#92–#112) relies on.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)


def _make_db():
    """An AsyncMock session whose ``execute`` is awaitable."""
    db = MagicMock()
    db.execute = AsyncMock()
    return db


def test_detector_result_defaults():
    """DetectorResult exposes the agreed field surface with safe defaults."""
    r = DetectorResult(schema_code=SchemaCode.OPS_01, severity=Severity.WATCH)
    assert r.schema_code is SchemaCode.OPS_01
    assert r.severity is Severity.WATCH
    assert r.branch is None
    assert r.data_slots == {}
    assert r.missing_slots == []
    assert r.suppression_reason is None


def test_windfarm_id_accepts_object_or_int():
    """windfarm_id normalizes either a bare int or an ORM-like object."""
    ctx_int = DetectionContext(db=_make_db(), windfarm=42, period_start=START, period_end=END)
    assert ctx_int.windfarm_id == 42

    ctx_obj = DetectionContext(
        db=_make_db(), windfarm=SimpleNamespace(id=7), period_start=START, period_end=END
    )
    assert ctx_obj.windfarm_id == 7


@pytest.mark.asyncio
async def test_prefetched_values_are_returned_without_db():
    """prefetched short-circuits the accessor — proves the injection contract."""
    db = _make_db()
    sentinel = {"capture_rate": 0.62, "zone_avg": 0.69, "gap_pp": 7.0, "bidzone_code": "NO2"}
    ctx = DetectionContext(
        db=db,
        windfarm=1,
        period_start=START,
        period_end=END,
        prefetched={"capture_rate": sentinel},
    )

    result = await ctx.load_capture_rate()

    assert result is sentinel
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_capture_rate_is_memoized():
    """Two load_capture_rate() calls hit the DB exactly once."""
    db = _make_db()

    # Fake the PriceAnalyticsService so no real query is built.
    fake_pa = MagicMock()
    fake_pa.calculate_capture_rate = AsyncMock(
        return_value={"overall": {"capture_rate": 0.60}, "periods": []}
    )
    fake_pa.compare_capture_rates_by_bidzone = AsyncMock(
        return_value={"zone_average_capture_rate": 0.70}
    )

    # db.execute returns: bidzone_id lookup, then bidzone code lookup.
    bidzone_row = MagicMock()
    bidzone_row.scalar_one_or_none.return_value = 99
    code_row = MagicMock()
    code_row.scalar_one_or_none.return_value = "NO2"
    db.execute.side_effect = [bidzone_row, code_row]

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    ctx._price_analytics_svc = fake_pa  # inject fake analytics

    first = await ctx.load_capture_rate()
    second = await ctx.load_capture_rate()

    assert first == second
    assert first == {
        "capture_rate": 0.6,
        "zone_avg": 0.7,
        "gap_pp": 10.0,
        "bidzone_code": "NO2",
    }
    # Underlying DB call ran exactly once across the two accessor calls.
    assert db.execute.await_count == 2  # one bidzone lookup + one code lookup, total
    assert fake_pa.calculate_capture_rate.await_count == 1
    assert fake_pa.compare_capture_rates_by_bidzone.await_count == 1


@pytest.mark.asyncio
async def test_load_monthly_performance_matches_legacy_calc():
    """Accessor returns the normalized monthly rows from canned proxy rows.

    The legacy ``_calc_monthly_availability`` first tries the
    ``performance_summaries`` ORM query, then falls back to the raw-SQL
    availability proxy. Faking the ORM path against a mock session is brittle, so
    we drive the documented fallback path: the ORM query returns no rows and the
    proxy SQL returns canned rows. We assert the accessor produces the same
    shape/keys the legacy method produces for those same rows.
    """
    db = _make_db()

    # 1st execute() = performance_summaries ORM query -> no summaries (fallback).
    empty_summaries = MagicMock()
    empty_summaries.scalars.return_value.all.return_value = []

    # 2nd execute() = proxy SQL -> canned rows.
    proxy_result = MagicMock()
    proxy_result.fetchall.return_value = [
        SimpleNamespace(month="2024-01", gen_hours=700, total_hours=744, availability_pct=94.09),
        SimpleNamespace(month="2024-02", gen_hours=690, total_hours=696, availability_pct=99.14),
    ]
    db.execute.side_effect = [empty_summaries, proxy_result]

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    rows = await ctx.load_monthly_performance()

    assert rows == [
        {"month": "2024-01", "gen_hours": 700, "total_hours": 744, "availability_pct": 94.09},
        {"month": "2024-02", "gen_hours": 690, "total_hours": 696, "availability_pct": 99.14},
    ]
    # Each row carries exactly the legacy keys.
    for row in rows:
        assert set(row.keys()) == {"month", "gen_hours", "total_hours", "availability_pct"}

    # Memoized: a second call does not re-query.
    db.execute.reset_mock()
    rows_again = await ctx.load_monthly_performance()
    assert rows_again == rows
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_ppa_info_empty_when_no_ppa():
    """load_ppa_info returns {} when no PPA row exists, and memoizes."""
    db = _make_db()
    ppa_result = MagicMock()
    ppa_result.scalars.return_value.first.return_value = None
    db.execute.return_value = ppa_result

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_ppa_info() == {}

    db.execute.reset_mock()
    assert await ctx.load_ppa_info() == {}
    db.execute.assert_not_called()
