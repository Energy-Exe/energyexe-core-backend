"""Unit tests for the Performance Analytics payload maths (EPR-186, EPR-198)."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.performance_analytics_service import PerformanceAnalyticsService as S
from app.services.performance_analytics_service import (
    add_months,
    hours_in_year,
    is_fallback_target,
    month_range,
    parse_month,
)


def test_parse_month_and_ranges():
    assert parse_month("2024-10") == (2024, 10)
    for bad in ("2024-13", "24-01", "2024", "abc"):
        with pytest.raises(ValueError):
            parse_month(bad)
    assert add_months((2024, 11), 3) == (2025, 2)
    assert add_months((2024, 1), -1) == (2023, 12)
    assert month_range((2024, 11), (2025, 2)) == [
        (2024, 11),
        (2024, 12),
        (2025, 1),
        (2025, 2),
    ]


def test_anchor_window_ends_at_the_last_index_month():
    # Norway (EPR-199): "24 months" to Sep 2026 with an index only to Dec 2025.
    index = month_range((2015, 1), (2025, 12))
    assert S.anchor_window((2024, 10), (2026, 9), index) == ((2024, 1), (2025, 12))
    # Data to the end of the window (or past it), or none at all: unchanged.
    assert S.anchor_window((2024, 10), (2026, 9), month_range((2020, 1), (2026, 9))) == (
        (2024, 10),
        (2026, 9),
    )
    assert S.anchor_window((2024, 10), (2026, 9), []) == ((2024, 10), (2026, 9))


def test_rolling_mean_uses_history_before_window():
    history = {add_months((2023, 1), i): float(90 + i) for i in range(24)}
    out = S.rolling_mean(history, [(2024, 1)])
    # Feb 2023 .. Jan 2024 -> values 91..102
    assert out[(2024, 1)] == pytest.approx(96.5)


def test_rolling_mean_needs_nine_of_twelve_months():
    history = {add_months((2024, 1), i): 100.0 for i in range(8)}
    assert S.rolling_mean(history, [(2024, 8)])[(2024, 8)] is None
    history[(2024, 9)] = 100.0
    assert S.rolling_mean(history, [(2024, 9)])[(2024, 9)] == 100.0


def test_cumulative_p50_analysis():
    months = [(2024, 1), (2024, 2), (2024, 3)]
    actual = {(2024, 1): 11.0, (2024, 2): 8.0, (2024, 3): 9.0}
    p50 = {m: 10.0 for m in months}
    out = S.p50_analysis(months, actual, p50)

    assert out["gap_gwh"] == -2.0
    assert out["avg_annual_generation_gwh"] == pytest.approx(28.0 / 3 * 12, abs=1e-3)
    assert out["cumulative"][-1] == {
        "month": "2024-03",
        "cumulative_p50_gwh": 30.0,
        "cumulative_actual_gwh": 28.0,
        "cumulative_gap_gwh": -2.0,
    }


def test_no_p50_months_gives_none():
    assert S.p50_analysis([(2024, 1)], {(2024, 1): 5.0}, {}) is None


def _full_year(year, gwh=10.0, ratio=1.0, p50_annual=120.0):
    months = [(year, mo) for mo in range(1, 13)]
    hours = {m: hours_in_year(year) // 12 + 1 for m in months}
    return (
        {m: ratio for m in months},
        {m: gwh for m in months},
        hours,
        {m: p50_annual / 12 for m in months},
    )


def test_annual_waterfall_reconciles():
    ratio, gen, hours, p50 = _full_year(2024, gwh=10.0, ratio=1.2, p50_annual=110.0)
    (w,) = S.annual_waterfalls(ratio, gen, hours, p50)

    assert w["complete"] and w["reason"] is None
    assert w["actual_gwh"] == 120.0
    assert w["expected_gwh"] == pytest.approx(100.0)
    assert w["performance_gwh"] == pytest.approx(20.0)  # beat its own curve
    assert w["wind_resource_gwh"] == pytest.approx(-10.0)  # weak wind year
    assert w["p50_gwh"] + w["wind_resource_gwh"] + w["performance_gwh"] == pytest.approx(
        w["actual_gwh"]
    )


def test_annual_ratio_is_unweighted_mean():
    ratio, gen, hours, p50 = _full_year(2024)
    # A big month at 1.5 and a small one at 0.5 average to 1.0, not output-weighted.
    ratio[(2024, 1)], gen[(2024, 1)] = 1.5, 100.0
    ratio[(2024, 7)], gen[(2024, 7)] = 0.5, 1.0
    (w,) = S.annual_waterfalls(ratio, gen, hours, p50)
    assert w["ratio_mean"] == 1.0
    assert w["performance_gwh"] == pytest.approx(0.0)


def test_annual_waterfall_guards():
    ratio, gen, hours, p50 = _full_year(2021)
    del ratio[(2021, 3)]
    (w,) = S.annual_waterfalls(ratio, gen, hours, p50)
    assert not w["complete"] and w["reason"] == "11 of 12 monthly performance ratios"
    assert w["performance_gwh"] is None

    ratio, gen, hours, p50 = _full_year(2022)
    hours[(2022, 2)] = 100
    (w,) = S.annual_waterfalls(ratio, gen, hours, p50)
    assert w["reason"] == "incomplete generation data"

    ratio, gen, hours, p50 = _full_year(2023)
    del p50[(2023, 12)]
    (w,) = S.annual_waterfalls(ratio, gen, hours, p50)
    assert w["reason"] == "no P50 target for the full year"

    ratio, gen, hours, p50 = _full_year(2024)
    ratio[(2024, 5)] = 0.0  # a zero ratio counts as missing
    (w,) = S.annual_waterfalls(ratio, gen, hours, p50)
    assert w["months_with_ratio"] == 11 and not w["complete"]


def test_annual_waterfalls_newest_first():
    a = _full_year(2023)
    b = _full_year(2024)
    merged = [{**x, **y} for x, y in zip(a, b)]
    assert [w["year"] for w in S.annual_waterfalls(*merged)] == [2024, 2023]


def test_monthly_p50_picks_covering_target():
    targets = [
        {"start": date(2020, 1, 1), "end": date(2024, 6, 30), "annual_gwh": 120.0, "source": "a"},
        {"start": date(2024, 7, 1), "end": None, "annual_gwh": 240.0, "source": "b"},
    ]
    by_month, used = S.monthly_p50(targets, [(2019, 12), (2024, 6), (2024, 7)])
    assert (2019, 12) not in by_month
    assert by_month[(2024, 6)] == 10.0
    assert by_month[(2024, 7)] == 20.0
    assert [t["source"] for t in used] == ["a", "b"]


def test_fallback_targets_are_detected():
    assert is_fallback_target("auto-computed (3-yr historical mean)", None)
    assert is_fallback_target(None, "fallback computed (3-yr historical mean)")
    assert not is_fallback_target("Original target before repowering", "https://x")
    assert not is_fallback_target(None, None)


def test_hours_in_year_leap():
    assert hours_in_year(2024) == 8784
    assert hours_in_year(2023) == 8760


def test_annual_waterfall_lists_p50_only_year():
    p50 = {(2022, m): 10.0 for m in range(1, 13)}
    (w,) = S.annual_waterfalls({}, {}, {}, p50)
    assert w["year"] == 2022 and w["months_with_ratio"] == 0
    assert w["reason"] == "0 of 12 monthly performance ratios"


def test_monthly_p50_latest_start_wins_on_overlap():
    targets = [
        {"start": date(2020, 1, 1), "end": None, "annual_gwh": 120.0, "source": "old"},
        {"start": date(2023, 1, 1), "end": None, "annual_gwh": 240.0, "source": "new"},
    ]
    by_month, used = S.monthly_p50(targets, [(2023, 5)])
    assert by_month[(2023, 5)] == 20.0 and [t["source"] for t in used] == ["new"]


def _service_with(targets, gen_history):
    svc = S(AsyncMock())
    wf = SimpleNamespace(
        id=1,
        name="Farm",
        nameplate_capacity_mw=10.0,
        bidzone_id=None,
        bidzone_code=None,
        bidzone_name=None,
        country_id=1,
        country_name="Norway",
    )
    svc._windfarm = AsyncMock(return_value=wf)
    svc._summary_history = AsyncMock(return_value=({}, {}))
    svc._monthly_prices = AsyncMock(return_value=({}, None))
    svc._generation_history = AsyncMock(return_value=gen_history)
    svc._p50_targets = AsyncMock(return_value=targets)
    return svc


@pytest.mark.asyncio
async def test_waterfall_kept_when_window_has_no_p50_months():
    """EPR-198 waterfalls are window-independent: a window before the target
    still returns the block, with an empty window analysis."""
    targets = [{"start": date(2023, 1, 1), "end": None, "annual_gwh": 120.0, "source": "s"}]
    gen = ({(2023, m): 10.0 for m in range(1, 13)}, {(2023, m): 700 for m in range(1, 13)})
    data = await _service_with(targets, gen).get_performance(1, (2021, 1), (2021, 12))
    p50 = data["p50"]
    assert p50["months_used"] == 0 and p50["cumulative"] == []
    assert p50["annual_target_gwh"] == 120.0
    assert [w["year"] for w in p50["annual_waterfalls"]] == [2023]


@pytest.mark.asyncio
async def test_no_targets_gives_no_p50_block():
    data = await _service_with([], ({}, {})).get_performance(1, (2021, 1), (2021, 12))
    assert data["p50"] is None


@pytest.mark.asyncio
async def test_unknown_windfarm_returns_none():
    svc = S(AsyncMock())
    svc._windfarm = AsyncMock(return_value=None)
    assert await svc.get_performance(1, (2021, 1), (2021, 2)) is None
