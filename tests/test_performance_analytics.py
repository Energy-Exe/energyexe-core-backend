"""Unit tests for the Performance Analytics payload maths (EPR-186, EPR-198)."""

from datetime import date

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
