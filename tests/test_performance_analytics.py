"""Unit tests for the Performance Analytics payload maths (EPR-186)."""

from datetime import date

import pytest

from app.services.performance_analytics_service import (
    PerformanceAnalyticsService as S,
    add_months,
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


def test_waterfall_reconciles_and_splits_by_index():
    months = [(2024, 1), (2024, 2), (2024, 3)]
    actual = {(2024, 1): 11.0, (2024, 2): 8.0, (2024, 3): 9.0}
    p50 = {m: 10.0 for m in months}
    # Mar has no index: its whole gap goes to wind.
    index = {(2024, 1): 110.0, (2024, 2): 80.0}
    out = S.p50_analysis(months, actual, p50, index)

    w = out["waterfall"]
    assert w["p50_gwh"] == 30.0
    assert w["actual_gwh"] == 28.0
    assert w["months_without_index"] == 1
    # Jan: expected 10 -> wind 0, perf +1. Feb: expected 10 -> wind 0, perf -2.
    assert w["performance_gwh"] == pytest.approx(-1.0)
    assert w["wind_resource_gwh"] == pytest.approx(-1.0)
    assert w["p50_gwh"] + w["wind_resource_gwh"] + w["performance_gwh"] == pytest.approx(
        w["actual_gwh"]
    )
    assert out["gap_gwh"] == -2.0
    assert out["avg_annual_generation_gwh"] == pytest.approx(28.0 / 3 * 12, abs=1e-3)
    assert out["cumulative"][-1] == {
        "month": "2024-03",
        "cumulative_p50_gwh": 30.0,
        "cumulative_actual_gwh": 28.0,
        "cumulative_gap_gwh": -2.0,
    }


def test_zero_index_treated_as_missing():
    out = S.p50_analysis([(2024, 1)], {(2024, 1): 5.0}, {(2024, 1): 10.0}, {(2024, 1): 0.0})
    assert out["waterfall"]["months_without_index"] == 1
    assert out["waterfall"]["wind_resource_gwh"] == -5.0


def test_no_p50_months_gives_none():
    assert S.p50_analysis([(2024, 1)], {(2024, 1): 5.0}, {}, {}) is None


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
