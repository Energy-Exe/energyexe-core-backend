"""Unit tests for the Generation page payload maths (EPR-187)."""

from datetime import date

import pytest

from app.services.generation_analytics_service import GenerationAnalyticsService as G
from app.services.generation_analytics_service import hours_in_month, period_key, period_keys


def test_period_keys_per_granularity():
    assert period_keys(date(2024, 2, 28), date(2024, 3, 1), "day") == [
        "2024-02-28",
        "2024-02-29",
        "2024-03-01",
    ]
    assert period_keys(date(2024, 11, 1), date(2025, 2, 28), "month") == [
        "2024-11",
        "2024-12",
        "2025-01",
        "2025-02",
    ]
    assert period_keys(date(2021, 1, 1), date(2023, 12, 31), "year") == [
        "2021",
        "2022",
        "2023",
    ]
    assert period_key(date(2024, 5, 9), "month") == "2024-05"
    assert hours_in_month(2024, 2) == 696


def test_roll_up_years_sums_energy_and_hours():
    out = G.roll_up_years({"2024-01": (10.0, 744), "2024-02": (5.0, 696), "2025-01": (1.0, 10)})
    assert out == {"2024": (15.0, 1440), "2025": (1.0, 10)}


def test_capacity_factor_uses_hours_covered():
    assert G.capacity_factor(500.0, 10.0, 100) == pytest.approx(0.5)
    assert G.capacity_factor(None, 10.0, 100) is None
    assert G.capacity_factor(1.0, None, 100) is None
    assert G.capacity_factor(1.0, 10.0, 0) is None


def test_build_points_fills_missing_periods_with_null():
    pts = G.build_points(["2024-01", "2024-02"], {"2024-02": (720.0, 720)}, 2.0)
    assert pts[0] == {"period": "2024-01", "mwh": None, "hours": None, "cf": None}
    assert pts[1]["cf"] == pytest.approx(0.5)


def test_summary_best_worst_skip_empty_periods():
    pts = G.build_points(
        ["2024-01", "2024-02", "2024-03"],
        {"2024-01": (100.0, 100), "2024-03": (300.0, 100)},
        10.0,
    )
    s = G.summarise(pts, 10.0)
    assert s["total_mwh"] == 400.0
    assert s["avg_mwh"] == 200.0
    # Window CF is energy-weighted: 400 / (10 MW x 200 h).
    assert s["cf"] == pytest.approx(0.2)
    assert s["best"] == {"period": "2024-03", "mwh": 300.0}
    assert s["worst"] == {"period": "2024-01", "mwh": 100.0}
    assert s["periods_with_data"] == 2


def test_summary_without_data():
    pts = G.build_points(["2024-01"], {}, 10.0)
    assert G.summarise(pts, 10.0)["total_mwh"] is None
