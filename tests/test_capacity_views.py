"""Capacity-in-force denominators for the capacity-factor views (Part B).

The pipeline helpers are covered in test_capacity.py; this file covers the
period helpers (`capacity_hours`, `monthly_capacity`, `weighted_capacity`) and
the view services' acceptance of a per-period capacity mapping.
"""

from datetime import date

import pandas as pd
import pytest

from app.services.comparison_analytics_service import cf_series
from app.services.generation_analytics_service import GenerationAnalyticsService as G
from app.services.generation_analytics_service import capacity_for
from app.utils.capacity import (
    UnitWindow,
    capacity_hours,
    capacity_series,
    in_force_hours,
    monthly_capacity,
    utc_midnight,
    weighted_capacity,
)

TODAY = date(2026, 10, 31)


def _w(cap, start, end=None):
    return UnitWindow(float(cap), utc_midnight(start), utc_midnight(end) if end else None)


# Two-phase farm: 15 MW from 2022-01-01, +15 MW from 2023-01-01 (nameplate 30).
PHASED = [_w(15, "2022-01-01"), _w(15, "2023-01-01")]


class TestCapacityHours:
    def test_constant_farm_is_nameplate_times_hours(self):
        w = [_w(30, "2010-01-01")]
        assert capacity_hours(
            w, date(2024, 1, 1), date(2025, 1, 1), 30.0, today=TODAY
        ) == pytest.approx(30.0 * 8784)

    def test_no_units_is_nameplate_times_hours(self):
        assert capacity_hours(
            [], date(2024, 1, 1), date(2024, 2, 1), 12.0, today=TODAY
        ) == pytest.approx(12.0 * 744)

    def test_phase_year_uses_capacity_in_force(self):
        assert capacity_hours(
            PHASED, date(2022, 1, 1), date(2023, 1, 1), 30.0, today=TODAY
        ) == pytest.approx(15.0 * 8760)
        assert capacity_hours(
            PHASED, date(2023, 1, 1), date(2024, 1, 1), 30.0, today=TODAY
        ) == pytest.approx(30.0 * 8760)

    def test_hours_before_any_unit_count_at_nameplate(self):
        # 2021 is before the first unit: nameplate, exactly as the hourly helper does.
        assert capacity_hours(
            PHASED, date(2021, 1, 1), date(2022, 1, 1), 30.0, today=TODAY
        ) == pytest.approx(30.0 * 8760)

    def test_blocked_registry_falls_back_to_nameplate(self):
        # units sum to 46, nameplate 36 → > 3 % → constant nameplate
        w = [_w(23, "2020-01-01"), _w(23, "2024-01-01")]
        assert capacity_hours(
            w, date(2021, 1, 1), date(2022, 1, 1), 36.0, today=TODAY
        ) == pytest.approx(36.0 * 8760)

    def test_anchoring_scales_rounding(self):
        # registry 521.8 vs nameplate 522 → anchored to 522
        w = [_w(521.8, "2010-01-01")]
        assert capacity_hours(
            w, date(2024, 1, 1), date(2024, 1, 2), 522.0, today=TODAY
        ) == pytest.approx(522.0 * 24)

    def test_matches_integral_of_hourly_series(self):
        hours = pd.date_range("2022-12-25", "2023-01-08", freq="h", inclusive="left", tz="UTC")
        series = capacity_series(PHASED, hours, 30.0, today=TODAY)
        assert capacity_hours(
            PHASED, hours[0], hours[-1] + pd.Timedelta(hours=1), 30.0, today=TODAY
        ) == pytest.approx(float(series.sum()))

    def test_accepts_tz_aware_datetimes_not_at_midnight(self):
        s = pd.Timestamp("2023-06-01 06:00", tz="UTC")
        e = pd.Timestamp("2023-06-01 18:00", tz="UTC")
        assert capacity_hours(PHASED, s, e, 30.0, today=TODAY) == pytest.approx(30.0 * 12)

    def test_in_force_hours_unions_overlapping_windows(self):
        s, e = utc_midnight("2023-01-01"), utc_midnight("2023-01-02")
        assert in_force_hours(PHASED, s, e) == 24
        gap = [
            _w(10, "2020-01-01", "2023-01-01"),
            UnitWindow(10.0, pd.Timestamp("2023-01-01 12:00", tz="UTC"), None),
        ]
        assert in_force_hours(gap, s, e) == 12


class TestMonthlyCapacity:
    def test_phase_boundary_inside_month_is_hours_weighted(self):
        w = [_w(15, "2022-01-01"), _w(15, "2023-01-16")]
        caps = monthly_capacity(w, ["2022-12", "2023-01", "2023-02"], 30.0, today=TODAY)
        assert caps["2022-12"] == pytest.approx(15.0)
        assert caps["2023-01"] == pytest.approx((15 * 15 * 24 + 30 * 16 * 24) / 744)
        assert caps["2023-02"] == pytest.approx(30.0)

    def test_weighted_capacity_uses_hours_present(self):
        caps = {"2023-01": 15.0, "2023-02": 30.0, "2023-03": 30.0}
        hours = {"2023-01": 744, "2023-02": 672}  # March has no data
        assert weighted_capacity(caps, hours) == pytest.approx((15 * 744 + 30 * 672) / (744 + 672))
        assert weighted_capacity(caps, {}) is None


class TestViewsAcceptMapping:
    def test_capacity_for_float_or_mapping(self):
        assert capacity_for(10.0, "2024-01") == 10.0
        assert capacity_for({"2024-01": 5.0}, "2024-01") == 5.0
        assert capacity_for({"2024-01": 5.0}, "2024-02") is None
        assert capacity_for(None, "2024-01") is None

    def test_build_points_per_period_capacity(self):
        pts = G.build_points(
            ["2022", "2023"],
            {"2022": (15 * 8760 * 0.4, 8760), "2023": (30 * 8760 * 0.4, 8760)},
            {"2022": 15.0, "2023": 30.0},
        )
        assert [p["cf"] for p in pts] == [0.4, 0.4]
        # the old constant basis would halve 2022
        old = G.build_points(["2022"], {"2022": (15 * 8760 * 0.4, 8760)}, 30.0)
        assert old[0]["cf"] == 0.2

    def test_summarise_weights_capacity_by_hours(self):
        pts = G.build_points(
            ["2022", "2023"],
            {"2022": (15 * 8760 * 0.4, 8760), "2023": (30 * 8760 * 0.4, 8760)},
            {"2022": 15.0, "2023": 30.0},
        )
        s = G.summarise(pts, {"2022": 15.0, "2023": 30.0})
        assert s["cf"] == 0.4
        # float capacity keeps the legacy behaviour
        assert G.summarise(pts, 30.0)["cf"] == pytest.approx(0.3, abs=1e-4)

    def test_capacity_by_period_year_weights_months_with_data(self):
        w = [_w(15, "2022-01-01"), _w(15, "2022-07-01")]
        monthly_gen = {f"2022-{m:02d}": (100.0, 24 * 30) for m in (1, 2, 3)}  # data only in H1
        caps = G.capacity_by_period(w, 30.0, ["2022"], "year", monthly_gen)
        assert caps["2022"] == pytest.approx(15.0)
        # no data at all → plain mean of the twelve months
        caps_none = G.capacity_by_period(w, 30.0, ["2022"], "year", {})
        assert 15.0 < caps_none["2022"] < 30.0
        # days take their month's capacity
        caps_day = G.capacity_by_period(w, 30.0, ["2022-06-30", "2022-07-01"], "day", {})
        assert caps_day["2022-06-30"] == pytest.approx(15.0)
        assert caps_day["2022-07-01"] == pytest.approx(30.0)
        assert G.capacity_by_period(w, None, ["2022"], "year", {}) is None

    def test_cf_series_with_mapping(self):
        by_month = {"2023-01": (15 * 744 * 0.4, 744), "2023-02": (30 * 672 * 0.4, 672)}
        assert cf_series(["2023-01", "2023-02"], by_month, {"2023-01": 15.0, "2023-02": 30.0}) == [
            40.0,
            40.0,
        ]
        assert cf_series(["2023-01", "2023-02"], by_month, 30.0) == [20.0, 40.0]
