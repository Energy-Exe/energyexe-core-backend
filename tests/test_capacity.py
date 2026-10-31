"""Unit tests for app/utils/capacity.py — installed capacity in force per hour."""

import numpy as np
import pandas as pd
import pytest

from app.utils.capacity import (
    BASIS_IN_FORCE,
    CAPACITY_BASIS_ATTR,
    STATUS_BLOCKED,
    STATUS_IN_FORCE,
    STATUS_NO_UNITS,
    UnitWindow,
    anchor_date,
    attach_capacity,
    capacity_at,
    capacity_basis_status,
    capacity_col,
    capacity_series,
    monthly_capacity,
    utc_midnight,
    weighted_capacity,
    windows_from_rows,
)

TODAY = "2026-10-31"


def _w(cap, start, end=None):
    return UnitWindow(float(cap), utc_midnight(start), utc_midnight(end))


def _hours(start, end):
    return pd.Series(pd.date_range(start, end, freq="h", tz="UTC", inclusive="left"))


# ─── Window arithmetic ──────────────────────────────────────────


class TestCapacitySeries:
    def test_phased_build_out_steps_up_on_the_phase_start(self):
        windows = [_w(10, "2020-01-01"), _w(20, "2021-01-01")]
        hours = _hours("2020-06-01", "2021-06-01")
        cap = capacity_series(windows, hours, nameplate=30.0, today=TODAY)
        assert (cap[hours.dt.year == 2020] == 10.0).all()
        assert (cap[hours.dt.year == 2021] == 30.0).all()

    def test_partial_decommission_smola(self):
        # Phase 2 150.4 MW to 2024-02-01 (exclusive), Phase 3 145.8 MW from then on.
        windows = [_w(150.4, "2005-11-20", "2024-02-01"), _w(145.8, "2024-02-01")]
        hours = pd.Series(
            pd.to_datetime(
                ["2015-06-01 12:00", "2024-01-31 23:00", "2024-02-01 00:00", "2025-12-31 23:00"],
                utc=True,
            )
        )
        cap = capacity_series(windows, hours, nameplate=145.8, today=TODAY)
        assert list(cap.round(1)) == [150.4, 150.4, 145.8, 145.8]

    def test_repower_phases_touching_on_boundary_day_count_once(self):
        # Midtfjellet shape: unit A ends 2012-12-31, unit B starts 2012-12-31.
        windows = [_w(12.5, "2012-01-01", "2012-12-31"), _w(25.0, "2012-12-31")]
        hours = pd.Series(pd.to_datetime(["2012-12-30 23:00", "2012-12-31 00:00"], utc=True))
        cap = capacity_series(windows, hours, nameplate=25.0, today=TODAY)
        assert list(cap) == [12.5, 25.0]

    def test_hours_before_any_unit_fall_back_to_nameplate(self):
        windows = [_w(40, "2010-01-01")]
        hours = pd.Series(pd.to_datetime(["2009-12-31 23:00", "2010-01-01 00:00"], utc=True))
        cap = capacity_series(windows, hours, nameplate=40.0, today=TODAY)
        assert list(cap) == [40.0, 40.0]

    def test_no_units_gives_constant_nameplate(self):
        hours = _hours("2020-01-01", "2020-01-02")
        cap = capacity_series([], hours, nameplate=99.0, today=TODAY)
        assert (cap == 99.0).all()
        assert capacity_basis_status([], 99.0, today=TODAY) == STATUS_NO_UNITS

    def test_non_utc_hours_are_compared_at_utc_midnight(self):
        windows = [_w(150.4, "2005-11-20", "2024-02-01"), _w(145.8, "2024-02-01")]
        # 00:30 Oslo time on 1 Feb is still 31 Jan in UTC → old capacity.
        hours = pd.Series(
            pd.to_datetime(["2024-02-01 00:30", "2024-02-01 01:30"]).tz_localize("Europe/Oslo")
        )
        cap = capacity_series(windows, hours, nameplate=145.8, today=TODAY)
        assert list(cap.round(1)) == [150.4, 145.8]

    def test_raw_mode_returns_unit_sum_unscaled(self):
        windows = [_w(260.0, "2012-09-01"), _w(262.0, "2012-09-01")]
        hours = _hours("2020-01-01", "2020-01-02")
        assert (capacity_series(windows, hours, 521.8, anchor=False, today=TODAY) == 522.0).all()


# ─── Anchoring and tolerance ────────────────────────────────────


class TestAnchoring:
    def test_registry_rounding_is_absorbed_so_constant_farms_are_unchanged(self):
        # Clyde: units sum to 522.0, nameplate 521.8 → anchored series equals nameplate.
        windows = [_w(112.1, "2012-09-01"), _w(108.1, "2012-09-01"), _w(301.8, "2017-06-01")]
        hours = _hours("2020-01-01", "2020-01-03")
        cap = capacity_series(windows, hours, nameplate=521.8, today=TODAY)
        assert np.allclose(cap, 521.8)
        assert capacity_basis_status(windows, 521.8, today=TODAY) == STATUS_IN_FORCE

    def test_anchoring_scales_history_by_the_same_ratio(self):
        windows = [_w(112.1, "2012-09-01"), _w(108.1, "2012-09-01"), _w(301.8, "2017-06-01")]
        hours = _hours("2015-01-01", "2015-01-02")
        cap = capacity_series(windows, hours, nameplate=521.8, today=TODAY)
        assert cap.iloc[0] == pytest.approx(220.2 * 521.8 / 522.0)

    def test_registry_disagreeing_with_nameplate_is_blocked(self):
        # Mailiao: nameplate 36, but units say 46 MW since 2024 → never invent 28.2 MW.
        windows = [_w(36, "2000-12-27", "2023-12-31"), _w(46, "2024-01-01")]
        hours = pd.Series(pd.to_datetime(["2020-06-01", "2025-06-01"], utc=True))
        cap = capacity_series(windows, hours, nameplate=36.0, today=TODAY)
        assert list(cap) == [36.0, 36.0]
        assert capacity_basis_status(windows, 36.0, today=TODAY) == STATUS_BLOCKED

    def test_future_dated_unit_is_not_in_force_today_and_blocks(self):
        # Crossdykes typo: first power 2121 → today's sum is half the nameplate.
        windows = [_w(23, "2021-06-01"), _w(23, "2121-06-01")]
        assert capacity_basis_status(windows, 46.0, today=TODAY) == STATUS_BLOCKED
        hours = _hours("2023-01-01", "2023-01-02")
        assert (capacity_series(windows, hours, 46.0, today=TODAY) == 46.0).all()

    def test_fully_decommissioned_farm_anchors_on_its_last_in_force_day(self):
        windows = [_w(50, "2010-01-01", "2020-01-01")]
        assert anchor_date(windows, today=TODAY) == utc_midnight("2019-12-31")
        hours = pd.Series(pd.to_datetime(["2015-06-01", "2021-06-01"], utc=True))
        cap = capacity_series(windows, hours, nameplate=50.0, today=TODAY)
        assert list(cap) == [50.0, 50.0]  # second hour: S=0 → nameplate

    def test_anchor_is_independent_of_the_hours_passed(self):
        windows = [_w(150.4, "2005-11-20", "2024-02-01"), _w(145.8, "2024-02-01")]
        one_year = pd.Series(pd.to_datetime(["2020-06-01"], utc=True))
        full = pd.Series(pd.to_datetime(["2020-06-01", "2025-06-01"], utc=True))
        a = capacity_series(windows, one_year, 145.8, today=TODAY).iloc[0]
        b = capacity_series(windows, full, 145.8, today=TODAY).iloc[0]
        assert a == b == pytest.approx(150.4)

    def test_capacity_at_matches_series(self):
        windows = [_w(150.4, "2005-11-20", "2024-02-01"), _w(145.8, "2024-02-01")]
        assert capacity_at(windows, "2020-01-01", 145.8, today=TODAY) == pytest.approx(150.4)
        assert capacity_at(windows, "2025-01-01", 145.8, today=TODAY) == pytest.approx(145.8)
        assert capacity_at([], "2025-01-01", 145.8, today=TODAY) == 145.8


# ─── Frame helpers ──────────────────────────────────────────────


class TestFrameHelpers:
    def _df(self):
        hours = _hours("2022-12-31 22:00", "2023-01-01 02:00")
        return pd.DataFrame({"hour": hours, "generation_mwh": [7.5, 7.5, 15.0, 15.0]})

    def test_attach_capacity_sets_column_p_pu_and_marker(self):
        windows = [_w(15, "2022-01-01"), _w(15, "2023-01-01")]
        df = attach_capacity(self._df(), windows, nameplate=30.0, today=TODAY)
        assert list(df["capacity_mw"]) == [15.0, 15.0, 30.0, 30.0]
        assert list(df["p_pu"]) == [0.5, 0.5, 0.5, 0.5]
        assert df.attrs[CAPACITY_BASIS_ATTR] == BASIS_IN_FORCE

    def test_capacity_col_prefers_column(self):
        df = pd.DataFrame({"capacity_mw": [1.0, 2.0]})
        assert list(capacity_col(df, 99.0)) == [1.0, 2.0]

    def test_capacity_col_is_constant_for_plain_fixtures(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        assert list(capacity_col(df, 30.0)) == [30.0, 30.0, 30.0]

    def test_capacity_col_refuses_silent_fallback_on_in_force_frames(self):
        windows = [_w(30, "2022-01-01")]
        df = attach_capacity(self._df(), windows, nameplate=30.0, today=TODAY)
        projected = df[["hour", "p_pu"]]  # attrs survive projection in pandas 2.x
        with pytest.raises(ValueError, match="capacity_mw"):
            capacity_col(projected, 30.0)

    def test_windows_from_rows_skips_zero_capacity_and_uses_first_power(self):
        rows = [
            {"capacity_mw": 10, "first_power_date": "2020-01-01", "start_date": "2019-06-01"},
            {"capacity_mw": 0, "start_date": "2020-01-01"},
            {"capacity_mw": 5, "start_date": "2021-01-01", "end_date": "2022-01-01"},
        ]
        ws = windows_from_rows(rows)
        assert len(ws) == 2
        assert ws[0].start == utc_midnight("2020-01-01")
        assert ws[1].end == utc_midnight("2022-01-01")


# ─── Period helpers (capacity-factor views) ─────────────────────


class TestPeriodHelpers:
    def test_monthly_capacity_prorates_a_mid_month_phase_boundary(self):
        windows = [_w(10, "2020-01-01"), _w(10, "2024-02-15")]
        caps = monthly_capacity(windows, ["2024-01", "2024-02", "2024-03"], 20.0, today=TODAY)
        assert caps["2024-01"] == pytest.approx(10.0)
        # Feb 2024 = 696 h: 14 days (336 h) at 10 MW, 15 days (360 h) at 20 MW.
        assert caps["2024-02"] == pytest.approx((336 * 10 + 360 * 20) / 696)
        assert caps["2024-03"] == pytest.approx(20.0)

    def test_weighted_capacity_weights_by_hours_present_in_both(self):
        caps = {"2024-01": 10.0, "2024-02": 20.0, "2024-03": 30.0}
        hours = {"2024-01": 744, "2024-02": 696}
        assert weighted_capacity(caps, hours) == pytest.approx((744 * 10 + 696 * 20) / 1440)
        assert weighted_capacity(caps, {}) is None


class TestRegistryGuard:
    """Farms whose output contradicts the unit history stay on constant nameplate."""

    def test_blocked_farm_gets_constant_nameplate_everywhere(self):
        from app.utils.capacity import (
            REGISTRY_BLOCKED_WINDFARM_IDS,
            STATUS_BLOCKED,
            capacity_at,
            capacity_basis_status,
            monthly_capacity,
        )

        blocked = next(iter(REGISTRY_BLOCKED_WINDFARM_IDS))
        windows = [_w(75.9, "2013-02-01"), _w(49.2, "2023-10-26")]  # Chungwei as registered
        hours = _hours("2020-01-01", "2020-01-03")
        assert capacity_basis_status(windows, 125.1, windfarm_id=blocked) == STATUS_BLOCKED
        assert (capacity_series(windows, hours, 125.1, windfarm_id=blocked) == 125.1).all()
        assert capacity_at(windows, "2020-06-01", 125.1, windfarm_id=blocked) == 125.1
        assert monthly_capacity(windows, ["2020-06"], 125.1, windfarm_id=blocked) == {
            "2020-06": 125.1
        }
        # Without the id the same registry would (wrongly) give 75.9 MW in 2020.
        assert np.allclose(capacity_series(windows, hours, 125.1), 75.9)
        df = pd.DataFrame({"hour": hours, "generation_mwh": 90.0})
        attach_capacity(df, windows, 125.1, windfarm_id=blocked)
        assert (df["capacity_mw"] == 125.1).all()

    def test_implausible_years_flags_only_years_with_heavy_excess(self):
        from app.utils.capacity import implausible_years

        df = pd.DataFrame(
            {
                "year": [2020] * 100 + [2021] * 100,
                # 2020: 25 % of hours above the ceiling; 2021: 5 % (early commissioning)
                "p_pu": [1.2] * 25 + [0.5] * 75 + [1.2] * 5 + [0.5] * 95,
            }
        )
        assert implausible_years(df, 1.02) == {2020: 0.25}
        assert implausible_years(df, 1.02, min_share=0.04) == {2020: 0.25, 2021: 0.05}
        assert implausible_years(pd.DataFrame(), 1.02) == {}
