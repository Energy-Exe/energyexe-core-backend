"""Curve units == hourly units (Aje, EPR-184 dev note).

A two-phase farm (15 MW in 2022, 30 MW in 2023) with identical specific output
in both years. On the in-force capacity basis every module must see the two
years as the same farm: same curve, index ≈ 100, no lost energy, no trend.
The same fixture on the old constant-nameplate basis shows the bias, which is
what this file guards against.
"""

import numpy as np
import pandas as pd
import pytest

from app.services.degradation_service import DegradationService
from app.services.performance_anomaly_service import PerformanceAnomalyService
from app.services.performance_pipeline_service import PerformancePipelineService
from app.services.power_curve_service import PowerCurveService
from app.services.wind_normalisation_service import WindNormalisationService
from app.utils.capacity import UnitWindow, attach_capacity, utc_midnight

NAMEPLATE = 30.0
TODAY = "2026-10-31"
WINDOWS = [
    UnitWindow(15.0, utc_midnight("2022-01-01"), None),
    UnitWindow(15.0, utc_midnight("2023-01-01"), None),
]


def _two_phase_farm(seed=7):
    rng = np.random.default_rng(seed)
    hours = pd.date_range("2022-01-01", "2024-01-01", freq="h", tz="UTC", inclusive="left")
    n = len(hours)
    wind = np.clip(rng.weibull(2.0, n) * 8.0, 0, 25.0)
    pu_clean = 1.0 / (1.0 + np.exp(-(wind - 8.5)))
    pu = np.clip(pu_clean + rng.normal(0, 0.03, n), 0.0, 1.0)
    installed = np.where(hours.year == 2022, 15.0, 30.0)
    df = pd.DataFrame(
        {
            "hour": hours,
            "year": hours.year.astype(int),
            "generation_mwh": pu * installed,
            "wind_speed": wind,
            "market_price": 50.0,
        }
    )
    return df


def _in_force(df):
    return attach_capacity(df.copy(), WINDOWS, NAMEPLATE, today=TODAY)


def _constant(df):
    out = df.copy()
    out["p_pu"] = out["generation_mwh"] / NAMEPLATE
    return out


def _curve_lookup(stats):
    return {float(r.wind_bin_left): float(r.q50_pu) for r in stats.itertuples()}


@pytest.fixture(scope="module")
def farm():
    return _two_phase_farm()


class TestInForceBasisIsConsistent:
    def test_capacity_column_follows_the_phases(self, farm):
        df = _in_force(farm)
        assert (df.loc[df.year == 2022, "capacity_mw"] == 15.0).all()
        assert (df.loc[df.year == 2023, "capacity_mw"] == 30.0).all()

    def test_curve_is_identical_across_phases(self, farm):
        df = _in_force(farm)
        s22 = PowerCurveService.compute_bin_stats(df[df.year == 2022])
        s23 = PowerCurveService.compute_bin_stats(df[df.year == 2023])
        merged = s22.merge(s23, on="wind_bin_left", suffixes=("_22", "_23"))
        assert len(merged) >= 8
        assert (merged["q50_pu_22"] - merged["q50_pu_23"]).abs().max() < 0.03

    def test_normalisation_index_is_flat(self, farm):
        df = _in_force(farm)
        lookup = _curve_lookup(PowerCurveService.compute_bin_stats(df))
        hourly = WindNormalisationService.compute_hourly_ratios(df, lookup, NAMEPLATE)
        by_year = hourly.groupby("year")["norm_ratio"].mean()
        # Mean of actual/median is not exactly 1 (noise tails in low bins), but
        # the two phases must agree: same farm, same efficiency.
        assert by_year[2022] == pytest.approx(by_year[2023], abs=0.02)
        monthly, yearly = WindNormalisationService.compute_indices(hourly)
        assert (monthly["index_vs_base"].sub(100).abs() < 5).all()
        assert (yearly["index_vs_base"].sub(100).abs() < 2).all()

    def test_odi_sees_no_lost_energy_and_expected_scales_with_capacity(self, farm):
        df = _in_force(farm)
        for year in (2022, 2023):
            dfy = df[df.year == year]
            stats = PowerCurveService.compute_bin_stats(dfy)
            flagged = PerformanceAnomalyService.classify_hours(dfy, stats, NAMEPLATE, None)
            lost = flagged["lost_mwh"].sum()
            assert lost < 0.01 * dfy["generation_mwh"].sum()
            # expected_mwh is on the same MW scale as that year's actual output
            ok = flagged["expected_mwh"].notna()
            ratio = flagged.loc[ok, "expected_mwh"].sum() / flagged.loc[ok, "generation_mwh"].sum()
            assert ratio == pytest.approx(1.0, abs=0.05)

    def test_degradation_slope_is_flat(self, farm):
        df = _in_force(farm)
        curves = {
            y: _curve_lookup(PowerCurveService.compute_bin_stats(df[df.year == y]))
            for y in (2022, 2023)
        }
        residuals = DegradationService.compute_residuals(df, curves)
        fit = DegradationService.fit_degradation_trend(residuals)
        assert fit is not None
        assert abs(fit["slope_pct"]) < 1.0

    def test_constraint_proxy_scales_with_capacity_in_force(self, farm):
        df = _in_force(farm)
        df["wind_bin"] = np.floor(df["wind_speed"])
        df["effective_price"] = 50.0
        gap = {b: 0.1 for b in range(2, 26)}
        out = PerformancePipelineService.compute_constraint_proxy(df, gap, NAMEPLATE)
        in_band = df[df["wind_bin"].between(2, 25)]
        expected = 0.1 * in_band["capacity_mw"].sum()
        assert out["constraint_proxy_mwh"] == pytest.approx(expected)
        assert out["lost_value_eur"] == pytest.approx(expected * 50.0)
        assert out["proxy_hours"] == len(in_band)


class TestConstantNameplateShowsTheBias:
    """The old basis must FAIL the same checks — proves the tests discriminate."""

    def test_curve_differs_across_phases(self, farm):
        df = _constant(farm)
        s22 = PowerCurveService.compute_bin_stats(df[df.year == 2022])
        s23 = PowerCurveService.compute_bin_stats(df[df.year == 2023])
        merged = s22.merge(s23, on="wind_bin_left", suffixes=("_22", "_23"))
        assert (merged["q50_pu_22"] - merged["q50_pu_23"]).abs().max() > 0.2

    def test_normalisation_index_steps(self, farm):
        df = _constant(farm)
        lookup = _curve_lookup(PowerCurveService.compute_bin_stats(df))
        hourly = WindNormalisationService.compute_hourly_ratios(df, lookup, NAMEPLATE)
        by_year = hourly.groupby("year")["norm_ratio"].mean()
        assert by_year[2022] < 0.75 < by_year[2023]

    def test_degradation_slope_is_spurious(self, farm):
        df = _constant(farm)
        pooled = _curve_lookup(PowerCurveService.compute_bin_stats(df))
        residuals = DegradationService.compute_residuals(df, {2022: pooled, 2023: pooled})
        fit = DegradationService.fit_degradation_trend(residuals)
        assert fit is not None
        assert fit["slope_pct"] > 10.0
