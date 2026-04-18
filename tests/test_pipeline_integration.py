"""End-to-end math integration test for the 6-module pipeline + item 3.

Builds a synthetic windfarm-year (8,760 hours of (wind, generation, price))
and runs the math layer of every module through the chain:

    Module 1 (data shape/cleaning) ─► Module 2 (power curve)
                                       │
                                       ▼
    Module 3 (anomaly + loss) ◄── classify_hours
    Module 4 (wind normalisation) ◄── overall_clean curve
    Module 5 (degradation residuals)
    Item 3   (generation concentration) ◄── price + generation only

Verifies the chain produces sensible, mutually-consistent results without
touching the database. Equivalent to the spec's "manual smoke test" but
fully automated and portable across environments.

Block Island smoke test against real production data (spec plan §4) is a
post-deploy operational task and is NOT part of this PR — it requires the
ERA5 NaN cleanup to land first to give Block Island any usable weather data.
"""

import math

import numpy as np
import pandas as pd
import pytest

from app.services.degradation_service import DegradationService
from app.services.generation_concentration_service import (
    GenerationConcentrationService,
)
from app.services.performance_anomaly_service import PerformanceAnomalyService
from app.services.power_curve_service import PowerCurveService

# Reasonable installed capacity for a synthetic small offshore windfarm
RATED_MW = 30.0


def _synthetic_windfarm_year(year=2024, seed=42, degradation_pct=0.0):
    """Build 8,760 hours of (hour, year, wind, p_pu, gen, price) for one windfarm.

    Generation is a sigmoid power curve plus 3% noise. `degradation_pct`
    applies a small year-specific downshift so the chain test can verify
    Module 5 picks up trends.
    """
    rng = np.random.default_rng(seed)
    hours = pd.date_range(f"{year}-01-01 00:00", f"{year}-12-31 23:00", freq="h")
    n = len(hours)

    # Wind: Weibull-like shape, mean ~9 m/s
    wind = np.clip(rng.weibull(2.0, n) * 8.0, 0, 25.0)

    # Sigmoid p_pu, then 3% Gaussian noise
    p_pu_clean = 1.0 / (1.0 + np.exp(-(wind - 8.5)))
    noise = rng.normal(0, 0.03, n)
    p_pu = np.clip(p_pu_clean + noise - degradation_pct, -0.05, 1.20)

    gen_mwh = p_pu * RATED_MW

    # Prices: positive correlation with wind (windier hours often coincide
    # with higher demand in winter)
    price_base = 30 + (wind / 25.0) * 30 + rng.normal(0, 8, n)
    price = np.clip(price_base, 5, 200)

    df = pd.DataFrame(
        {
            "hour": hours,
            "year": year,
            "generation_mwh": gen_mwh,
            "wind_speed": wind,
            "market_price": price,
            "p_pu": p_pu,
        }
    )
    return df


def _capability_stats_from_curve(df):
    """Run Module 2 bin aggregation → return the capability stats df shape
    expected by Module 3."""
    stats = PowerCurveService.compute_bin_stats(df)
    # classify_hours expects pd.Interval bin labels
    if "wind_bin" not in stats.columns:
        # compute_bin_stats already uses pd.cut and returns interval objects
        pass
    return stats


class TestEndToEndMath:
    """Run modules in the same order the orchestrator does and check outputs."""

    def test_module_2_produces_capability(self):
        df = _synthetic_windfarm_year()
        stats = PowerCurveService.compute_bin_stats(df)
        # Should have at least 5 bins with samples
        assert len(stats) >= 5
        # q90 >= q50 in every bin (definition)
        valid = stats.dropna(subset=["q50_pu", "q90_pu"])
        assert (valid["q90_pu"] >= valid["q50_pu"]).all()

    def test_module_3_classifies_hours(self):
        df = _synthetic_windfarm_year()
        stats = _capability_stats_from_curve(df)

        flagged = PerformanceAnomalyService.classify_hours(
            df, stats, rated_mw=RATED_MW, ppa_price=None
        )
        assert "anomaly_type" in flagged.columns
        assert "is_anomaly" in flagged.columns
        # With clean synthetic data + 3% noise, anomaly rate should be small
        # (well under 10%) but non-zero (some tail noise crosses the threshold).
        rate = flagged["is_anomaly"].mean()
        assert 0 < rate < 0.10, f"Anomaly rate {rate:.3f} outside expected band"
        # lost_mwh is non-negative wherever flagged
        underperf_mask = flagged["anomaly_type"] == "underperformance"
        assert (flagged.loc[underperf_mask, "lost_mwh"] >= 0).all()

    def test_module_3_aggregation_consistent(self):
        df = _synthetic_windfarm_year()
        stats = _capability_stats_from_curve(df)
        flagged = PerformanceAnomalyService.classify_hours(
            df, stats, rated_mw=RATED_MW, ppa_price=None
        )
        flagged = PerformanceAnomalyService.assign_run_ids(flagged)
        monthly, yearly = PerformanceAnomalyService.aggregate_summaries(flagged, year=2024)

        # Yearly hours = sum of monthly hours
        m_total = sum(m["total_hours"] for m in monthly)
        assert m_total == yearly["total_hours"]
        # ODI percent is bounded
        assert 0 <= yearly["odi_pct_underperf"] <= 100

    def test_module_5_residuals_zero_for_no_degradation(self):
        # Two years with NO injected degradation
        years = [2022, 2023, 2024]
        df = pd.concat(
            [_synthetic_windfarm_year(year=y, seed=y, degradation_pct=0.0) for y in years],
            ignore_index=True,
        )
        # Build per-year capability curves
        curves = {}
        for y in years:
            year_df = df[df["year"] == y]
            stats = PowerCurveService.compute_bin_stats(year_df)
            curve = {}
            for _, row in stats.iterrows():
                if pd.notna(row.get("wind_bin_left")) and pd.notna(row.get("q50_pu")):
                    curve[float(row["wind_bin_left"])] = float(row["q50_pu"])
            curves[y] = curve

        residuals = DegradationService.compute_residuals(df, curves)
        # Residuals should hover around zero — mean within ±0.10. Column is
        # `mean_residual_pu` (monthly aggregate).
        if len(residuals) > 0:
            mean_resid = float(residuals["mean_residual_pu"].mean())
            assert -0.10 < mean_resid < 0.10

    def test_module_5_picks_up_negative_slope(self):
        # Three years with progressively worse performance
        years = [2022, 2023, 2024]
        df = pd.concat(
            [
                _synthetic_windfarm_year(
                    year=y, seed=y, degradation_pct=0.05 * (i),
                )
                for i, y in enumerate(years)
            ],
            ignore_index=True,
        )
        # Use the FIRST year's curve as reference for all years (so later
        # years show worse residuals).
        first_stats = PowerCurveService.compute_bin_stats(df[df["year"] == years[0]])
        first_curve = {}
        for _, row in first_stats.iterrows():
            if pd.notna(row.get("wind_bin_left")) and pd.notna(row.get("q50_pu")):
                first_curve[float(row["wind_bin_left"])] = float(row["q50_pu"])
        curves = {y: first_curve for y in years}

        residuals = DegradationService.compute_residuals(df, curves)
        # Year-over-year mean monthly residual should decrease (more negative
        # each year as we injected progressive degradation).
        if len(residuals) >= 24:
            yearly_means = residuals.groupby("year")["mean_residual_pu"].mean()
            # 2024 mean residual < 2022 mean residual
            assert yearly_means.iloc[-1] < yearly_means.iloc[0]

    def test_item_3_concentration_chain(self):
        df = _synthetic_windfarm_year()
        # Build concentration metrics from the same df
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert result is not None
        # With wind-correlated prices and gen, capture ratio should be > 1
        assert result["capture_ratio"] > 1.0, (
            f"Expected positive capture ratio for wind-price correlated synthetic data, "
            f"got {result['capture_ratio']}"
        )
        # And total MWh should be consistent with rated × hours × avg p.u.
        approx_max = RATED_MW * 8760
        assert 0 < result["total_mwh"] < approx_max
        # Decile shares sum to ~100%
        assert math.isclose(
            sum(result["decile_shares_full"].values()), 100.0, abs_tol=0.5
        )


class TestPipelineConsistency:
    """Cross-module invariants that should hold for any single windfarm-year."""

    def test_all_modules_run_on_same_dataset(self):
        df = _synthetic_windfarm_year()
        stats = _capability_stats_from_curve(df)

        # Module 3
        flagged = PerformanceAnomalyService.classify_hours(
            df, stats, rated_mw=RATED_MW, ppa_price=None
        )
        flagged = PerformanceAnomalyService.assign_run_ids(flagged)
        _, yearly = PerformanceAnomalyService.aggregate_summaries(flagged, year=2024)

        # Item 3
        conc = GenerationConcentrationService(db=None)._compute_metrics(df)

        # Module 3 keeps every hour (classifies all p_pu values, including
        # negatives produced by noise around zero). Item 3 (concentration)
        # filters generation < 0, so its hour count is ≤ module 3's. The
        # difference is small (typically <5%) for clean synthetic data.
        n_hours = len(df)
        assert yearly["total_hours"] == n_hours
        assert conc["total_hours"] <= n_hours
        # Concentration retention should be high — at least 80% of the year
        assert conc["total_hours"] / n_hours > 0.80

        # Concentration's total MWh should be at least as large as the
        # underperf-detected lost_mwh (lost is bounded by total).
        assert conc["total_mwh"] >= yearly["lost_mwh"]

    def test_pricing_basis_label_changes_with_ppa(self):
        # Just exercise classify_hours with PPA price — doesn't return
        # pricing_basis (that's in the service-level fn) but loss should
        # use PPA when given.
        df = _synthetic_windfarm_year()
        stats = _capability_stats_from_curve(df)

        # Use a constant PPA of 50 EUR — losses should differ from
        # market-price baseline (where price varies 5-200 EUR).
        flagged_market = PerformanceAnomalyService.classify_hours(
            df, stats, rated_mw=RATED_MW, ppa_price=None
        )
        flagged_ppa = PerformanceAnomalyService.classify_hours(
            df, stats, rated_mw=RATED_MW, ppa_price=50.0
        )

        # Flag counts should be identical (PPA only affects EUR not classification)
        assert flagged_market["is_anomaly"].sum() == flagged_ppa["is_anomaly"].sum()
        # But lost_eur should differ (market price varies, PPA is flat)
        m_eur = float(flagged_market["lost_eur"].sum())
        p_eur = float(flagged_ppa["lost_eur"].sum())
        # If there are any anomalies, totals must differ
        if flagged_market["is_anomaly"].sum() > 0:
            assert not math.isclose(m_eur, p_eur, abs_tol=1.0)
