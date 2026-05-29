"""Unit tests for degradation service — pure pandas/numpy, no database."""

import numpy as np
import pandas as pd

from app.services.degradation_service import (
    OP_WIND_MAX,
    OP_WIND_MIN,
    DegradationService,
    remove_seasonal_component,
)

# ─── Test fixtures ───────────────────────────────────────────────


def _make_yearly_curves(years, slope=0.0):
    """Create mock yearly capability curves with optional degradation."""
    curves = {}
    for i, year in enumerate(years):
        degradation_offset = slope * i
        curve = {}
        for wbin in range(4, 15):
            mid = wbin + 0.5
            q50 = 1 / (1 + np.exp(-(mid - 8))) + degradation_offset
            curve[float(wbin)] = max(q50, 0.0)
        curves[year] = curve
    return curves


def _make_hourly_df(years, n_per_year=500, seed=42, degradation_rate=0.0):
    """Create synthetic hourly data with optional degradation trend.

    Small fixture: n_per_year hours starting Jan 1 each year.
    """
    rng = np.random.RandomState(seed)
    rows = []
    for i, year in enumerate(years):
        wind = rng.uniform(4, 14, size=n_per_year)
        base_p_pu = 1 / (1 + np.exp(-(wind - 8)))
        noise = rng.normal(0, 0.03, n_per_year)
        degradation = degradation_rate * i
        p_pu = np.clip(base_p_pu + noise + degradation, 0, 1.0)

        hours = pd.date_range(f"{year}-01-01", periods=n_per_year, freq="h")
        for j in range(n_per_year):
            rows.append(
                {
                    "hour": hours[j],
                    "year": year,
                    "generation_mwh": p_pu[j] * 100,
                    "wind_speed": wind[j],
                    "market_price": 30.0,
                    "p_pu": p_pu[j],
                }
            )
    return pd.DataFrame(rows)


def _make_full_year_df(
    start: str,
    end: str,
    *,
    seed: int = 42,
    slope_pu_per_year: float = 0.0,
    seasonal_amplitude: float = 0.0,
    noise_sigma: float = 0.02,
) -> pd.DataFrame:
    """Create full hourly synthetic data with controlled slope + optional seasonality.

    - Wind uniform in [6, 14] m/s; baseline p_pu follows logistic curve centred at 8.
      The 6-14 range keeps every wind bin's reference above the 0.10 operational
      cut-off, so compute_residuals returns one row per hour with no gaps. That
      keeps the positional period=8760 cycle aligned with the calendar — gaps
      would stretch the cycle and the deseasonalisation would no longer recover
      the true slope (a known spec limitation, see plan Math reference).
    - slope_pu_per_year applied to year_fraction
    - seasonal_amplitude × sin(2π·dayofyear/365) added to p_pu
    - noise N(0, noise_sigma) added per hour
    """
    rng = np.random.RandomState(seed)
    hours = pd.date_range(start=start, end=end, freq="h", inclusive="left")
    n = len(hours)
    wind = rng.uniform(6, 14, size=n)
    base_p_pu = 1 / (1 + np.exp(-(wind - 8)))

    year_arr = hours.year.to_numpy()
    doy_arr = hours.dayofyear.to_numpy()
    year_fraction = year_arr + (doy_arr - 1) / 365.25

    t0 = year_fraction[0]
    trend = slope_pu_per_year * (year_fraction - t0)
    seasonal = seasonal_amplitude * np.sin(2 * np.pi * doy_arr / 365.0)
    noise = rng.normal(0, noise_sigma, n)

    p_pu = np.clip(base_p_pu + trend + seasonal + noise, 0, 1.0)

    return pd.DataFrame(
        {
            "hour": hours,
            "year": year_arr.astype(int),
            "generation_mwh": p_pu * 100.0,
            "wind_speed": wind,
            "market_price": 30.0,
            "p_pu": p_pu,
        }
    )


# ─── compute_residuals ──────────────────────────────────────────


class TestComputeResiduals:
    """Test residual computation."""

    def test_returns_hourly_rows(self):
        years = [2020, 2021]
        df = _make_hourly_df(years, n_per_year=1000)
        curves = _make_yearly_curves(years)

        residuals = DegradationService.compute_residuals(df, curves)
        # Returns per-hour rows now, not monthly aggregates
        assert len(residuals) > 0
        assert "hour" in residuals.columns
        assert "year" in residuals.columns
        assert "year_fraction" in residuals.columns
        assert "residual_pu" in residuals.columns
        assert "ref_pu" in residuals.columns
        # Should have many rows (≈ n_per_year × years), not 12-24 monthly buckets
        assert len(residuals) > 100

    def test_year_fraction_uses_dayofyear(self):
        """year_fraction = year + (dayofyear - 1) / 365.25 per spec :1001."""
        years = [2020]
        df = _make_hourly_df(years, n_per_year=200)
        curves = _make_yearly_curves(years)

        residuals = DegradationService.compute_residuals(df, curves)
        # First hour of year → dayofyear=1 → fraction=year + 0
        first = residuals.sort_values("hour").iloc[0]
        assert abs(first["year_fraction"] - 2020.0) < 1e-9

    def test_filters_operational_wind_range(self):
        years = [2020, 2021]
        df = _make_hourly_df(years, n_per_year=500)
        # Out-of-range hours
        df.loc[0, "wind_speed"] = 2.0
        df.loc[1, "wind_speed"] = 20.0
        curves = _make_yearly_curves(years)

        residuals = DegradationService.compute_residuals(df, curves)
        # Filtered rows shouldn't appear
        assert (residuals["wind_speed"] >= OP_WIND_MIN).all()
        assert (residuals["wind_speed"] <= OP_WIND_MAX).all()

    def test_residuals_near_zero_for_clean_data(self):
        """With no degradation, residual mean should be near zero."""
        years = [2020, 2021, 2022]
        df = _make_hourly_df(years, n_per_year=2000, degradation_rate=0.0)
        curves = _make_yearly_curves(years, slope=0.0)

        residuals = DegradationService.compute_residuals(df, curves)
        assert abs(residuals["residual_pu"].mean()) < 0.05

    def test_returns_empty_if_no_matching_curves(self):
        years = [2020]
        df = _make_hourly_df(years)
        curves = {2025: {5.0: 0.5}}  # Wrong year

        residuals = DegradationService.compute_residuals(df, curves)
        assert residuals.empty


class TestQ50OperationalFloor:
    """Issue #80 — the operational floor must always be q50, even for the q90 fit.

    The spec filters the operational subset on ``q50_bin >= 0.10`` for BOTH
    references (``energyexe_pipeline_full.py:998``). A low-wind bin whose q50 is
    below the floor but whose q90 clears it must be EXCLUDED from the q90 fit.
    """

    @staticmethod
    def _two_bin_df(seed=1):
        """Hours split between a low-wind bin (4) and a mid bin (8), one year."""
        rng = np.random.RandomState(seed)
        n = 200
        wind = np.concatenate([np.full(n, 4.5), np.full(n, 8.5)])  # bins 4 and 8
        p_pu = np.concatenate([rng.uniform(0.0, 0.1, n), rng.uniform(0.4, 0.6, n)])
        hours = pd.date_range("2020-01-01", periods=len(wind), freq="h")
        return pd.DataFrame(
            {
                "hour": hours,
                "year": 2020,
                "generation_mwh": p_pu * 100,
                "wind_speed": wind,
                "market_price": 30.0,
                "p_pu": p_pu,
            }
        )

    # q50 below floor in bin 4; q90 above floor in bin 4. Bin 8 clears both.
    Q50_CURVES = {2020: {4.0: 0.05, 8.0: 0.50}}
    Q90_CURVES = {2020: {4.0: 0.15, 8.0: 0.70}}

    def test_q90_fit_uses_q50_floor_excludes_low_wind_bin(self):
        df = self._two_bin_df()
        residuals = DegradationService.compute_residuals(
            df, self.Q90_CURVES, floor_curves=self.Q50_CURVES
        )
        # Bin 4's q50 (0.05) is below the 0.10 floor → excluded despite q90=0.15.
        assert set(residuals["wind_bin"].unique()) == {8.0}
        # Residual is still taken against the ACTIVE (q90) curve for bin 8.
        assert residuals["ref_pu"].unique().tolist() == [0.70]

    def test_q90_without_floor_curve_would_admit_low_wind_bin(self):
        """Guard: with no floor curve the q90 curve floors itself (the old bug)."""
        df = self._two_bin_df()
        residuals = DegradationService.compute_residuals(df, self.Q90_CURVES)
        # q90 bin 4 (0.15) clears 0.10 → bin 4 leaks in. This is exactly the
        # behaviour #80 fixes; the orchestrator must pass floor_curves=q50.
        assert 4.0 in set(residuals["wind_bin"].unique())

    def test_q50_fit_unchanged_floor_equals_active(self):
        df = self._two_bin_df()
        residuals = DegradationService.compute_residuals(df, self.Q50_CURVES)
        # q50 bin 4 (0.05) below floor → excluded; only bin 8 survives.
        assert set(residuals["wind_bin"].unique()) == {8.0}
        assert residuals["ref_pu"].unique().tolist() == [0.50]


# ─── fit_degradation_trend ─────────────────────────────────────


class TestFitDegradationTrend:
    """Test OLS trend fitting on hourly residuals."""

    @staticmethod
    def _build_synthetic_residual_df(year_fracs, residuals):
        """Helper: build hourly-shape DF with required columns."""
        return pd.DataFrame(
            {
                "hour": pd.date_range("2020-01-01", periods=len(year_fracs), freq="D"),
                "year": [int(y) for y in year_fracs],
                "year_fraction": year_fracs,
                "p_pu": np.full(len(year_fracs), 0.4),
                "ref_pu": np.full(len(year_fracs), 0.4),
                "wind_bin": np.full(len(year_fracs), 7.0),
                "residual_pu": residuals,
            }
        )

    def test_detects_degradation(self):
        """Synthetic data with -0.01 p.u./year slope should be detected."""
        rng = np.random.RandomState(42)
        n = 200  # < 2 × period(8760) → seasonal decompose skipped
        year_fracs = np.linspace(2020, 2024, n)
        residuals = -0.01 * (year_fracs - 2020) + rng.normal(0, 0.005, n)

        df = self._build_synthetic_residual_df(year_fracs, residuals)
        trend = DegradationService.fit_degradation_trend(df)
        assert trend is not None
        assert trend["slope"] < 0
        assert abs(trend["slope"] - (-0.01)) < 0.005

    def test_detects_no_trend(self):
        rng = np.random.RandomState(42)
        n = 200
        year_fracs = np.linspace(2020, 2023, n)
        residuals = rng.normal(0, 0.01, n)

        df = self._build_synthetic_residual_df(year_fracs, residuals)
        trend = DegradationService.fit_degradation_trend(df)
        assert trend is not None
        assert abs(trend["slope"]) < 0.01

    def test_confidence_interval(self):
        rng = np.random.RandomState(42)
        n = 200
        year_fracs = np.linspace(2020, 2022, n)
        residuals = -0.005 * (year_fracs - 2020) + rng.normal(0, 0.01, n)

        df = self._build_synthetic_residual_df(year_fracs, residuals)
        trend = DegradationService.fit_degradation_trend(df)
        assert trend["ci95"] is not None
        assert trend["ci95"][0] < trend["slope"] < trend["ci95"][1]

    def test_returns_empty_summary_for_insufficient_data(self):
        """Below 100 hours after filtering → returns None or empty summary."""
        df = self._build_synthetic_residual_df([2024.0], [0.0])
        trend = DegradationService.fit_degradation_trend(df)
        # n < 100 → empty / None
        assert trend is None or trend.get("n", 0) == 0

    def test_r_squared_in_range(self):
        rng = np.random.RandomState(42)
        n = 300
        year_fracs = np.linspace(2020, 2023, n)
        residuals = -0.01 * (year_fracs - 2020) + rng.normal(0, 0.002, n)

        df = self._build_synthetic_residual_df(year_fracs, residuals)
        trend = DegradationService.fit_degradation_trend(df)
        assert 0 <= trend["r2"] <= 1


# ─── A1 golden tests — full hourly pipeline ────────────────────


class TestA1Golden:
    """A1.T1-A1.T3: synthetic full-hourly tests per the Milestone A plan.

    Each test creates 3+ years of hourly data with a controlled slope and
    runs the full compute_residuals → fit_degradation_trend pipeline.
    Pass criteria are spelled out in the plan's Milestone A section.
    """

    def test_a1_t1_clean_slope_recovery(self):
        """A1.T1: 3yr × 8760h, slope = -0.005 p.u./yr, no seasonality, σ=0.02.

        Pass: |slope - (-0.005)| < 0.0005; CI95 includes -0.005.
        """
        df = _make_full_year_df(
            start="2020-01-01",
            end="2023-01-01",
            seed=7,
            slope_pu_per_year=-0.005,
            seasonal_amplitude=0.0,
            noise_sigma=0.02,
        )
        years = sorted(df["year"].unique().tolist())
        curves = _make_yearly_curves(years, slope=0.0)

        residuals = DegradationService.compute_residuals(df, curves)
        # Should be roughly 2-3 years × (curves are 4-14 m/s) — ≥10k hours
        assert len(residuals) > 10_000

        trend = DegradationService.fit_degradation_trend(residuals)
        assert trend is not None
        slope = trend["slope"]
        assert abs(slope - (-0.005)) < 0.0005, f"slope={slope} expected ≈ -0.005"

        ci = trend["ci95"]
        assert ci is not None
        assert ci[0] <= -0.005 <= ci[1], f"CI95 {ci} does not contain truth -0.005"

    def test_a1_t2_seasonal_slope_recovery(self):
        """A1.T2: 3yr × 8760h, slope -0.005 + annual seasonal swing, σ=0.02.

        Without deseasonalisation, slope estimate is biased.
        After deseasonalising, slope recovers within ±0.0005.
        """
        df = _make_full_year_df(
            start="2020-01-01",
            end="2023-01-01",
            seed=11,
            slope_pu_per_year=-0.005,
            seasonal_amplitude=0.05,
            noise_sigma=0.02,
        )
        years = sorted(df["year"].unique().tolist())
        curves = _make_yearly_curves(years, slope=0.0)

        residuals = DegradationService.compute_residuals(df, curves)
        trend = DegradationService.fit_degradation_trend(residuals)
        assert trend is not None
        slope = trend["slope"]
        # Deseasonalised — slope should land within ~4× the OLS standard error
        # of truth. With σ=0.02 noise + tiny residual seasonal over 26k hours
        # that's roughly ±0.001.
        assert (
            abs(slope - (-0.005)) < 0.001
        ), f"slope={slope} expected ≈ -0.005 after deseasonalisation"

    def test_a1_t3_uneven_boundaries_zero_slope(self):
        """A1.T3: Aug 2020 → May 2024, seasonal cycle, true slope = 0.

        Without deseasonalisation, partial-year boundaries bias the slope
        (more winter than summer in the data or vice versa).
        After deseasonalising, slope ≈ 0 within ±0.001.
        """
        df = _make_full_year_df(
            start="2020-08-01",
            end="2024-05-01",
            seed=23,
            slope_pu_per_year=0.0,
            seasonal_amplitude=0.05,
            noise_sigma=0.02,
        )
        years = sorted(df["year"].unique().tolist())
        curves = _make_yearly_curves(years, slope=0.0)

        residuals = DegradationService.compute_residuals(df, curves)
        trend = DegradationService.fit_degradation_trend(residuals)
        assert trend is not None
        slope = trend["slope"]
        assert abs(slope) < 0.001, f"slope={slope} expected ≈ 0 after deseasonalisation"


# ─── A2 — baseline_cap_pu computed from data ───────────────────


class TestA2Baseline:
    """A2: baseline_cap_pu = hours-weighted median of ref_pu in first year."""

    @staticmethod
    def _curve_with_median(median_value):
        """Build a yearly capability curve whose hours-weighted median lands
        at ``median_value``. Uses uniform wind so every bin gets ~equal hours.
        Each bin's ref_pu = median_value (constant) so the median is exact.
        """
        curves = {}
        for year in (2020, 2021, 2022):
            curve = {float(wbin): float(median_value) for wbin in range(6, 15)}
            curves[year] = curve
        return curves

    def _build_with_curve(self, median_value: float):
        df = _make_full_year_df(
            start="2020-01-01",
            end="2023-01-01",
            seed=31,
            slope_pu_per_year=0.0,
            seasonal_amplitude=0.0,
            noise_sigma=0.02,
        )
        curves = self._curve_with_median(median_value)
        residuals = DegradationService.compute_residuals(df, curves)
        return DegradationService.fit_degradation_trend(residuals)

    def test_a2_t1_low_baseline(self):
        """Curve with median 0.20 → baseline_cap_pu ≈ 0.20."""
        trend = self._build_with_curve(0.20)
        assert trend is not None
        assert abs(trend["baseline_cap_pu"] - 0.20) < 0.005

    def test_a2_t2_high_baseline(self):
        """Curve with median 0.50 → baseline_cap_pu ≈ 0.50."""
        trend = self._build_with_curve(0.50)
        assert trend is not None
        assert abs(trend["baseline_cap_pu"] - 0.50) < 0.005

    def test_a2_t3_invalid_baseline_skips(self):
        """If the first-year baseline is invalid, skip (return None).

        Issue #80: the old code fell back to a hardcoded 0.35, silently
        resurrecting Bug-C. With no valid first-year capability we cannot
        express slope as a % of capability, so the fit is skipped instead.
        """
        rng = np.random.RandomState(42)
        n = 500
        df = pd.DataFrame(
            {
                "hour": pd.date_range("2020-01-01", periods=n, freq="h"),
                "year": np.full(n, 2020),
                "year_fraction": 2020 + np.linspace(0, 1, n),
                "wind_speed": rng.uniform(7, 13, n),
                "wind_bin": np.full(n, 8.0),
                # ref_pu NaN forces the baseline median to NaN
                "ref_pu": np.full(n, np.nan),
                "p_pu": rng.uniform(0.2, 0.5, n),
                "residual_pu": rng.normal(0, 0.02, n),
            }
        )
        trend = DegradationService.fit_degradation_trend(df)
        assert trend is None

    def test_a2_t4_slope_pct_matches_slope_divided_by_baseline(self):
        """slope_pct = slope_pu / baseline_cap_pu × 100 (spec :1053)."""
        trend = self._build_with_curve(0.40)
        assert trend is not None
        expected_pct = trend["slope"] / trend["baseline_cap_pu"] * 100
        assert abs(trend["slope_pct"] - expected_pct) < 1e-9


# ─── A3 — CI95_pct surfaced in trend dict ──────────────────────


class TestA3CIPct:
    """A3: ci95_pct populated as (ci_lower/baseline×100, ci_upper/baseline×100)."""

    def test_a3_t1_ci_pct_matches_formula(self):
        df = _make_full_year_df(
            start="2020-01-01",
            end="2023-01-01",
            seed=41,
            slope_pu_per_year=-0.005,
            seasonal_amplitude=0.0,
            noise_sigma=0.02,
        )
        curves = TestA2Baseline._curve_with_median(0.35)
        residuals = DegradationService.compute_residuals(df, curves)
        trend = DegradationService.fit_degradation_trend(residuals)
        assert trend is not None
        assert trend["ci95"] is not None
        assert trend["ci95_pct"] is not None
        lo_pu, hi_pu = trend["ci95"]
        lo_pct, hi_pct = trend["ci95_pct"]
        baseline = trend["baseline_cap_pu"]
        assert abs(lo_pct - lo_pu / baseline * 100) < 1e-9
        assert abs(hi_pct - hi_pu / baseline * 100) < 1e-9

    def test_a3_t2_ci_pct_none_when_ci_none(self):
        """If n < 3, ci95 is None → ci95_pct also None."""
        # Build a 2-row residual DF; OLS technically works but std_err 0 → no CI
        df = pd.DataFrame(
            {
                "hour": pd.date_range("2020-01-01", periods=200, freq="h"),
                "year": np.full(200, 2020),
                "year_fraction": np.full(200, 2020.0),  # zero variance → ssx=0 → None
                "wind_speed": np.full(200, 8.0),
                "wind_bin": np.full(200, 8.0),
                "ref_pu": np.full(200, 0.35),
                "p_pu": np.full(200, 0.35),
                "residual_pu": np.full(200, 0.0),
            }
        )
        trend = DegradationService.fit_degradation_trend(df)
        # ssx == 0 → returns None entirely per implementation
        assert trend is None


# ─── remove_seasonal_component helper ──────────────────────────


class TestRemoveSeasonalComponent:
    """Test the deseasonalisation helper directly."""

    def test_short_series_returned_unchanged(self):
        """len < 2 × period → return as-is."""
        n = 1000
        s = pd.Series(np.random.randn(n))
        out = remove_seasonal_component(s, period=8760)
        # No mutation possible — period > len
        pd.testing.assert_series_equal(s, out)

    def test_long_series_removes_seasonal(self):
        """Series with seasonal cycle + trend → output should have flatter seasonal."""
        rng = np.random.RandomState(42)
        n = 2 * 8760 + 100
        t = np.arange(n)
        seasonal = 0.5 * np.sin(2 * np.pi * t / 8760)
        trend = 0.001 * t / 8760
        noise = rng.normal(0, 0.05, n)
        s = pd.Series(seasonal + trend + noise)

        out = remove_seasonal_component(s, period=8760)
        # Std of seasonal swing should drop substantially
        assert out.std() < s.std()
