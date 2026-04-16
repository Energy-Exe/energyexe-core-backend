"""Unit tests for degradation service — pure pandas/numpy, no database."""

import numpy as np
import pandas as pd
import pytest

from app.services.degradation_service import (
    DegradationService,
    OP_WIND_MAX,
    OP_WIND_MIN,
)


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
    """Create synthetic hourly data with optional degradation trend."""
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
            rows.append({
                "hour": hours[j],
                "year": year,
                "generation_mwh": p_pu[j] * 100,
                "wind_speed": wind[j],
                "market_price": 30.0,
                "p_pu": p_pu[j],
            })
    return pd.DataFrame(rows)


class TestComputeResiduals:
    """Test residual computation."""

    def test_produces_monthly_residuals(self):
        years = [2020, 2021, 2022, 2023]
        df = _make_hourly_df(years, n_per_year=1000)
        curves = _make_yearly_curves(years)

        residuals = DegradationService.compute_residuals(df, curves)
        assert len(residuals) > 0
        assert "year_fraction" in residuals.columns
        assert "mean_residual_pu" in residuals.columns

    def test_filters_operational_wind_range(self):
        years = [2020, 2021]
        df = _make_hourly_df(years, n_per_year=500)
        # Add some hours outside operational range
        df.loc[0, "wind_speed"] = 2.0  # Below OP_WIND_MIN
        df.loc[1, "wind_speed"] = 20.0  # Above OP_WIND_MAX
        curves = _make_yearly_curves(years)

        residuals = DegradationService.compute_residuals(df, curves)
        # Result should not include data from out-of-range hours
        assert len(residuals) > 0

    def test_residuals_near_zero_for_clean_data(self):
        """With no degradation, mean residuals should be near zero."""
        years = [2020, 2021, 2022]
        df = _make_hourly_df(years, n_per_year=2000, degradation_rate=0.0)
        curves = _make_yearly_curves(years, slope=0.0)

        residuals = DegradationService.compute_residuals(df, curves)
        assert abs(residuals["mean_residual_pu"].mean()) < 0.05

    def test_returns_empty_if_no_matching_curves(self):
        years = [2020]
        df = _make_hourly_df(years)
        curves = {2025: {5.0: 0.5}}  # Wrong year

        residuals = DegradationService.compute_residuals(df, curves)
        assert residuals.empty


class TestFitDegradationTrend:
    """Test OLS trend fitting."""

    def test_detects_degradation(self):
        """Synthetic data with -0.01 p.u./year slope should be detected."""
        rng = np.random.RandomState(42)
        months = 48
        year_fracs = np.linspace(2020, 2024, months)
        residuals = -0.01 * (year_fracs - 2020) + rng.normal(0, 0.005, months)

        df = pd.DataFrame({
            "year": [int(y) for y in year_fracs],
            "month": [(i % 12) + 1 for i in range(months)],
            "year_fraction": year_fracs,
            "mean_residual_pu": residuals,
        })

        trend = DegradationService.fit_degradation_trend(df)
        assert trend is not None
        assert trend["slope"] < 0  # Negative = degradation
        assert abs(trend["slope"] - (-0.01)) < 0.005  # Close to true slope

    def test_detects_no_trend(self):
        """Flat data should have slope near zero."""
        rng = np.random.RandomState(42)
        months = 36
        year_fracs = np.linspace(2020, 2023, months)
        residuals = rng.normal(0, 0.01, months)

        df = pd.DataFrame({
            "year": [2020 + i // 12 for i in range(months)],
            "month": [(i % 12) + 1 for i in range(months)],
            "year_fraction": year_fracs,
            "mean_residual_pu": residuals,
        })

        trend = DegradationService.fit_degradation_trend(df)
        assert trend is not None
        assert abs(trend["slope"]) < 0.01  # Near zero

    def test_confidence_interval(self):
        """CI should be computed for n >= 3."""
        rng = np.random.RandomState(42)
        months = 24
        year_fracs = np.linspace(2020, 2022, months)
        residuals = -0.005 * (year_fracs - 2020) + rng.normal(0, 0.01, months)

        df = pd.DataFrame({
            "year": [int(y) for y in year_fracs],
            "month": [(i % 12) + 1 for i in range(months)],
            "year_fraction": year_fracs,
            "mean_residual_pu": residuals,
        })

        trend = DegradationService.fit_degradation_trend(df)
        assert trend["ci95"] is not None
        assert trend["ci95"][0] < trend["slope"] < trend["ci95"][1]

    def test_returns_none_for_insufficient_data(self):
        df = pd.DataFrame({
            "year": [2024],
            "month": [1],
            "year_fraction": [2024.042],
            "mean_residual_pu": [0.0],
        })
        trend = DegradationService.fit_degradation_trend(df)
        assert trend is None

    def test_r_squared_in_range(self):
        rng = np.random.RandomState(42)
        months = 36
        year_fracs = np.linspace(2020, 2023, months)
        residuals = -0.01 * (year_fracs - 2020) + rng.normal(0, 0.002, months)

        df = pd.DataFrame({
            "year": [int(y) for y in year_fracs],
            "month": [(i % 12) + 1 for i in range(months)],
            "year_fraction": year_fracs,
            "mean_residual_pu": residuals,
        })

        trend = DegradationService.fit_degradation_trend(df)
        assert 0 <= trend["r2"] <= 1
