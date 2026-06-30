"""Unit tests for performance anomaly service — pure pandas/numpy, no database."""

import numpy as np
import pandas as pd
import pytest

from app.services.performance_anomaly_service import (
    CEILING_PU,
    LONG_RUN_HOURS,
    OVERPERF_MAD_K,
    UNDERPERF_MAD_K,
    PerformanceAnomalyService,
)


def _make_capability_stats():
    """Create mock capability curve stats for bins 4-13 m/s."""
    records = []
    for left in range(4, 14):
        # Simulate sigmoid power curve
        mid = left + 0.5
        q50 = 1 / (1 + np.exp(-(mid - 8)))
        q90 = min(q50 + 0.08, 1.0)
        mad = 0.03
        records.append({
            "wind_bin": pd.Interval(float(left), float(left + 1), closed="left"),
            "q50_pu": q50,
            "q90_pu": q90,
            "mad_pu": mad,
            "sample_count": 500,
        })
    return pd.DataFrame(records)


def _make_hourly_df(n=100, seed=42):
    """Create synthetic hourly data."""
    rng = np.random.RandomState(seed)
    wind = rng.uniform(4, 14, size=n)
    p_pu = 1 / (1 + np.exp(-(wind - 8))) + rng.normal(0, 0.02, n)
    p_pu = np.clip(p_pu, 0, 1.0)

    return pd.DataFrame({
        "hour": pd.date_range("2024-01-01", periods=n, freq="h"),
        "generation_mwh": p_pu * 100,
        "wind_speed": wind,
        "market_price": rng.uniform(20, 60, n),
        "p_pu": p_pu,
    })


class TestClassifyHours:
    """Test hour classification."""

    def test_flags_underperformance(self):
        df = _make_hourly_df()
        cap = _make_capability_stats()
        # Inject a severely underperforming hour
        df.loc[0, "p_pu"] = 0.01
        df.loc[0, "wind_speed"] = 10.0  # Expected q50 ≈ 0.88
        df.loc[0, "generation_mwh"] = 1.0

        result = PerformanceAnomalyService.classify_hours(df, cap, rated_mw=100.0)
        assert result.loc[0, "anomaly_type"] == "underperformance"
        assert result.loc[0, "lost_mwh"] > 0

    def test_flags_overperformance_ceiling(self):
        df = _make_hourly_df()
        cap = _make_capability_stats()
        df.loc[0, "p_pu"] = 1.05  # Above ceiling
        df.loc[0, "wind_speed"] = 10.0

        result = PerformanceAnomalyService.classify_hours(df, cap, rated_mw=100.0)
        assert result.loc[0, "anomaly_type"] == "overperformance"

    def test_normal_hours_not_flagged(self):
        df = _make_hourly_df(n=500)
        cap = _make_capability_stats()

        result = PerformanceAnomalyService.classify_hours(df, cap, rated_mw=100.0)
        anomaly_pct = result["is_anomaly"].sum() / len(result) * 100
        # Most hours should be normal (< 30% anomalous with reasonable data)
        assert anomaly_pct < 30, f"Too many anomalies: {anomaly_pct:.1f}%"

    def test_lost_mwh_non_negative(self):
        df = _make_hourly_df()
        cap = _make_capability_stats()
        result = PerformanceAnomalyService.classify_hours(df, cap, rated_mw=100.0)
        assert (result["lost_mwh"] >= 0).all()

    def test_lost_eur_uses_ppa_price(self):
        df = _make_hourly_df()
        cap = _make_capability_stats()
        df.loc[0, "p_pu"] = 0.01
        df.loc[0, "wind_speed"] = 10.0
        df.loc[0, "generation_mwh"] = 1.0

        result = PerformanceAnomalyService.classify_hours(
            df, cap, rated_mw=100.0, ppa_price=50.0
        )
        # Lost EUR should use PPA price
        lost_mwh = result.loc[0, "lost_mwh"]
        lost_eur = result.loc[0, "lost_eur"]
        assert abs(lost_eur - lost_mwh * 50.0) < 0.01

    def test_full_bin_coverage_no_categorical_error(self):
        """Regression: pipeline_anomaly_error 'float * Categorical'.

        `wind_bin_interval` is a pandas Categorical (pd.cut). Series.map over it
        returns a Categorical-dtyped Series when every category maps to a distinct
        value (pandas 2.x). That made q50_bin/q90_bin/mad_bin categorical, so the
        downstream arithmetic (`UNDERPERF_MAD_K * mad_bin`, `q50_bin * rated_mw`)
        raised TypeError. This only surfaced for windfarms whose capability stats
        covered the bins one-to-one (e.g. wf 7200, 2018/2020) — partial coverage
        leaves duplicate Nones, which keeps the map non-categorical. Here every bin
        maps to a unique value to reproduce the failing case.
        """
        # Capability stats for every bin pd.cut produces (2..25 m/s), each with
        # strictly unique q50/q90/mad so the .map() stays Categorical.
        records = []
        for i, left in enumerate(range(2, 25)):
            records.append({
                "wind_bin": pd.Interval(float(left), float(left + 1), closed="left"),
                "q50_pu": 0.30 + 0.01 * i,
                "q90_pu": 0.60 + 0.011 * i,
                "mad_pu": 0.020 + 0.001 * i,
                "sample_count": 500,
            })
        cap = pd.DataFrame(records)
        df = _make_hourly_df(n=60)

        # Must not raise; loss columns must be numeric (not Categorical).
        result = PerformanceAnomalyService.classify_hours(df, cap, rated_mw=100.0)
        assert result["expected_mwh"].dtype.kind == "f"
        assert (result["lost_mwh"] >= 0).all()


class TestAssignRunIds:
    """Test consecutive underperformance run grouping."""

    def test_consecutive_hours_same_run(self):
        df = pd.DataFrame({
            "hour": pd.date_range("2024-01-01", periods=5, freq="h"),
            "anomaly_type": ["underperformance"] * 5,
            "p_pu": [0.1] * 5,
        })
        result = PerformanceAnomalyService.assign_run_ids(df)
        # All 5 consecutive hours should be in the same run
        assert result["run_id"].nunique() == 1

    def test_gap_creates_new_run(self):
        hours = pd.to_datetime([
            "2024-01-01 00:00", "2024-01-01 01:00",  # Run 0
            "2024-01-01 05:00", "2024-01-01 06:00",  # Run 1 (gap)
        ])
        df = pd.DataFrame({
            "hour": hours,
            "anomaly_type": ["underperformance"] * 4,
            "p_pu": [0.1] * 4,
        })
        result = PerformanceAnomalyService.assign_run_ids(df)
        assert result["run_id"].nunique() == 2

    def test_no_underperformance_no_runs(self):
        df = pd.DataFrame({
            "hour": pd.date_range("2024-01-01", periods=5, freq="h"),
            "anomaly_type": [None] * 5,
            "p_pu": [0.5] * 5,
        })
        result = PerformanceAnomalyService.assign_run_ids(df)
        assert result["run_id"].isna().all()


class TestAggregateODI:
    """Test ODI aggregation."""

    def test_odi_metrics(self):
        df = pd.DataFrame({
            "hour": pd.date_range("2024-01-01", periods=100, freq="h"),
            "anomaly_type": ["underperformance"] * 10 + [None] * 90,
            "lost_mwh": [5.0] * 10 + [0.0] * 90,
            "lost_eur": [200.0] * 10 + [0.0] * 90,
            "expected_mwh": [50.0] * 100,
            "market_price": [40.0] * 100,
            "run_id": list(range(10)) + [None] * 90,
        })
        monthly, yearly = PerformanceAnomalyService.aggregate_summaries(df, 2024)

        assert yearly["odi_pct_underperf"] == 10.0  # 10/100 * 100
        assert yearly["lost_mwh"] == 50.0  # 10 * 5.0
        assert yearly["underperf_hours"] == 10

    def test_zero_underperformance(self):
        df = pd.DataFrame({
            "hour": pd.date_range("2024-06-01", periods=50, freq="h"),
            "anomaly_type": [None] * 50,
            "lost_mwh": [0.0] * 50,
            "lost_eur": [0.0] * 50,
            "expected_mwh": [50.0] * 50,
            "market_price": [40.0] * 50,
            "run_id": [None] * 50,
        })
        monthly, yearly = PerformanceAnomalyService.aggregate_summaries(df, 2024)

        assert yearly["odi_pct_underperf"] == 0.0
        assert yearly["lost_mwh"] == 0.0
