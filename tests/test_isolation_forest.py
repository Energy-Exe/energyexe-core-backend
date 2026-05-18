"""Unit tests for PerformanceAnomalyService.detect_isolation_forest_anomalies.

Validates spec item 5.2 — IsolationForest secondary anomaly layer is
informational only, returns the right shape, and gracefully degrades when
sklearn is unavailable.
"""

import numpy as np
import pandas as pd
import pytest

from app.services.performance_anomaly_service import (
    HAS_SKLEARN,
    PerformanceAnomalyService,
)


def _normal_curve_df(n_normal=400, n_outliers=12, seed=42):
    """Build a (wind_speed, p_pu) df with a clear power-curve-like shape +
    a handful of obvious outliers far above the curve."""
    rng = np.random.default_rng(seed)
    # Sigmoid-ish normal data
    wind = rng.uniform(4, 14, n_normal)
    base = 1.0 / (1.0 + np.exp(-(wind - 8.5)))
    p_pu = np.clip(base + rng.normal(0, 0.03, n_normal), 0, 1.0)

    # Outliers: extreme p_pu values at random wind speeds
    outlier_wind = rng.uniform(4, 14, n_outliers)
    outlier_pu = np.full(n_outliers, 1.5)  # well above the natural ceiling

    return pd.DataFrame(
        {
            "wind_speed": np.concatenate([wind, outlier_wind]),
            "p_pu": np.concatenate([p_pu, outlier_pu]),
        }
    )


@pytest.mark.skipif(not HAS_SKLEARN, reason="sklearn not installed")
class TestIsolationForestWithSklearn:
    def test_returns_boolean_series_aligned_with_index(self):
        df = _normal_curve_df()
        flag = PerformanceAnomalyService.detect_isolation_forest_anomalies(df)
        assert isinstance(flag, pd.Series)
        assert flag.dtype == bool
        assert len(flag) == len(df)
        assert flag.index.equals(df.index)

    def test_flags_some_outliers(self):
        df = _normal_curve_df(n_normal=400, n_outliers=20)
        flag = PerformanceAnomalyService.detect_isolation_forest_anomalies(
            df, contamination=0.05
        )
        # We expect approximately contamination × n flagged
        # Allow generous range — IsolationForest is randomised
        n_flagged = int(flag.sum())
        assert 5 <= n_flagged <= 60

    def test_too_few_rows_returns_all_false(self):
        df = _normal_curve_df(n_normal=20, n_outliers=2)
        flag = PerformanceAnomalyService.detect_isolation_forest_anomalies(df)
        assert flag.sum() == 0  # threshold for "stable fit" is 50 rows

    def test_handles_nan_inputs(self):
        df = _normal_curve_df()
        # Inject some NaN rows that should be silently dropped
        df.loc[0, "wind_speed"] = float("nan")
        df.loc[1, "p_pu"] = float("nan")
        flag = PerformanceAnomalyService.detect_isolation_forest_anomalies(df)
        # NaN rows should not be flagged (kept False since they were dropped
        # before fit and re-attached as False)
        assert flag.iloc[0] == False  # noqa: E712 — explicit boolean
        assert flag.iloc[1] == False  # noqa: E712


class TestIsolationForestNoSklearn:
    """If sklearn is not installed, the function should no-op gracefully.

    This test runs regardless of HAS_SKLEARN by monkeypatching the module flag.
    """

    def test_returns_all_false_when_sklearn_missing(self, monkeypatch):
        monkeypatch.setattr(
            "app.services.performance_anomaly_service.HAS_SKLEARN", False
        )
        df = _normal_curve_df()
        flag = PerformanceAnomalyService.detect_isolation_forest_anomalies(df)
        assert isinstance(flag, pd.Series)
        assert flag.sum() == 0
        assert len(flag) == len(df)
