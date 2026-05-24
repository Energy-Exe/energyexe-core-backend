"""W2 — anomaly service warns when > 5% of hours have NaN market_price."""

import logging

import numpy as np
import pandas as pd
import pytest
import structlog
from structlog.testing import capture_logs

from app.services.performance_anomaly_service import (
    NAN_PRICE_WARN_RATIO,
    _warn_if_market_price_nan_heavy,
)


def _df_with_nan_ratio(nan_ratio: float, n: int = 1000) -> pd.DataFrame:
    rng = np.random.RandomState(0)
    prices = rng.uniform(20, 60, n)
    n_nan = int(n * nan_ratio)
    if n_nan:
        idx = rng.choice(n, size=n_nan, replace=False)
        prices[idx] = np.nan
    return pd.DataFrame(
        {
            "hour": pd.date_range("2024-01-01", periods=n, freq="h"),
            "market_price": prices,
        }
    )


def test_warns_above_threshold():
    df = _df_with_nan_ratio(nan_ratio=0.10)  # 10% > 5%
    with capture_logs() as logs:
        _warn_if_market_price_nan_heavy(df, windfarm_id=42, year=2024)
    assert any(
        log.get("event") == "anomaly_nan_price_heavy" and log.get("windfarm_id") == 42
        for log in logs
    )


def test_silent_below_threshold():
    df = _df_with_nan_ratio(nan_ratio=0.02)  # 2% < 5%
    with capture_logs() as logs:
        _warn_if_market_price_nan_heavy(df, windfarm_id=42, year=2024)
    assert not any(log.get("event") == "anomaly_nan_price_heavy" for log in logs)


def test_silent_for_empty_df():
    with capture_logs() as logs:
        _warn_if_market_price_nan_heavy(pd.DataFrame(), windfarm_id=42, year=2024)
    assert not any(log.get("event") == "anomaly_nan_price_heavy" for log in logs)


def test_silent_when_no_nans():
    df = _df_with_nan_ratio(nan_ratio=0.0)
    with capture_logs() as logs:
        _warn_if_market_price_nan_heavy(df, windfarm_id=42, year=2024)
    assert not any(log.get("event") == "anomaly_nan_price_heavy" for log in logs)


def test_silent_when_missing_column():
    df = pd.DataFrame({"hour": pd.date_range("2024-01-01", periods=10, freq="h")})
    with capture_logs() as logs:
        _warn_if_market_price_nan_heavy(df, windfarm_id=42, year=2024)
    assert not any(log.get("event") == "anomaly_nan_price_heavy" for log in logs)


def test_threshold_constant_matches_plan():
    """Plan W2 specifies 5% — keep it visible as a constant."""
    assert NAN_PRICE_WARN_RATIO == 0.05
