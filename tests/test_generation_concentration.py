"""Unit tests for GenerationConcentrationService._compute_metrics.

Pure pandas/numpy — no database, no async fixtures. Mirrors the pattern
established in tests/test_degradation.py.

Validates spec item 3 math:
- decile shares sum to ~100%
- capture ratio behaves correctly under perfect / inverse / random correlation
- nan and zero-price hours are dropped
- low-data scenarios return None gracefully
"""

import math

import numpy as np
import pandas as pd
import pytest

from app.services.generation_concentration_service import (
    GenerationConcentrationService,
)


def _make_hourly_df(prices, generations):
    """Build a minimal DataFrame the service expects."""
    assert len(prices) == len(generations)
    return pd.DataFrame(
        {
            "hour": pd.date_range("2024-01-01", periods=len(prices), freq="h"),
            "market_price": prices,
            "generation_mwh": generations,
        }
    )


def _full_year_df(rng_seed=42):
    """8,760-hour synthetic year with random prices and a wind-correlated gen profile."""
    rng = np.random.default_rng(rng_seed)
    prices = rng.uniform(10, 100, 8760)
    # Generation has weak positive correlation with price (windy & valuable hours align)
    generations = rng.uniform(10, 100, 8760) + 0.1 * (prices - 50)
    generations = np.clip(generations, 0, None)
    return _make_hourly_df(prices, generations)


class TestComputeMetricsBasics:
    """Basic invariants of the decile decomposition."""

    def test_returns_none_when_too_few_rows(self):
        df = _make_hourly_df([10, 20, 30], [1, 2, 3])
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert result is None

    def test_returns_none_when_total_mwh_zero(self):
        prices = list(np.linspace(10, 100, 200))
        gens = [0.0] * 200
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert result is None

    def test_decile_shares_sum_to_100(self):
        df = _full_year_df()
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert result is not None
        total = sum(result["decile_shares_full"].values())
        assert math.isclose(total, 100.0, abs_tol=0.5)  # rounding tolerance

    def test_decile_shares_have_10_buckets(self):
        df = _full_year_df()
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        # qcut may collapse buckets only if many ties; with random uniform
        # prices we should get all 10
        assert len(result["decile_shares_full"]) == 10

    def test_total_hours_and_total_mwh_consistent(self):
        df = _full_year_df()
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        # df has 8760 rows and our generation values are all >= 0; some
        # rounding noise allowed but should be very close.
        assert result["total_hours"] == 8760
        assert result["total_mwh"] > 0


class TestCaptureRatioInterpretation:
    """Capture ratio = volume-weighted-price / time-weighted-price.

    >1 = generates in higher-price hours (positive correlation)
    =1 = uncorrelated
    <1 = generates in lower-price hours (inverse correlation)
    """

    def test_perfect_positive_correlation_above_1(self):
        # Generation = price → all generation in high-price hours
        prices = list(np.linspace(10, 100, 1000))
        gens = list(prices)  # generation directly proportional
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert result["capture_ratio"] > 1.05

    def test_perfect_inverse_correlation_below_1(self):
        # Generation = -price + offset → all generation in LOW-price hours
        prices = list(np.linspace(10, 100, 1000))
        gens = [110 - p for p in prices]  # inverse
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert result["capture_ratio"] < 0.95

    def test_uncorrelated_near_1(self):
        # Random gen — capture ratio should hover near 1.0
        rng = np.random.default_rng(0)
        prices = list(rng.uniform(10, 100, 5000))
        gens = list(rng.uniform(10, 100, 5000))  # independent draws
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert 0.95 < result["capture_ratio"] < 1.05


class TestDecileShares:
    """Top decile / bottom decile / quartile breakdown."""

    def test_perfect_correlation_concentrates_in_top_decile(self):
        prices = list(np.linspace(10, 100, 1000))
        gens = list(prices)
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        # When gen = price, the top-priced hours have proportionally more
        # generation. Top decile should be substantially > 10%.
        assert result["top_decile_share_pct"] > 15.0
        # And bottom decile should be < 10%
        assert result["bottom_decile_share_pct"] < 8.0

    def test_perfect_inverse_concentrates_in_bottom_decile(self):
        prices = list(np.linspace(10, 100, 1000))
        gens = [110 - p for p in prices]
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        # Top decile starves; bottom decile gets the most
        assert result["top_decile_share_pct"] < 8.0
        assert result["bottom_decile_share_pct"] > 15.0

    def test_quartile_neutral_around_30(self):
        df = _full_year_df()
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        # With weak correlation in our synthetic data, each quartile should
        # be loosely in 20-40% range
        assert 18.0 < result["bottom_quartile_share_pct"] < 40.0
        assert 18.0 < result["top_quartile_share_pct"] < 40.0


class TestNullAndZeroFiltering:
    """Spec: rows with NaN price or negative generation are dropped."""

    def test_nan_prices_dropped(self):
        # 1000 rows total, half have NaN price
        rng = np.random.default_rng(1)
        prices = list(rng.uniform(10, 100, 500)) + [float("nan")] * 500
        gens = list(rng.uniform(10, 100, 1000))
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert result is not None
        # The 500 NaN-price rows are dropped
        assert result["total_hours"] == 500

    def test_negative_generation_dropped(self):
        rng = np.random.default_rng(2)
        prices = list(rng.uniform(10, 100, 1000))
        gens = list(rng.uniform(10, 100, 500)) + [-1.0] * 500
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        # Filter is `>= 0`, so -1.0 rows drop
        assert result["total_hours"] == 500


class TestPriceFields:
    """Volume-weighted vs time-weighted average prices."""

    def test_time_weighted_avg_matches_simple_mean(self):
        prices = list(np.linspace(10, 100, 1000))
        gens = [50.0] * 1000  # constant generation
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        # When gen is constant, weighted == time-weighted == simple mean
        assert math.isclose(
            result["time_weighted_avg_price_eur"],
            float(np.mean(prices)), abs_tol=0.01,
        )
        assert math.isclose(
            result["weighted_avg_capture_price_eur"],
            float(np.mean(prices)), abs_tol=0.01,
        )

    def test_weighted_higher_than_time_when_positive_correlation(self):
        prices = list(np.linspace(10, 100, 1000))
        gens = list(prices)  # positive correlation
        df = _make_hourly_df(prices, gens)
        result = GenerationConcentrationService(db=None)._compute_metrics(df)
        assert (
            result["weighted_avg_capture_price_eur"]
            > result["time_weighted_avg_price_eur"]
        )
