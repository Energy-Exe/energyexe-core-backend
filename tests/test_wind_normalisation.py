"""Unit tests for wind normalisation service — Module 4."""

import numpy as np
import pandas as pd
import pytest

from app.services.wind_normalisation_service import NORM_WIND_MIN_MPS, WindNormalisationService

# ─── Fixtures ─────────────────────────────────────────────────


def _make_hourly_df(start: str, end: str, *, seed: int = 13) -> pd.DataFrame:
    """Synthetic full-hourly DF with norm_ratio in {year, month, norm_ratio,
    hour} shape that compute_indices expects.
    """
    rng = np.random.RandomState(seed)
    hours = pd.date_range(start=start, end=end, freq="h", inclusive="left")
    n = len(hours)
    # Norm ratio centred ~0.95 with small noise
    ratios = 0.95 + rng.normal(0, 0.05, n)
    return pd.DataFrame(
        {
            "hour": hours,
            "year": hours.year.astype(int),
            "month": hours.month.astype(int),
            "wind_speed": rng.uniform(5, 12, n),
            "norm_ratio": ratios,
        }
    )


# ─── compute_indices ──────────────────────────────────────────


class TestComputeIndices:
    """C1: yearly = mean of monthly means; separate yearly historical_mean."""

    def test_monthly_index_unchanged_for_full_coverage(self):
        """With full coverage, monthly indices behave like before (~100)."""
        df = _make_hourly_df("2020-01-01", "2023-01-01")
        monthly, yearly = WindNormalisationService.compute_indices(df)
        # 36 months
        assert len(monthly) == 36
        # Each month near 100
        assert all(50 < v < 200 for v in monthly["index_vs_base"])

    def test_yearly_uses_mean_of_monthly_means(self):
        """Spec: yearly avg = mean of monthly avg_norm_ratios (not hourly avg).
        Constructed so the two diverge: make Dec have far more hours than other
        months. Hourly groupby would weight Dec; monthly-mean would not.
        """
        # Jan-Nov: 1 hour each. Dec: 1000 hours.
        rows = []
        ratios = []
        for month in range(1, 12):
            rows.append({"hour": pd.Timestamp(2020, month, 1), "year": 2020, "month": month})
            ratios.append(1.00)
        for h in range(1000):
            rows.append(
                {
                    "hour": pd.Timestamp(2020, 12, 1) + pd.Timedelta(hours=h),
                    "year": 2020,
                    "month": 12,
                }
            )
            ratios.append(0.50)
        df = pd.DataFrame(rows)
        df["norm_ratio"] = ratios

        monthly, yearly = WindNormalisationService.compute_indices(df)
        # Monthly avg_norm_ratio for 2020-12 should be 0.50, others 1.00
        dec = monthly[monthly["month"] == 12].iloc[0]
        assert abs(dec["avg_norm_ratio"] - 0.50) < 1e-9
        # Yearly = mean of those 12 monthly values = (11 * 1.00 + 0.50) / 12
        expected = (11 * 1.00 + 0.50) / 12
        assert abs(yearly.iloc[0]["avg_norm_ratio"] - expected) < 1e-9

    def test_yearly_historical_mean_separate_from_monthly(self):
        """yearly index uses yearly's own historical_mean — verify that
        when 2024 has only 1 month while 2023 has 12, the yearly index for
        2024 differs from what monthly-historical-mean would produce.
        """
        rows = []
        # 2023: 12 full months at norm_ratio=1.00 (768 hrs total)
        for month in range(1, 13):
            for h in range(64):
                rows.append(
                    {
                        "hour": pd.Timestamp(2023, month, 1) + pd.Timedelta(hours=h),
                        "year": 2023,
                        "month": month,
                        "norm_ratio": 1.00,
                    }
                )
        # 2024: only January, at norm_ratio=0.80
        for h in range(64):
            rows.append(
                {
                    "hour": pd.Timestamp(2024, 1, 1) + pd.Timedelta(hours=h),
                    "year": 2024,
                    "month": 1,
                    "norm_ratio": 0.80,
                }
            )

        df = pd.DataFrame(rows)
        monthly, yearly = WindNormalisationService.compute_indices(df)

        # Yearly avg for 2023 = mean of 12 ones = 1.0; for 2024 = mean of [0.80] = 0.80
        y23 = yearly[yearly["year"] == 2023].iloc[0]
        y24 = yearly[yearly["year"] == 2024].iloc[0]
        assert abs(y23["avg_norm_ratio"] - 1.0) < 1e-9
        assert abs(y24["avg_norm_ratio"] - 0.80) < 1e-9

        # Yearly historical_mean = mean of yearly values = (1.0 + 0.80) / 2 = 0.90
        # So 2023 index = 1.0 / 0.90 × 100 ≈ 111.11
        # And 2024 index = 0.80 / 0.90 × 100 ≈ 88.89
        assert abs(y23["index_vs_base"] - (1.0 / 0.90 * 100)) < 0.01
        assert abs(y24["index_vs_base"] - (0.80 / 0.90 * 100)) < 0.01

    def test_empty_input_returns_empty(self):
        df = pd.DataFrame(columns=["hour", "year", "month", "norm_ratio"])
        monthly, yearly = WindNormalisationService.compute_indices(df)
        assert monthly.empty
        assert yearly.empty
