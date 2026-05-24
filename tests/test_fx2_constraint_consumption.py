"""FX2 — Modules 3/4/5 consume active structural-constraint flags.

Covers the pure helpers (no DB) for building the row-level mask. The
orchestrator wiring is tested implicitly via the existing pipeline
integration tests; this file pins down the building blocks.
"""

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from app.services.structural_constraint_detection_service import build_constraint_mask


def _make_df(start: str, hours: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hour": pd.date_range(start, periods=hours, freq="h", tz="UTC"),
            "p_pu": np.zeros(hours),
        }
    )


class TestBuildConstraintMask:
    def test_no_periods_returns_all_false(self):
        df = _make_df("2024-01-01", 100)
        mask = build_constraint_mask(df, periods=[])
        assert mask.sum() == 0
        assert len(mask) == len(df)

    def test_single_period_masks_correct_rows(self):
        df = _make_df("2024-01-01 00:00", 24)
        period = {
            "period_start": pd.Timestamp("2024-01-01 06:00", tz="UTC"),
            "period_end": pd.Timestamp("2024-01-01 11:00", tz="UTC"),
        }
        mask = build_constraint_mask(df, periods=[period])
        # Hours 6,7,8,9,10,11 → 6 rows (closed-closed interval)
        assert mask.sum() == 6
        assert mask.iloc[6:12].all()
        assert not mask.iloc[:6].any()
        assert not mask.iloc[12:].any()

    def test_multiple_periods_union(self):
        df = _make_df("2024-01-01 00:00", 48)
        periods = [
            {
                "period_start": pd.Timestamp("2024-01-01 02:00", tz="UTC"),
                "period_end": pd.Timestamp("2024-01-01 05:00", tz="UTC"),
            },
            {
                "period_start": pd.Timestamp("2024-01-01 10:00", tz="UTC"),
                "period_end": pd.Timestamp("2024-01-01 13:00", tz="UTC"),
            },
        ]
        mask = build_constraint_mask(df, periods=periods)
        # 4 + 4 = 8 hours (non-overlapping closed-closed intervals)
        assert mask.sum() == 8

    def test_overlapping_periods_dont_double_count(self):
        df = _make_df("2024-01-01 00:00", 48)
        periods = [
            {
                "period_start": pd.Timestamp("2024-01-01 02:00", tz="UTC"),
                "period_end": pd.Timestamp("2024-01-01 10:00", tz="UTC"),
            },
            {
                "period_start": pd.Timestamp("2024-01-01 05:00", tz="UTC"),
                "period_end": pd.Timestamp("2024-01-01 12:00", tz="UTC"),
            },
        ]
        mask = build_constraint_mask(df, periods=periods)
        # Union: 2..12 = 11 hours
        assert mask.sum() == 11

    def test_handles_naive_timestamps_in_df(self):
        """If df['hour'] is tz-naive we still mask correctly (localize UTC)."""
        df = pd.DataFrame(
            {
                "hour": pd.date_range("2024-01-01 00:00", periods=10, freq="h"),
                "p_pu": np.zeros(10),
            }
        )
        period = {
            "period_start": datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
            "period_end": datetime(2024, 1, 1, 6, tzinfo=timezone.utc),
        }
        mask = build_constraint_mask(df, periods=[period])
        assert mask.sum() == 4

    def test_handles_naive_period_bounds(self):
        """If period dicts have tz-naive datetimes we still mask correctly."""
        df = _make_df("2024-01-01 00:00", 10)
        period = {
            "period_start": datetime(2024, 1, 1, 3),  # naive
            "period_end": datetime(2024, 1, 1, 6),
        }
        mask = build_constraint_mask(df, periods=[period])
        assert mask.sum() == 4

    def test_empty_df(self):
        df = pd.DataFrame({"hour": pd.Series([], dtype="datetime64[ns, UTC]"), "p_pu": []})
        period = {
            "period_start": pd.Timestamp("2024-01-01", tz="UTC"),
            "period_end": pd.Timestamp("2024-01-02", tz="UTC"),
        }
        mask = build_constraint_mask(df, periods=[period])
        assert len(mask) == 0
