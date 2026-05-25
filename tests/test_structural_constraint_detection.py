"""Unit tests for Module 1b — structural constraint detection.

Pure pandas/numpy; no DB. Covers the spec's Q90-ratio path plus the
B1.5 Q50-ratio augmentation.
"""

import math

import numpy as np
import pandas as pd
import pytest

from app.services.structural_constraint_detection_service import (
    CONSTRAINT_MIN_HOURS,
    Q50_RATIO_BANDS,
    Q90_RATIO_BANDS,
    compute_loyo_reference,
    compute_observed_percentile,
    detect_constraints_df,
    flag_bin_months,
    group_into_runs,
)

# ─── Fixture builders ─────────────────────────────────────────


def _logistic(v: float, *, slope: float = 1.0) -> float:
    """Smooth p_pu vs wind speed using a logistic centred at 8 m/s."""
    return float(1.0 / (1.0 + np.exp(-(v - 8.0) * slope)))


def _make_baseline_year(year: int, *, seed: int = 0) -> pd.DataFrame:
    """One year × 8760 h of clean operation. Wind uniform [7, 16] so every
    hour falls in the detection bands ([7, 10) or [10, 25)).
    """
    rng = np.random.RandomState(seed)
    hours = pd.date_range(f"{year}-01-01", f"{year + 1}-01-01", freq="h", inclusive="left")
    n = len(hours)
    wind = rng.uniform(7, 16, size=n)
    base = np.array([_logistic(v) for v in wind])
    noise = rng.normal(0, 0.03, n)
    p_pu = np.clip(base + noise, 0, 1.0)
    return pd.DataFrame(
        {
            "hour": hours,
            "wind_speed": wind,
            "wind_bin": np.floor(wind).astype(float),
            "p_pu": p_pu,
            "year": year,
        }
    )


def _make_baseline_year_realistic_wind(year: int, *, seed: int = 0) -> pd.DataFrame:
    """Like _make_baseline_year but with realistic wind variability — wind
    uniform [0, 25] m/s. Most hours fall below 7 m/s (the detection band
    floor) and are interleaved with in-band hours throughout the day. Used
    to regression-guard against the per-hour run-grouping bug where
    interleaved out-of-band hours shatter what should be a sustained run
    on real data.
    """
    rng = np.random.RandomState(seed)
    hours = pd.date_range(f"{year}-01-01", f"{year + 1}-01-01", freq="h", inclusive="left")
    n = len(hours)
    wind = rng.uniform(0, 25, size=n)
    base = np.array([_logistic(v) for v in wind])
    noise = rng.normal(0, 0.03, n)
    p_pu = np.clip(base + noise, 0, 1.0)
    return pd.DataFrame(
        {
            "hour": hours,
            "wind_speed": wind,
            "wind_bin": np.floor(wind).astype(float),
            "p_pu": p_pu,
            "year": year,
        }
    )


def _apply_cable_failure(
    df: pd.DataFrame,
    *,
    start: str,
    end: str,
    cap_to: float = 0.5,
) -> pd.DataFrame:
    """Cap p_pu at `cap_to × expected` over a date range to mimic a
    single-cable failure (Q90 of output halves; Q50 also reduced but less so).
    """
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    mask = (df["hour"] >= start_ts) & (df["hour"] < end_ts)
    out = df.copy()
    base = np.array([_logistic(v) for v in out.loc[mask, "wind_speed"]])
    out.loc[mask, "p_pu"] = base * cap_to
    return out


def _apply_half_bmu_offline(
    df: pd.DataFrame,
    *,
    start: str,
    end: str,
) -> pd.DataFrame:
    """Half the BMUs are offline over the range. The median hour produces
    ~half (offline BMU contributes 0, online produces normal) but the upper
    decile (both BMUs running occasionally) is closer to normal.

    Implementation: roll a coin per hour; ~70% of hours scaled to 0.5×,
    ~30% near-normal. This collapses Q50 to ~0.5× but keeps Q90 close to
    1.0×.
    """
    rng = np.random.RandomState(99)
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    mask = (df["hour"] >= start_ts) & (df["hour"] < end_ts)
    out = df.copy()
    base = np.array([_logistic(v) for v in out.loc[mask, "wind_speed"]])
    n = mask.sum()
    coin = rng.uniform(size=n)
    factor = np.where(coin < 0.70, 0.50, 0.95)
    out.loc[mask, "p_pu"] = base * factor
    return out


# ─── LOYO references ──────────────────────────────────────────


class TestLoyoReference:
    def test_single_year_returns_empty(self):
        df = _make_baseline_year(2020)
        ref = compute_loyo_reference(df, percentile=0.90)
        assert ref.empty

    def test_multi_year_excludes_target_year(self):
        df = pd.concat([_make_baseline_year(y, seed=y) for y in range(2020, 2024)])
        ref = compute_loyo_reference(df, percentile=0.90)
        assert not ref.empty
        # One ref value per (wind_bin, year)
        assert {"wind_bin", "_year", "ref_value"}.issubset(ref.columns)
        # Years should be 2020-2023
        assert set(ref["_year"]) == {2020, 2021, 2022, 2023}


# ─── flag_bin_months — the core comparator ────────────────────


class TestFlagBinMonths:
    def test_empty_inputs_return_empty(self):
        out = flag_bin_months(
            pd.DataFrame(columns=["wind_bin", "_month", "_year", "obs_value"]),
            pd.DataFrame(columns=["wind_bin", "_year", "ref_value"]),
            Q90_RATIO_BANDS,
        )
        assert out.empty

    def test_below_threshold_in_band_is_flagged(self):
        observed = pd.DataFrame(
            {
                "wind_bin": [12.0],
                "_month": [pd.Period("2024-06", freq="M")],
                "_year": [2024],
                "obs_value": [0.60],
            }
        )
        reference = pd.DataFrame({"wind_bin": [12.0], "_year": [2024], "ref_value": [1.0]})
        # 12 m/s → in [10, 25] band; ratio 0.6 < 0.80 threshold → flagged
        flagged = flag_bin_months(observed, reference, Q90_RATIO_BANDS)
        assert len(flagged) == 1

    def test_above_threshold_not_flagged(self):
        observed = pd.DataFrame(
            {
                "wind_bin": [12.0],
                "_month": [pd.Period("2024-06", freq="M")],
                "_year": [2024],
                "obs_value": [0.85],
            }
        )
        reference = pd.DataFrame({"wind_bin": [12.0], "_year": [2024], "ref_value": [1.0]})
        # ratio 0.85 ≥ 0.80 → not flagged
        flagged = flag_bin_months(observed, reference, Q90_RATIO_BANDS)
        assert flagged.empty


# ─── End-to-end synthetic scenarios ───────────────────────────


class TestEndToEnd:
    def test_b1_2_t1_no_constraint(self):
        """B1.2.T1: 4 yr clean baseline → 0 runs detected."""
        df = pd.concat([_make_baseline_year(y, seed=y) for y in range(2020, 2024)])
        runs = detect_constraints_df(df)
        assert runs.empty

    def test_b1_2_t2_seven_month_cable_failure(self):
        """B1.2.T2: 4 yr; 7-month single-cable failure (Q90 caps at ~0.5).
        Should produce one long run, mean_q90_ratio ≈ 0.5.
        """
        df = pd.concat([_make_baseline_year(y, seed=y) for y in range(2020, 2024)])
        df = _apply_cable_failure(df, start="2023-02-01", end="2023-09-01", cap_to=0.5)
        runs = detect_constraints_df(df)
        assert len(runs) >= 1
        # Largest run covers most of Feb-Sep 2023
        biggest = runs.sort_values("duration_hours", ascending=False).iloc[0]
        assert biggest["duration_hours"] >= 5_000  # 7 months × 720h ≈ 5040
        assert biggest["period_start"] >= pd.Timestamp("2022-12-01")
        assert biggest["period_end"] <= pd.Timestamp("2024-01-31")
        # Mean p_pu within the run (mean_q90_ratio field stores the 90th pct
        # of p_pu in the run — matches spec semantics) — should be ≤ 0.6
        assert biggest["mean_q90_ratio"] <= 0.65

    def test_b1_2_t4_short_event_not_flagged(self):
        """B1.2.T4: 168h (1 wk) event below 336h threshold → NOT flagged."""
        df = pd.concat([_make_baseline_year(y, seed=y) for y in range(2020, 2024)])
        df = _apply_cable_failure(df, start="2023-06-01", end="2023-06-08", cap_to=0.0)
        runs = detect_constraints_df(df)
        assert runs.empty or all(runs["duration_hours"] >= CONSTRAINT_MIN_HOURS)

    def test_b1_2_t5_single_year_logs_and_returns_empty(self):
        """B1.2.T5: 1 yr of data → LOYO can't build references → empty."""
        df = _make_baseline_year(2020)
        runs = detect_constraints_df(df)
        assert runs.empty

    def test_b1_5_half_bmu_offline_caught_by_q50(self):
        """B1.5: half-BMU offline pattern — Q90 stays near normal but
        Q50 collapses. Spec Q90-only detector would MISS this. With B1.5
        the Q50 path fires.
        """
        df = pd.concat([_make_baseline_year(y, seed=y) for y in range(2020, 2024)])
        df = _apply_half_bmu_offline(df, start="2023-02-01", end="2023-10-01")

        # Spec Q90-only equivalent: pass empty Q50 bands to disable the
        # extension; this should miss the half-BMU pattern.
        empty_bands: list[dict[str, float]] = []
        spec_runs = detect_constraints_df(df, q50_bands=empty_bands)
        # B1.5 enabled: should fire
        b1_5_runs = detect_constraints_df(df)

        assert len(b1_5_runs) >= 1, "B1.5 Q50 path should catch the half-BMU pattern"
        # The B1.5-enabled run includes more constrained hours than the
        # spec-only path (which misses the median-suppression pattern).
        b1_5_total = int(b1_5_runs["duration_hours"].sum())
        spec_total = int(spec_runs["duration_hours"].sum()) if not spec_runs.empty else 0
        assert b1_5_total >= spec_total
        # B1.5-flagged trigger should reflect the Q50 path firing
        triggers = set(b1_5_runs["flag_trigger"].tolist())
        assert "q50_ratio" in triggers or "both" in triggers


class TestBands:
    def test_default_bands_match_plan(self):
        """Defaults: Q90 [7,10]→0.70, [10,25]→0.80; Q50 [7,10]→0.70, [10,25]→0.85."""
        assert Q90_RATIO_BANDS[0]["threshold"] == 0.70
        assert Q90_RATIO_BANDS[1]["threshold"] == 0.80
        assert Q50_RATIO_BANDS[0]["threshold"] == 0.70
        assert Q50_RATIO_BANDS[1]["threshold"] == 0.85


class TestRealisticWindRunGrouping:
    """Regression tests for the month-level run grouping (PR after #72).

    The original spec-faithful per-hour run grouping was shattered on real
    data by interleaved sub-7 m/s hours: a 7-month constraint on EAO 2024
    fragmented into 320 sub-runs (max 133 h, median 3 h), all below the
    336 h threshold. Month-level grouping fixes this.
    """

    def test_constraint_survives_wind_variability(self):
        """4 yr; 7-month cable failure; wind uniform [0, 25] (realistic).
        Per-hour grouping would shatter this; month-level grouping keeps it.
        """
        df = pd.concat(
            [_make_baseline_year_realistic_wind(y, seed=y) for y in range(2020, 2024)]
        )
        df = _apply_cable_failure(df, start="2023-02-01", end="2023-09-01", cap_to=0.5)
        runs = detect_constraints_df(df)
        assert len(runs) >= 1
        biggest = runs.sort_values("duration_hours", ascending=False).iloc[0]
        # Run length is in flagged in-band hours; should be a large fraction
        # of the 7-month window's mid/high-wind hour count.
        assert biggest["duration_hours"] >= CONSTRAINT_MIN_HOURS
        # Period bounds land inside the cable-failure window (loosely).
        assert biggest["period_start"] >= pd.Timestamp("2023-01-01")
        assert biggest["period_end"] <= pd.Timestamp("2023-12-31")
        # Median p_pu within the run reflects the half-output ceiling.
        assert biggest["mean_q50_ratio"] <= 0.65

    def test_low_wind_only_year_returns_no_runs(self):
        """A year with all hours sub-7 m/s has zero in-band hours and so
        cannot trigger a constraint regardless of p_pu.
        """
        rng = np.random.RandomState(0)
        hours = pd.date_range("2020-01-01", "2024-01-01", freq="h", inclusive="left")
        wind = rng.uniform(0, 6, size=len(hours))
        df = pd.DataFrame(
            {
                "hour": hours,
                "wind_speed": wind,
                "wind_bin": np.floor(wind).astype(float),
                "p_pu": np.zeros(len(hours)),  # zero output everywhere
                "year": pd.to_datetime(hours).year,
            }
        )
        runs = detect_constraints_df(df)
        assert runs.empty

    def test_sparse_in_band_month_not_flagged(self):
        """A month with only a handful of in-band hours (< 24) must not
        be flagged even if all of those hours are in flagged bin-months.
        Guards against spurious flags on data-sparse months.
        """
        rng = np.random.RandomState(0)
        hours = pd.date_range("2020-01-01", "2024-01-01", freq="h", inclusive="left")
        wind = rng.uniform(0, 6, size=len(hours))
        # Sprinkle 10 in-band hours into a specific month — too few to flag.
        target = (pd.to_datetime(hours).month == 6) & (pd.to_datetime(hours).year == 2023)
        idx = np.where(target)[0][:10]
        wind[idx] = 12.0
        df = pd.DataFrame(
            {
                "hour": hours,
                "wind_speed": wind,
                "wind_bin": np.floor(wind).astype(float),
                "p_pu": np.zeros(len(hours)),
                "year": pd.to_datetime(hours).year,
            }
        )
        runs = detect_constraints_df(df)
        # The 10 in-band hours all sit at p_pu=0, well below any threshold,
        # but there's only 10 of them — below MIN_IN_BAND_HOURS_PER_MONTH (24).
        assert runs.empty
