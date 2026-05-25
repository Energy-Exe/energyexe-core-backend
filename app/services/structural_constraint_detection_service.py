"""Structural-constraint detection — Module 1b.

Auto-detects sustained low-output periods that look like infrastructure
constraints (cable failures, half-BMU offline, curtailment programmes,
etc.). Writes ``pending_review`` rows to ``structural_constraint_flags``
for analyst follow-up.

Active flags (``review_status IN ('pending_review', 'confirmed')``) are
loaded back by the orchestrator and used to mask out constrained hours
from Modules 3/4/5 before they consume the dataset. Analysts can flip a
flag to ``'dismissed'`` to bring its hours back into the calculation.

Compared with the reference pipeline (``energyexe_pipeline_full.py
:425-494``) we extend the detector with a parallel Q50-ratio check
(B1.5). The spec's Q90-only check has a structural blind spot for the
2024 multi-BMU offshore pattern where Q50 collapses but Q90 stays near
normal — confirmed on EAO and Hornsea 1 during P-1 validation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.structural_constraint_flag import StructuralConstraintFlag

logger = structlog.get_logger(__name__)

# Configuration — band thresholds mirror the spec's Q90 structure
# (energyexe_pipeline_full.py :132-139) and add a parallel Q50 path.
Q90_RATIO_BANDS: List[Dict[str, float]] = [
    {"wind_min": 7.0, "wind_max": 10.0, "threshold": 0.70},
    {"wind_min": 10.0, "wind_max": 25.0, "threshold": 0.80},
]

# B1.5 — catches median-suppression patterns (half-BMU offline) that
# Q90-only checks miss. Thresholds derived from P-1.3 data: EAO ~0.80,
# Hornsea ~0.86; 0.85 captures both with margin for moderately
# constrained farms.
Q50_RATIO_BANDS: List[Dict[str, float]] = [
    {"wind_min": 7.0, "wind_max": 10.0, "threshold": 0.70},
    {"wind_min": 10.0, "wind_max": 25.0, "threshold": 0.85},
]

CONSTRAINT_MIN_HOURS = 336  # ~14 days; ignore short blips

# Run-grouping at calendar-month granularity. A month is "constrained"
# if >= CONSTRAINED_MONTH_THRESHOLD of its in-band hours fall in a
# flagged (bin, month) pair. Hours below IN_BAND_WIND_MIN_MPS are
# excluded from both numerator and denominator (they cannot be flagged
# and the spec's bands start at 7 m/s; including them inflates the
# denominator and hides real constraints).
#
# Why month-grouping: the spec's per-hour run grouping is shattered by
# normal wind variability — interleaved sub-7 m/s hours flip the
# constraint state ~5-10x per day and fragment what should be a
# sustained 7-month constraint into tiny sub-runs (verified on EAO 2024:
# 3,387 constrained hours -> 320 runs, max length 133 h, all below the
# 336 h threshold). Aggregating at month granularity matches how
# infrastructure constraints actually present and what an analyst would
# call out.
CONSTRAINED_MONTH_THRESHOLD = 0.25
IN_BAND_WIND_MIN_MPS = 7.0
MIN_IN_BAND_HOURS_PER_MONTH = 24  # don't flag a month from a handful of in-band hours


# ─── Pure helpers (testable, no DB) ────────────────────────────


def _bin_centre(b: Any) -> float:
    """Robustly return the centre of a wind bin regardless of its dtype.

    Accepts pandas Interval, plain floats (treated as left edge), or NaN.
    """
    if hasattr(b, "left") and hasattr(b, "right"):
        return (float(b.left) + float(b.right)) / 2.0
    try:
        if pd.isna(b):
            return float("nan")
    except (TypeError, ValueError):
        pass
    try:
        return float(b) + 0.5
    except (TypeError, ValueError):
        return float("nan")


def compute_loyo_reference(
    df_curve: pd.DataFrame, *, percentile: float, time_col: str = "hour"
) -> pd.DataFrame:
    """Build a leave-one-year-out percentile reference per wind bin.

    Returns a long DataFrame indexed by (wind_bin, _year) with the
    reference value computed from all years OTHER than the one under
    test (spec :436-443). One reference row per (bin, year) so a merge
    can map each bin-month back to its year-specific baseline.
    """
    if df_curve.empty:
        return pd.DataFrame(columns=["wind_bin", "_year", "ref_value"])

    df = df_curve.copy()
    df["_year"] = pd.to_datetime(df[time_col]).dt.year
    years = sorted(df["_year"].unique())
    if len(years) < 2:
        return pd.DataFrame(columns=["wind_bin", "_year", "ref_value"])

    rows = []
    for yr in years:
        ref_df = df[df["_year"] != yr]
        ref = (
            ref_df.groupby("wind_bin", observed=True)["p_pu"]
            .quantile(percentile)
            .reset_index()
            .rename(columns={"p_pu": "ref_value"})
        )
        ref["_year"] = yr
        rows.append(ref)

    return pd.concat(rows, ignore_index=True)


def compute_observed_percentile(
    df_curve: pd.DataFrame,
    *,
    percentile: float,
    time_col: str = "hour",
) -> pd.DataFrame:
    """Observed percentile per (wind_bin, calendar month, year).

    Spec :451-454.
    """
    if df_curve.empty:
        return pd.DataFrame(columns=["wind_bin", "_month", "_year", "obs_value"])

    df = df_curve.copy()
    df["_year"] = pd.to_datetime(df[time_col]).dt.year
    df["_month"] = pd.to_datetime(df[time_col]).dt.to_period("M")
    return (
        df.groupby(["wind_bin", "_month", "_year"], observed=True)["p_pu"]
        .quantile(percentile)
        .reset_index()
        .rename(columns={"p_pu": "obs_value"})
    )


def flag_bin_months(
    observed: pd.DataFrame,
    reference: pd.DataFrame,
    bands: List[Dict[str, float]],
) -> pd.DataFrame:
    """Merge observed vs reference and flag bin-months below the band threshold.

    Returns a DataFrame with columns ``wind_bin, _month`` for every flagged
    bin-month. Empty DataFrame when nothing fires.
    """
    if observed.empty or reference.empty:
        return pd.DataFrame(columns=["wind_bin", "_month"])

    merged = observed.merge(reference, on=["wind_bin", "_year"], how="left")
    merged = merged.dropna(subset=["ref_value"])
    merged = merged[merged["ref_value"] > 0].copy()
    if merged.empty:
        return pd.DataFrame(columns=["wind_bin", "_month"])

    merged["ratio"] = merged["obs_value"] / merged["ref_value"]
    # Categorical-safe v_center (spec patch from P-1.1)
    merged["v_center"] = merged["wind_bin"].astype(object).apply(_bin_centre).astype(float)

    flagged_mask = pd.Series(False, index=merged.index)
    for band in bands:
        in_band = merged["v_center"].between(band["wind_min"], band["wind_max"], inclusive="left")
        below = merged["ratio"] < band["threshold"]
        flagged_mask |= in_band & below

    return merged.loc[flagged_mask, ["wind_bin", "_month"]].drop_duplicates()


def group_into_runs(
    df_curve: pd.DataFrame,
    flagged_q90: pd.DataFrame,
    flagged_q50: pd.DataFrame,
    *,
    min_hours: int = CONSTRAINT_MIN_HOURS,
    time_col: str = "hour",
    constrained_month_threshold: float = CONSTRAINED_MONTH_THRESHOLD,
    in_band_wind_min_mps: float = IN_BAND_WIND_MIN_MPS,
    min_in_band_hours_per_month: int = MIN_IN_BAND_HOURS_PER_MONTH,
) -> pd.DataFrame:
    """Group flagged hours into runs at calendar-month granularity.

    Algorithm:
      1. Restrict to in-band hours (``wind_bin >= in_band_wind_min_mps``).
         Hours below the lowest detection band cannot be flagged and
         break the spec's hour-level run grouping on real data.
      2. Compute per-month ``flagged_share = flagged_hrs / in_band_hrs``.
         A month is "constrained" when ``flagged_share`` meets the
         threshold AND has at least ``min_in_band_hours_per_month`` of
         in-band data (avoids spurious flags from sparse months).
      3. Group consecutive constrained months into runs.
      4. Aggregate per-run stats (using only flagged in-band hours
         within the run's calendar range, matching spec semantics where
         ``duration_hours`` = count of constrained hours).
      5. Filter by ``min_hours``.

    A bin-month is considered constrained when EITHER the Q90 OR Q50
    detector flagged it (B1.5). The ``flag_trigger`` column records
    which path dominated: ``'q90_ratio' | 'q50_ratio' | 'both'``.
    """
    if df_curve.empty:
        return pd.DataFrame()

    df = df_curve.copy()
    df["_month"] = pd.to_datetime(df[time_col]).dt.to_period("M")

    q90_keys = (
        set(zip(flagged_q90["wind_bin"], flagged_q90["_month"])) if not flagged_q90.empty else set()
    )
    q50_keys = (
        set(zip(flagged_q50["wind_bin"], flagged_q50["_month"])) if not flagged_q50.empty else set()
    )
    all_keys = q90_keys | q50_keys
    if not all_keys:
        return pd.DataFrame()

    df["_key"] = list(zip(df["wind_bin"], df["_month"]))
    df["_in_flagged_bin_month"] = df["_key"].isin(all_keys)
    df["_q90_only"] = df["_key"].isin(q90_keys - q50_keys)
    df["_q50_only"] = df["_key"].isin(q50_keys - q90_keys)
    df["_both"] = df["_key"].isin(q90_keys & q50_keys)

    # In-band hours only (denominator for flagged_share + the run source)
    df_band = df[df["wind_bin"] >= in_band_wind_min_mps].copy()
    if df_band.empty:
        return pd.DataFrame()

    # Per-month aggregation
    monthly = (
        df_band.groupby("_month")
        .agg(
            in_band_hours=("_key", "size"),
            flagged_hours=("_in_flagged_bin_month", "sum"),
        )
        .reset_index()
    )
    monthly["flagged_share"] = monthly["flagged_hours"] / monthly["in_band_hours"]
    monthly["_constrained_month"] = (
        (monthly["flagged_share"] >= constrained_month_threshold)
        & (monthly["in_band_hours"] >= min_in_band_hours_per_month)
    )

    if not monthly["_constrained_month"].any():
        return pd.DataFrame()

    # Group consecutive constrained months into runs (month-level)
    monthly = monthly.sort_values("_month").reset_index(drop=True)
    monthly["_run_id"] = (
        monthly["_constrained_month"] != monthly["_constrained_month"].shift()
    ).cumsum()

    runs_meta = (
        monthly[monthly["_constrained_month"]]
        .groupby("_run_id")
        .agg(
            month_start=("_month", "min"),
            month_end=("_month", "max"),
        )
        .reset_index(drop=True)
    )

    rows = []
    for _, run in runs_meta.iterrows():
        m_start, m_end = run["month_start"], run["month_end"]
        in_run = df_band[(df_band["_month"] >= m_start) & (df_band["_month"] <= m_end)]
        flagged_in_run = in_run[in_run["_in_flagged_bin_month"]]
        if flagged_in_run.empty:
            continue

        q90_only = int(flagged_in_run["_q90_only"].sum())
        q50_only = int(flagged_in_run["_q50_only"].sum())
        both = int(flagged_in_run["_both"].sum())
        if both > 0 and q90_only == 0 and q50_only == 0:
            trigger = "both"
        elif q50_only > q90_only and both == 0:
            trigger = "q50_ratio"
        elif q90_only > q50_only and both == 0:
            trigger = "q90_ratio"
        else:
            trigger = "both"

        rows.append(
            {
                "period_start": flagged_in_run[time_col].min(),
                "period_end": flagged_in_run[time_col].max(),
                "duration_hours": int(len(flagged_in_run)),
                "wind_bins_affected": int(flagged_in_run["wind_bin"].nunique()),
                "mean_q90_ratio": float(flagged_in_run["p_pu"].quantile(0.90)),
                "mean_q50_ratio": float(flagged_in_run["p_pu"].quantile(0.50)),
                "flag_trigger": trigger,
            }
        )

    if not rows:
        return pd.DataFrame()

    runs = pd.DataFrame(rows)
    runs = runs[runs["duration_hours"] >= min_hours].reset_index(drop=True)
    return runs


def detect_constraints_df(
    df_curve: pd.DataFrame,
    *,
    min_hours: int = CONSTRAINT_MIN_HOURS,
    q90_bands: Optional[List[Dict[str, float]]] = None,
    q50_bands: Optional[List[Dict[str, float]]] = None,
    time_col: str = "hour",
) -> pd.DataFrame:
    """Run the full pipeline: LOYO references → observed → flag → group.

    Pure function; takes a DataFrame in, returns a DataFrame of runs out.
    """
    q90_bands = q90_bands if q90_bands is not None else Q90_RATIO_BANDS
    q50_bands = q50_bands if q50_bands is not None else Q50_RATIO_BANDS

    if df_curve.empty or "p_pu" not in df_curve.columns:
        return pd.DataFrame()

    if "wind_bin" not in df_curve.columns:
        df_curve = df_curve.copy()
        df_curve["wind_bin"] = np.floor(df_curve["wind_speed"]).astype(float)

    ref_q90 = compute_loyo_reference(df_curve, percentile=0.90, time_col=time_col)
    ref_q50 = compute_loyo_reference(df_curve, percentile=0.50, time_col=time_col)
    if ref_q90.empty and ref_q50.empty:
        logger.warning("module_1b_insufficient_years")
        return pd.DataFrame()

    obs_q90 = compute_observed_percentile(df_curve, percentile=0.90, time_col=time_col)
    obs_q50 = compute_observed_percentile(df_curve, percentile=0.50, time_col=time_col)

    flagged_q90 = flag_bin_months(obs_q90, ref_q90, q90_bands)
    flagged_q50 = flag_bin_months(obs_q50, ref_q50, q50_bands)

    return group_into_runs(
        df_curve, flagged_q90, flagged_q50, min_hours=min_hours, time_col=time_col
    )


# ─── Service (DB-bound) ────────────────────────────────────────


class StructuralConstraintDetectionService:
    """Persists detected constraint runs as ``pending_review`` flags."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def detect_constraints(
        self,
        windfarm_id: int,
        df_curve: pd.DataFrame,
        *,
        pipeline_run_id: Optional[int] = None,
        replace_existing: bool = True,
    ) -> Dict[str, Any]:
        """Detect and persist constraints for one windfarm.

        Args:
            df_curve: hourly DataFrame with columns ``hour, p_pu, wind_speed``
                (and optionally ``wind_bin``).
            replace_existing: if True, drop any prior auto-detected runs for
                this windfarm before re-inserting. Manually-confirmed rows
                (review_status != 'pending_review') are preserved.

        Returns a summary dict with ``runs_detected, total_constrained_hours``.
        """
        runs = detect_constraints_df(df_curve)
        runs_detected = len(runs)
        total_hours = int(runs["duration_hours"].sum()) if runs_detected else 0

        if replace_existing:
            await self.db.execute(
                delete(StructuralConstraintFlag).where(
                    StructuralConstraintFlag.windfarm_id == windfarm_id,
                    StructuralConstraintFlag.review_status == "pending_review",
                    StructuralConstraintFlag.flag_source == "auto_constraint_detector",
                )
            )

        for _, run in runs.iterrows():
            self.db.add(
                StructuralConstraintFlag(
                    windfarm_id=windfarm_id,
                    period_start=_ensure_tz(run["period_start"]),
                    period_end=_ensure_tz(run["period_end"]),
                    duration_hours=int(run["duration_hours"]),
                    wind_bins_affected=int(run["wind_bins_affected"]),
                    mean_q90_ratio=float(run["mean_q90_ratio"]),
                    mean_q50_ratio=float(run["mean_q50_ratio"]),
                    flag_trigger=str(run["flag_trigger"]),
                    flag_source="auto_constraint_detector",
                    review_status="pending_review",
                    pipeline_run_id=pipeline_run_id,
                )
            )

        return {
            "runs_detected": runs_detected,
            "total_constrained_hours": total_hours,
        }

    async def load_active_periods(self, windfarm_id: int) -> List[Dict[str, Any]]:
        """Return active constraint periods for a windfarm.

        "Active" = ``review_status IN ('pending_review', 'confirmed')``.
        Dismissed flags are excluded. Used by the orchestrator to mask out
        constrained hours from Modules 3/4/5 (FX2).
        """
        stmt = (
            select(
                StructuralConstraintFlag.period_start,
                StructuralConstraintFlag.period_end,
            )
            .where(StructuralConstraintFlag.windfarm_id == windfarm_id)
            .where(StructuralConstraintFlag.review_status.in_(("pending_review", "confirmed")))
        )
        rows = (await self.db.execute(stmt)).all()
        return [{"period_start": r.period_start, "period_end": r.period_end} for r in rows]


def build_constraint_mask(
    df: pd.DataFrame,
    periods: List[Dict[str, Any]],
    *,
    time_col: str = "hour",
) -> pd.Series:
    """Boolean Series aligned to ``df.index`` — True where the row's
    ``time_col`` falls inside any active constraint period.

    Periods are treated as closed-closed intervals to match how the
    detector groups runs (``period_end`` is the last constrained hour).
    """
    if df.empty or not periods:
        return pd.Series(False, index=df.index)

    ts = pd.to_datetime(df[time_col])
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("UTC")

    mask = pd.Series(False, index=df.index)
    for period in periods:
        start = pd.Timestamp(period["period_start"])
        end = pd.Timestamp(period["period_end"])
        if start.tz is None:
            start = start.tz_localize("UTC")
        if end.tz is None:
            end = end.tz_localize("UTC")
        mask |= (ts >= start) & (ts <= end)

    return mask


def _ensure_tz(ts: Any) -> datetime:
    """Coerce timestamps to UTC datetimes for storage."""
    dt = pd.Timestamp(ts).to_pydatetime()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt
