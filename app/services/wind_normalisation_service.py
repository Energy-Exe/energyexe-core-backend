"""Wind normalisation service — Module 4.

Removes inter-year wind resource variability from performance signal,
producing an operational performance index independent of how windy each year was.

Method: hourly norm_ratio = actual_mw / expected_mw (from power curve lookup).
Monthly/yearly indices are computed relative to the historical mean.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.performance_summary import PerformanceSummary
from app.models.power_curve_bin import PowerCurveBin
from app.models.windfarm import Windfarm

logger = structlog.get_logger(__name__)

NORM_WIND_MIN_MPS = 4.0  # Exclude low wind — too noisy


class WindNormalisationService:
    """Computes wind-normalised performance indices."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def compute_normalisation(
        self,
        windfarm_id: int,
        reference: str = "q50",
        pipeline_run_id: Optional[int] = None,
    ) -> dict:
        """Compute wind normalisation for a windfarm.

        Args:
            reference: 'q50' for P50 reference or 'q90' for P10 reference.

        Runs for all available years. Stores monthly/yearly indices to performance_summaries.
        """
        # Get rated capacity
        wf_result = await self.db.execute(
            select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        )
        rated_mw = wf_result.scalar_one_or_none()
        if not rated_mw:
            return {"error": "No rated capacity"}

        # Load overall_clean power curve as lookup
        curve_lookup = await self._load_curve_lookup(windfarm_id, reference)
        if not curve_lookup:
            return {"error": "No overall_clean power curve available"}

        # Load hourly data (all years)
        from app.services.power_curve_service import PowerCurveService

        pcs = PowerCurveService(self.db)
        df = await pcs._load_hourly_data(windfarm_id, None, None, float(rated_mw))
        if df.empty:
            return {"error": "No hourly data"}

        # Compute hourly ratios
        hourly = self.compute_hourly_ratios(df, curve_lookup, float(rated_mw))
        if hourly.empty:
            return {"error": "No qualifying hours for normalisation"}

        # Compute monthly and yearly indices
        monthly_idx, yearly_idx = self.compute_indices(hourly)

        # Store to performance_summaries
        await self._store_normalisation(
            windfarm_id, monthly_idx, yearly_idx, reference, pipeline_run_id
        )

        return {
            "reference": reference,
            "qualifying_hours": len(hourly),
            "months_computed": len(monthly_idx),
            "years_computed": len(yearly_idx),
        }

    # ─── Fast path: accept pre-loaded DataFrame ─────────────────

    async def compute_normalisation_from_df(
        self,
        windfarm_id: int,
        df: pd.DataFrame,
        rated_mw: float,
        reference: str = "q50",
        pipeline_run_id: Optional[int] = None,
    ) -> dict:
        """Compute normalisation using pre-loaded hourly DataFrame."""
        curve_lookup = await self._load_curve_lookup(windfarm_id, reference)
        if not curve_lookup:
            return {"error": "No overall_clean power curve available"}

        hourly = self.compute_hourly_ratios(df, curve_lookup, rated_mw)
        if hourly.empty:
            return {"error": "No qualifying hours for normalisation"}

        monthly_idx, yearly_idx = self.compute_indices(hourly)
        await self._store_normalisation(
            windfarm_id, monthly_idx, yearly_idx, reference, pipeline_run_id
        )

        return {
            "reference": reference,
            "qualifying_hours": len(hourly),
            "months_computed": len(monthly_idx),
            "years_computed": len(yearly_idx),
        }

    # ─── Pure computation (testable) ───────────────────────────

    @staticmethod
    def compute_hourly_ratios(
        df: pd.DataFrame,
        curve_lookup: Dict[float, float],
        rated_mw: float,
        min_wind: float = NORM_WIND_MIN_MPS,
    ) -> pd.DataFrame:
        """Compute norm_ratio = actual_mw / expected_mw per hour.

        Excludes: wind < min_wind, no curve value, expected_mw <= 0.
        """
        out = df.copy()

        # Assign wind bins (1.0 m/s)
        out["wind_bin"] = np.floor(out["wind_speed"]).astype(float)

        # Look up expected p_pu from curve
        out["expected_pu"] = out["wind_bin"].map(curve_lookup)

        # Convert to MW
        out["expected_mw"] = out["expected_pu"] * rated_mw
        out["actual_mw"] = out["generation_mwh"]  # hourly MWh ≈ MW for 1-hour periods

        # Filter
        valid = (
            (out["wind_speed"] >= min_wind)
            & out["expected_pu"].notna()
            & (out["expected_mw"] > 0)
            & out["actual_mw"].notna()
        )
        out = out[valid].copy()

        out["norm_ratio"] = out["actual_mw"] / out["expected_mw"]
        out["year"] = out["year"].astype(int)
        out["month"] = out["hour"].dt.month

        return out

    @staticmethod
    def compute_indices(
        hourly: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Compute monthly and yearly indices relative to historical mean.

        Monthly: hourly groupby (year, month).mean() of norm_ratio. Historical
        mean is the mean of those monthly values.

        Yearly: the mean of the monthly means (NOT a fresh hourly groupby) —
        this is what the reference pipeline does (`energyexe_pipeline_full.py
        :910-917`), and it avoids partial-month bias when a year is only
        partially covered. Yearly historical mean is recomputed from the yearly
        series — separate from the monthly historical mean.

        100 = long-run average performance. >100 = above average. <100 = below.
        """
        # Monthly aggregation
        monthly = hourly.groupby(["year", "month"], as_index=False).agg(
            avg_norm_ratio=("norm_ratio", "mean"),
            hours_used=("norm_ratio", "count"),
        )

        # Monthly historical mean and index
        monthly_hist = float(monthly["avg_norm_ratio"].mean()) if len(monthly) > 0 else 1.0
        if monthly_hist <= 0:
            monthly_hist = 1.0
        monthly["index_vs_base"] = monthly["avg_norm_ratio"] / monthly_hist * 100

        # Yearly = mean of monthly means + sum of hours_used.
        yearly = monthly.groupby("year", as_index=False).agg(
            avg_norm_ratio=("avg_norm_ratio", "mean"),
            hours_used=("hours_used", "sum"),
        )
        yearly_hist = float(yearly["avg_norm_ratio"].mean()) if len(yearly) > 0 else 1.0
        if yearly_hist <= 0:
            yearly_hist = 1.0
        yearly["index_vs_base"] = yearly["avg_norm_ratio"] / yearly_hist * 100

        return monthly, yearly

    # ─── Curve lookup ──────────────────────────────────────────

    async def _load_curve_lookup(self, windfarm_id: int, reference: str) -> Dict[float, float]:
        """Load overall_clean power curve as {wind_bin: q50/q90 p_pu}."""
        col = "q50_pu" if reference == "q50" else "q90_pu"

        result = await self.db.execute(
            select(PowerCurveBin).where(
                PowerCurveBin.windfarm_id == windfarm_id,
                PowerCurveBin.curve_type == "overall_clean",
            )
        )
        bins = result.scalars().all()
        lookup = {}
        for b in bins:
            val = (
                float(b.q50_pu)
                if reference == "q50" and b.q50_pu
                else float(b.q90_pu)
                if reference == "q90" and b.q90_pu
                else None
            )
            if val is not None:
                lookup[float(b.wind_bin)] = val
        return lookup

    # ─── Storage ───────────────────────────────────────────────

    async def _store_normalisation(
        self,
        windfarm_id: int,
        monthly: pd.DataFrame,
        yearly: pd.DataFrame,
        reference: str,
        pipeline_run_id: Optional[int] = None,
    ) -> None:
        """Bulk upsert normalisation columns in performance_summaries.

        Uses INSERT ... ON CONFLICT DO UPDATE for monthly rows (unique index exists)
        and raw SQL UPDATE for yearly rows (NULL month defeats ON CONFLICT).
        """
        # Map to model column names (q50 -> p50, q90 -> p10)
        if reference == "q50":
            ratio_col = "norm_ratio_p50"
            index_col = "norm_index_p50"
        else:
            ratio_col = "norm_ratio_p10"
            index_col = "norm_index_p10"

        # Monthly rows — bulk upsert
        monthly_rows = [
            {
                "windfarm_id": windfarm_id,
                "year": int(r["year"]),
                "month": int(r["month"]),
                "ratio": round(float(r["avg_norm_ratio"]), 5),
                "idx": round(float(r["index_vs_base"]), 3),
                "pipeline_run_id": pipeline_run_id,
            }
            for _, r in monthly.iterrows()
        ]

        if monthly_rows:
            await self.db.execute(
                text(
                    f"""
                    INSERT INTO performance_summaries
                      (windfarm_id, period_type, year, month, {ratio_col}, {index_col}, pipeline_run_id)
                    VALUES
                      (:windfarm_id, 'month', :year, :month, :ratio, :idx, :pipeline_run_id)
                    ON CONFLICT (windfarm_id, period_type, year, month) DO UPDATE SET
                      {ratio_col} = EXCLUDED.{ratio_col},
                      {index_col} = EXCLUDED.{index_col},
                      pipeline_run_id = COALESCE(EXCLUDED.pipeline_run_id, performance_summaries.pipeline_run_id),
                      updated_at = NOW()
                """
                ),
                monthly_rows,
            )

        # Yearly rows — try UPDATE first (anomaly service usually creates the yearly row),
        # fall back to INSERT if missing. Batched via single UPDATE + single INSERT.
        yearly_rows = [
            {
                "windfarm_id": windfarm_id,
                "year": int(r["year"]),
                "ratio": round(float(r["avg_norm_ratio"]), 5),
                "idx": round(float(r["index_vs_base"]), 3),
                "pipeline_run_id": pipeline_run_id,
            }
            for _, r in yearly.iterrows()
        ]

        for row in yearly_rows:
            updated = await self.db.execute(
                text(
                    f"""
                    UPDATE performance_summaries
                    SET {ratio_col} = :ratio,
                        {index_col} = :idx,
                        pipeline_run_id = COALESCE(:pipeline_run_id, pipeline_run_id),
                        updated_at = NOW()
                    WHERE windfarm_id = :windfarm_id
                      AND period_type = 'year'
                      AND year = :year
                      AND month IS NULL
                """
                ),
                row,
            )
            if updated.rowcount == 0:
                await self.db.execute(
                    text(
                        f"""
                        INSERT INTO performance_summaries
                          (windfarm_id, period_type, year, month, {ratio_col}, {index_col}, pipeline_run_id)
                        VALUES
                          (:windfarm_id, 'year', :year, NULL, :ratio, :idx, :pipeline_run_id)
                    """
                    ),
                    row,
                )
