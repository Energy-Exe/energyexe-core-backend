"""Constraint loss service — Module 3f.

Prices the infrastructure-driven energy/revenue loss for each CONFIRMED
structural-constraint period against the pooled ``overall_clean`` Q50
capability curve, and stores one row per period in
``constraint_loss_summaries`` (issue #82).

Constrained hours are masked out of the normal Module 3 ODI accounting
(issues #79/#81), so this is where their loss is attributed — mirroring the
reference pipeline's ``constraint_loss_summary.csv``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.constraint_loss_summary import ConstraintLossSummary
from app.models.power_curve_bin import PowerCurveBin
from app.models.structural_constraint_flag import StructuralConstraintFlag

logger = structlog.get_logger(__name__)


def _to_ts(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tz is None else ts


class ConstraintLossService:
    """Computes + persists per-period infrastructure loss vs overall_clean Q50."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ─── Pure computation (testable, no DB) ────────────────────

    @staticmethod
    def compute_period_loss(
        period_df: pd.DataFrame,
        overall_clean_q50: Dict[float, float],
        rated_mw: float,
        *,
        ppa_price: Optional[float] = None,
    ) -> Optional[Dict[str, float]]:
        """Loss for the hours of one constraint period vs overall_clean Q50.

        ``period_df`` is the hourly slice inside the period (columns ``hour,
        wind_speed, generation_mwh, market_price``). Expected output per hour is
        ``overall_clean Q50[bin] * rated_mw``; lost = ``max(0, expected -
        actual)`` per hour (matching the spec). Hours whose wind bin has no
        overall_clean value are skipped (no reference to price against).

        Returns None when no hour maps to a curve bin.
        """
        if period_df.empty:
            return None

        df = period_df.copy()
        df["wind_bin"] = np.floor(df["wind_speed"]).astype(float)
        df["expected_pu"] = df["wind_bin"].map(overall_clean_q50)
        df = df[df["expected_pu"].notna()].copy()
        if df.empty:
            return None

        df["expected_mwh"] = df["expected_pu"].astype(float) * rated_mw
        df["actual_mwh"] = df["generation_mwh"].astype(float)
        df["lost_mwh"] = (df["expected_mwh"] - df["actual_mwh"]).clip(lower=0.0)

        if ppa_price is not None:
            price = pd.Series(float(ppa_price), index=df.index)
        else:
            price = pd.to_numeric(df.get("market_price"), errors="coerce")
            if price.notna().any():
                price = price.fillna(price.mean())
            else:
                price = None

        lost_eur = float((df["lost_mwh"] * price).sum()) if price is not None else None

        return {
            "duration_hours": int(len(df)),
            "actual_mwh": round(float(df["actual_mwh"].sum()), 2),
            "expected_mwh": round(float(df["expected_mwh"].sum()), 2),
            "lost_mwh": round(float(df["lost_mwh"].sum()), 2),
            "lost_eur": round(lost_eur, 2) if lost_eur is not None else None,
        }

    # ─── DB orchestration ──────────────────────────────────────

    async def compute_and_store(
        self,
        windfarm_id: int,
        df_all: pd.DataFrame,
        rated_mw: float,
        *,
        ppa_price: Optional[float] = None,
        pipeline_run_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Compute + persist loss rows for every CONFIRMED constraint period.

        Idempotent: replaces any prior rows for this windfarm. Returns a summary
        dict ``{periods, total_lost_mwh, total_lost_eur}``.
        """
        flags = (
            await self.db.execute(
                select(
                    StructuralConstraintFlag.period_start,
                    StructuralConstraintFlag.period_end,
                    StructuralConstraintFlag.mean_q90_ratio,
                )
                .where(StructuralConstraintFlag.windfarm_id == windfarm_id)
                .where(StructuralConstraintFlag.review_status == "confirmed")
            )
        ).all()

        # Idempotent rebuild.
        await self.db.execute(
            delete(ConstraintLossSummary).where(ConstraintLossSummary.windfarm_id == windfarm_id)
        )

        if not flags or df_all.empty:
            return {"periods": 0, "total_lost_mwh": 0.0, "total_lost_eur": 0.0}

        overall_clean_q50 = await self._load_overall_clean_q50(windfarm_id)
        if not overall_clean_q50:
            logger.warning("constraint_loss_no_overall_clean", windfarm_id=windfarm_id)
            return {"periods": 0, "total_lost_mwh": 0.0, "total_lost_eur": 0.0}

        ts = pd.to_datetime(df_all["hour"])
        ts = ts.dt.tz_localize("UTC") if ts.dt.tz is None else ts

        rows_stored = 0
        total_lost_mwh = 0.0
        total_lost_eur = 0.0
        for f in flags:
            start, end = _to_ts(f.period_start), _to_ts(f.period_end)
            period_df = df_all[(ts >= start) & (ts <= end)]
            loss = self.compute_period_loss(
                period_df, overall_clean_q50, rated_mw, ppa_price=ppa_price
            )
            if loss is None:
                continue
            self.db.add(
                ConstraintLossSummary(
                    windfarm_id=windfarm_id,
                    period_start=start.to_pydatetime(),
                    period_end=end.to_pydatetime(),
                    duration_hours=loss["duration_hours"],
                    actual_mwh=loss["actual_mwh"],
                    expected_mwh=loss["expected_mwh"],
                    lost_mwh=loss["lost_mwh"],
                    lost_eur=loss["lost_eur"],
                    mean_q90_ratio=(
                        float(f.mean_q90_ratio) if f.mean_q90_ratio is not None else None
                    ),
                    reference_curve="overall_clean_q50",
                    pipeline_run_id=pipeline_run_id,
                )
            )
            rows_stored += 1
            total_lost_mwh += loss["lost_mwh"]
            total_lost_eur += loss["lost_eur"] or 0.0

        return {
            "periods": rows_stored,
            "total_lost_mwh": round(total_lost_mwh, 2),
            "total_lost_eur": round(total_lost_eur, 2),
        }

    async def _load_overall_clean_q50(self, windfarm_id: int) -> Dict[float, float]:
        rows = (
            await self.db.execute(
                select(PowerCurveBin.wind_bin, PowerCurveBin.q50_pu).where(
                    PowerCurveBin.windfarm_id == windfarm_id,
                    PowerCurveBin.curve_type == "overall_clean",
                    PowerCurveBin.year.is_(None),
                )
            )
        ).all()
        return {float(r.wind_bin): float(r.q50_pu) for r in rows if r.q50_pu is not None}
