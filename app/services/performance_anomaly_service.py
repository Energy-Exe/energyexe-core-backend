"""Performance anomaly service — Module 3: Anomaly Detection & Loss Quantification.

Identifies hours of operational underperformance and overperformance against
the empirical power curve, quantifies lost energy (MWh) and revenue (EUR),
computes ODI metrics, and groups consecutive underperformance into runs.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.performance_anomaly import PerformanceAnomaly
from app.models.performance_summary import PerformanceSummary
from app.models.power_curve_bin import PowerCurveBin
from app.models.windfarm import Windfarm
from app.models.ppa import PPA

logger = structlog.get_logger(__name__)

# ─── Thresholds from PDF spec ─────────────────────────────────
UNDERPERF_MAD_K = 2.5
OVERPERF_MAD_K = 1.5
CEILING_PU = 1.02
LONG_RUN_HOURS = 24


class PerformanceAnomalyService:
    """Detects performance anomalies and quantifies losses."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ─── Main entry ────────────────────────────────────────────

    async def detect_anomalies(
        self, windfarm_id: int, year: int, pipeline_run_id: Optional[int] = None
    ) -> dict:
        """Detect anomalies for a windfarm in a given year.

        Requires power curves to be built first (Module 2).
        """
        # Get rated capacity and PPA price
        wf_result = await self.db.execute(
            select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        )
        rated_mw = wf_result.scalar_one_or_none()
        if not rated_mw:
            return {"error": "No rated capacity"}

        ppa_price = await self._get_ppa_price(windfarm_id)

        # Load capability curve for this year
        capability = await self._load_capability_stats(windfarm_id, year)
        if capability.empty:
            return {"error": f"No capability curve for year {year}"}

        # Load hourly data
        df = await self._load_hourly_data(windfarm_id, year)
        if df.empty:
            return {"error": "No hourly data"}

        # Classify hours
        df_flagged = self.classify_hours(df, capability, float(rated_mw), ppa_price)

        # Assign run IDs to underperformance
        df_flagged = self.assign_run_ids(df_flagged)

        # Store anomalies (only flagged hours)
        anomalies = df_flagged[df_flagged["is_anomaly"]].copy()
        await self._store_anomalies(windfarm_id, year, anomalies)

        # Aggregate and store summaries
        monthly, yearly = self.aggregate_summaries(df_flagged, year)
        await self._store_summaries(windfarm_id, year, monthly, yearly, pipeline_run_id)

        return {
            "year": year,
            "total_hours": len(df_flagged),
            "underperf_hours": int((df_flagged["anomaly_type"] == "underperformance").sum()),
            "overperf_hours": int((df_flagged["anomaly_type"] == "overperformance").sum()),
            "lost_mwh": float(df_flagged["lost_mwh"].sum()),
            "lost_eur": float(df_flagged["lost_eur"].sum()),
        }

    # ─── Classification (pure, testable) ───────────────────────

    @staticmethod
    def classify_hours(
        df: pd.DataFrame,
        capability_stats: pd.DataFrame,
        rated_mw: float,
        ppa_price: Optional[float] = None,
    ) -> pd.DataFrame:
        """Classify each hour as underperformance, overperformance, or normal.

        Merges capability bin stats onto hourly data, then applies thresholds:
        - Underperformance: p_pu < q50_bin - 2.5 * MAD
        - Overperformance: p_pu > q90_bin + 1.5 * MAD or p_pu > 1.02
        - Lost MWh = max(0, q50_bin * rated_mw - actual_mwh)
        - Lost EUR = lost_mwh * price
        """
        out = df.copy()

        # Merge capability stats by wind bin
        bins = np.arange(2.0, 26.0, 1.0)
        out["wind_bin_interval"] = pd.cut(out["wind_speed"], bins=bins, right=False, include_lowest=True)

        # Map capability stats to the same intervals
        cap = capability_stats.copy()
        cap_map = {}
        for _, row in cap.iterrows():
            bin_iv = row.get("wind_bin")
            if pd.notna(bin_iv):
                cap_map[bin_iv] = {
                    "q50_bin": row.get("q50_pu"),
                    "q90_bin": row.get("q90_pu"),
                    "mad_bin": row.get("mad_pu"),
                }

        # Apply bin stats
        out["q50_bin"] = out["wind_bin_interval"].map(lambda iv: cap_map.get(iv, {}).get("q50_bin"))
        out["q90_bin"] = out["wind_bin_interval"].map(lambda iv: cap_map.get(iv, {}).get("q90_bin"))
        out["mad_bin"] = out["wind_bin_interval"].map(lambda iv: cap_map.get(iv, {}).get("mad_bin"))

        # Only classify where we have curve stats
        has_stats = out["q50_bin"].notna() & out["mad_bin"].notna()

        # Underperformance
        underperf = has_stats & (out["p_pu"] < (out["q50_bin"] - UNDERPERF_MAD_K * out["mad_bin"]))

        # Overperformance
        overperf = has_stats & (
            (out["p_pu"] > (out["q90_bin"] + OVERPERF_MAD_K * out["mad_bin"]))
            | (out["p_pu"] > CEILING_PU)
        )

        out["anomaly_type"] = None
        out.loc[underperf, "anomaly_type"] = "underperformance"
        out.loc[overperf, "anomaly_type"] = "overperformance"
        out["is_anomaly"] = underperf | overperf

        # Loss quantification (underperformance only)
        out["expected_mwh"] = out["q50_bin"] * rated_mw
        out["lost_mwh"] = np.where(
            underperf,
            np.maximum(0, out["expected_mwh"] - out["generation_mwh"]),
            0.0,
        )

        price = ppa_price if ppa_price else out.get("market_price", 0)
        out["lost_eur"] = np.where(
            underperf,
            out["lost_mwh"] * price,
            0.0,
        )

        # Wind bin as float for storage
        out["wind_bin_float"] = out["wind_bin_interval"].apply(
            lambda iv: float(iv.left) if pd.notna(iv) else np.nan
        )

        return out

    # ─── Run grouping (pure, testable) ─────────────────────────

    @staticmethod
    def assign_run_ids(df: pd.DataFrame) -> pd.DataFrame:
        """Group consecutive underperformance hours into runs (vectorized)."""
        out = df.copy().sort_values("hour").reset_index(drop=True)
        out["run_id"] = None

        under_mask = out["anomaly_type"] == "underperformance"
        if not under_mask.any():
            return out

        # Compute gap between consecutive underperf hours; gap > 1h = new run.
        under = out[under_mask]
        gap_sec = under["hour"].diff().dt.total_seconds()
        new_run = gap_sec.isna() | (gap_sec > 3600)
        run_ids = new_run.cumsum() - 1  # 0-indexed
        out.loc[under.index, "run_id"] = run_ids.values
        return out

    # ─── Aggregation (pure, testable) ──────────────────────────

    @staticmethod
    def aggregate_summaries(
        df_flagged: pd.DataFrame, year: int
    ) -> tuple[List[dict], dict]:
        """Aggregate ODI metrics monthly and yearly.

        Returns (monthly_list, yearly_dict).
        """
        def _agg_group(group: pd.DataFrame) -> dict:
            total = len(group)
            underperf = (group["anomaly_type"] == "underperformance").sum()
            overperf = (group["anomaly_type"] == "overperformance").sum()
            lost_mwh = float(group["lost_mwh"].sum())
            expected_mwh = float(group["expected_mwh"].sum()) if "expected_mwh" in group.columns else 0
            lost_eur = float(group["lost_eur"].sum())
            # Expected revenue = expected_mwh * avg market price
            avg_price = group["market_price"].mean() if "market_price" in group.columns else 0
            expected_rev = expected_mwh * float(avg_price) if pd.notna(avg_price) and avg_price > 0 else 0

            # Long runs
            long_runs = 0
            max_run = 0
            if "run_id" in group.columns:
                underperf_runs = group[group["anomaly_type"] == "underperformance"]
                if not underperf_runs.empty and underperf_runs["run_id"].notna().any():
                    run_sizes = underperf_runs.groupby("run_id").size()
                    long_runs = int((run_sizes >= LONG_RUN_HOURS).sum())
                    max_run = int(run_sizes.max()) if len(run_sizes) > 0 else 0

            return {
                "total_hours": int(total),
                "underperf_hours": int(underperf),
                "overperf_hours": int(overperf),
                "odi_pct_underperf": round(underperf / max(total, 1) * 100, 3),
                "lost_mwh": round(lost_mwh, 3),
                "expected_mwh": round(expected_mwh, 3),
                "odi_pct_loss_mwh": round(lost_mwh / max(expected_mwh, 1) * 100, 3) if expected_mwh > 0 else 0,
                "lost_eur": round(lost_eur, 2),
                "expected_revenue_eur": round(expected_rev, 2),
                "odi_pct_loss_eur": round(lost_eur / max(expected_rev, 1) * 100, 3) if expected_rev > 0 else 0,
                "long_run_count": long_runs,
                "max_run_hours": max_run,
            }

        # Monthly
        df_flagged = df_flagged.copy()
        df_flagged["month"] = df_flagged["hour"].dt.month
        monthly = []
        for month, group in df_flagged.groupby("month"):
            agg = _agg_group(group)
            agg["year"] = year
            agg["month"] = int(month)
            monthly.append(agg)

        # Yearly
        yearly = _agg_group(df_flagged)
        yearly["year"] = year
        yearly["month"] = None

        return monthly, yearly

    # ─── Data loading ──────────────────────────────────────────

    async def _load_hourly_data(self, windfarm_id: int, year: int) -> pd.DataFrame:
        """Load hourly data for a specific year. Delegates to PowerCurveService for
        consistent three-table pandas-merge strategy."""
        from app.services.power_curve_service import PowerCurveService
        # Get rated capacity
        wf_result = await self.db.execute(
            select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        )
        rated_mw = wf_result.scalar_one_or_none()
        if not rated_mw:
            return pd.DataFrame()

        pcs = PowerCurveService(self.db)
        df = await pcs._load_hourly_data(windfarm_id, year, year, float(rated_mw))
        if df.empty:
            return df
        return df[["hour", "generation_mwh", "wind_speed", "market_price", "p_pu"]]

    async def _load_capability_stats(self, windfarm_id: int, year: int) -> pd.DataFrame:
        """Load yearly capability curve bins."""
        result = await self.db.execute(
            select(PowerCurveBin).where(
                PowerCurveBin.windfarm_id == windfarm_id,
                PowerCurveBin.year == year,
                PowerCurveBin.curve_type == "capability",
            ).order_by(PowerCurveBin.wind_bin)
        )
        bins = result.scalars().all()
        if not bins:
            return pd.DataFrame()

        # Reconstruct pd.Interval objects for matching
        records = []
        for b in bins:
            left = float(b.wind_bin)
            right = left + 1.0
            records.append({
                "wind_bin": pd.Interval(left, right, closed="left"),
                "q50_pu": float(b.q50_pu) if b.q50_pu else None,
                "q90_pu": float(b.q90_pu) if b.q90_pu else None,
                "mad_pu": float(b.mad_pu) if b.mad_pu else None,
                "sample_count": b.sample_count,
            })
        return pd.DataFrame(records)

    async def _get_ppa_price(self, windfarm_id: int) -> Optional[float]:
        """Get PPA price for loss calculations."""
        result = await self.db.execute(
            select(PPA.ppa_price_eur_mwh).where(
                PPA.windfarm_id == windfarm_id,
                PPA.ppa_status == "active",
            ).order_by(PPA.ppa_end_date.desc().nullslast()).limit(1)
        )
        val = result.scalar_one_or_none()
        return float(val) if val else None

    # ─── Storage ───────────────────────────────────────────────

    async def _store_anomalies(self, windfarm_id: int, year: int, anomalies: pd.DataFrame) -> None:
        """Delete existing anomalies for this year and insert new."""
        # Delete existing
        await self.db.execute(
            text("""
                DELETE FROM performance_anomalies
                WHERE windfarm_id = :wf_id
                  AND EXTRACT(YEAR FROM hour) = :year
            """),
            {"wf_id": windfarm_id, "year": int(year)},
        )

        for _, row in anomalies.iterrows():
            pa = PerformanceAnomaly(
                windfarm_id=windfarm_id,
                hour=row["hour"],
                anomaly_type=row["anomaly_type"],
                actual_p_pu=float(row["p_pu"]) if pd.notna(row.get("p_pu")) else None,
                expected_p_pu=float(row["q50_bin"]) if pd.notna(row.get("q50_bin")) else None,
                wind_speed=float(row["wind_speed"]) if pd.notna(row.get("wind_speed")) else None,
                wind_bin=float(row["wind_bin_float"]) if pd.notna(row.get("wind_bin_float")) else None,
                lost_mwh=float(row["lost_mwh"]) if pd.notna(row.get("lost_mwh")) else None,
                lost_eur=float(row["lost_eur"]) if pd.notna(row.get("lost_eur")) else None,
                market_price=float(row["market_price"]) if pd.notna(row.get("market_price")) else None,
                run_id=int(row["run_id"]) if pd.notna(row.get("run_id")) else None,
            )
            self.db.add(pa)

    async def _store_summaries(
        self,
        windfarm_id: int,
        year: int,
        monthly: List[dict],
        yearly: dict,
        pipeline_run_id: Optional[int] = None,
    ) -> None:
        """Bulk upsert performance summaries (ODI columns only — other modules fill their columns).

        Uses a single INSERT ... ON CONFLICT DO UPDATE round trip instead of
        per-row SELECT-then-INSERT/UPDATE. The partial-index clause handles the
        NULL-vs-value distinction for `month` correctly via COALESCE in the conflict target.
        """
        rows = []
        for period in monthly + [yearly]:
            month = period.get("month")
            period_type = "month" if month else "year"
            rows.append({
                "windfarm_id": windfarm_id,
                "period_type": period_type,
                "year": year,
                "month": month,
                "total_hours": period["total_hours"],
                "underperf_hours": period["underperf_hours"],
                "overperf_hours": period["overperf_hours"],
                "odi_pct_underperf": period["odi_pct_underperf"],
                "lost_mwh": period["lost_mwh"],
                "expected_mwh": period["expected_mwh"],
                "odi_pct_loss_mwh": period["odi_pct_loss_mwh"],
                "lost_eur": period["lost_eur"],
                "expected_revenue_eur": period["expected_revenue_eur"],
                "odi_pct_loss_eur": period["odi_pct_loss_eur"],
                "long_run_count": period["long_run_count"],
                "max_run_hours": period["max_run_hours"],
                "pipeline_run_id": pipeline_run_id,
            })

        if not rows:
            return

        # ON CONFLICT on (windfarm_id, period_type, year, month) — requires NULLs-distinct
        # to be treated uniformly. Since PostgreSQL treats NULL as distinct in unique
        # indexes (pre-15 default), we split monthly and yearly upserts.
        monthly_rows = [r for r in rows if r["month"] is not None]
        yearly_rows = [r for r in rows if r["month"] is None]

        monthly_sql = text("""
            INSERT INTO performance_summaries
              (windfarm_id, period_type, year, month,
               total_hours, underperf_hours, overperf_hours, odi_pct_underperf,
               lost_mwh, expected_mwh, odi_pct_loss_mwh,
               lost_eur, expected_revenue_eur, odi_pct_loss_eur,
               long_run_count, max_run_hours, pipeline_run_id)
            VALUES
              (:windfarm_id, :period_type, :year, :month,
               :total_hours, :underperf_hours, :overperf_hours, :odi_pct_underperf,
               :lost_mwh, :expected_mwh, :odi_pct_loss_mwh,
               :lost_eur, :expected_revenue_eur, :odi_pct_loss_eur,
               :long_run_count, :max_run_hours, :pipeline_run_id)
            ON CONFLICT (windfarm_id, period_type, year, month) DO UPDATE SET
              total_hours = EXCLUDED.total_hours,
              underperf_hours = EXCLUDED.underperf_hours,
              overperf_hours = EXCLUDED.overperf_hours,
              odi_pct_underperf = EXCLUDED.odi_pct_underperf,
              lost_mwh = EXCLUDED.lost_mwh,
              expected_mwh = EXCLUDED.expected_mwh,
              odi_pct_loss_mwh = EXCLUDED.odi_pct_loss_mwh,
              lost_eur = EXCLUDED.lost_eur,
              expected_revenue_eur = EXCLUDED.expected_revenue_eur,
              odi_pct_loss_eur = EXCLUDED.odi_pct_loss_eur,
              long_run_count = EXCLUDED.long_run_count,
              max_run_hours = EXCLUDED.max_run_hours,
              pipeline_run_id = EXCLUDED.pipeline_run_id,
              updated_at = NOW()
        """)

        if monthly_rows:
            await self.db.execute(monthly_sql, monthly_rows)

        # Yearly case: PostgreSQL treats NULL as distinct in unique indexes by default,
        # so ON CONFLICT won't fire for `month IS NULL`. Delete-then-insert for idempotency.
        if yearly_rows:
            await self.db.execute(
                text("""
                    DELETE FROM performance_summaries
                    WHERE windfarm_id = :wf_id AND period_type = 'year'
                      AND year = :year AND month IS NULL
                """),
                {"wf_id": windfarm_id, "year": year},
            )
            await self.db.execute(
                text("""
                    INSERT INTO performance_summaries
                      (windfarm_id, period_type, year, month,
                       total_hours, underperf_hours, overperf_hours, odi_pct_underperf,
                       lost_mwh, expected_mwh, odi_pct_loss_mwh,
                       lost_eur, expected_revenue_eur, odi_pct_loss_eur,
                       long_run_count, max_run_hours, pipeline_run_id)
                    VALUES
                      (:windfarm_id, :period_type, :year, :month,
                       :total_hours, :underperf_hours, :overperf_hours, :odi_pct_underperf,
                       :lost_mwh, :expected_mwh, :odi_pct_loss_mwh,
                       :lost_eur, :expected_revenue_eur, :odi_pct_loss_eur,
                       :long_run_count, :max_run_hours, :pipeline_run_id)
                """),
                yearly_rows,
            )

    # ─── Fast path: accept pre-loaded DataFrame ─────────────────

    async def detect_anomalies_from_df(
        self, windfarm_id: int, year: int, df: pd.DataFrame,
        rated_mw: float, pipeline_run_id: Optional[int] = None,
    ) -> dict:
        """Detect anomalies using pre-loaded hourly DataFrame (avoids redundant SQL)."""
        ppa_price = await self._get_ppa_price(windfarm_id)

        capability = await self._load_capability_stats(windfarm_id, year)
        if capability.empty:
            return {"error": f"No capability curve for year {year}"}

        df_flagged = self.classify_hours(df, capability, rated_mw, ppa_price)
        df_flagged = self.assign_run_ids(df_flagged)

        anomalies = df_flagged[df_flagged["is_anomaly"]].copy()
        await self._store_anomalies_bulk(windfarm_id, year, anomalies)

        monthly, yearly = self.aggregate_summaries(df_flagged, year)
        await self._store_summaries(windfarm_id, year, monthly, yearly, pipeline_run_id)

        return {
            "year": year,
            "total_hours": len(df_flagged),
            "underperf_hours": int((df_flagged["anomaly_type"] == "underperformance").sum()),
            "overperf_hours": int((df_flagged["anomaly_type"] == "overperformance").sum()),
            "lost_mwh": float(df_flagged["lost_mwh"].sum()),
            "lost_eur": float(df_flagged["lost_eur"].sum()),
        }

    async def _store_anomalies_bulk(self, windfarm_id: int, year: int, anomalies: pd.DataFrame) -> None:
        """Bulk insert anomalies using raw SQL for speed."""
        # Delete existing
        await self.db.execute(
            text("""
                DELETE FROM performance_anomalies
                WHERE windfarm_id = :wf_id
                  AND EXTRACT(YEAR FROM hour) = :year
            """),
            {"wf_id": windfarm_id, "year": int(year)},
        )

        if anomalies.empty:
            return

        # Build values for bulk insert
        rows = []
        for _, row in anomalies.iterrows():
            rows.append({
                "windfarm_id": windfarm_id,
                "hour": row["hour"],
                "anomaly_type": row["anomaly_type"],
                "actual_p_pu": float(row["p_pu"]) if pd.notna(row.get("p_pu")) else None,
                "expected_p_pu": float(row["q50_bin"]) if pd.notna(row.get("q50_bin")) else None,
                "wind_speed": float(row["wind_speed"]) if pd.notna(row.get("wind_speed")) else None,
                "wind_bin": float(row["wind_bin_float"]) if pd.notna(row.get("wind_bin_float")) else None,
                "lost_mwh": float(row["lost_mwh"]) if pd.notna(row.get("lost_mwh")) else None,
                "lost_eur": float(row["lost_eur"]) if pd.notna(row.get("lost_eur")) else None,
                "market_price": float(row["market_price"]) if pd.notna(row.get("market_price")) else None,
                "run_id": int(row["run_id"]) if pd.notna(row.get("run_id")) else None,
            })

        # Batch insert in chunks of 1000
        for i in range(0, len(rows), 1000):
            chunk = rows[i:i + 1000]
            await self.db.execute(
                text("""
                    INSERT INTO performance_anomalies
                    (windfarm_id, hour, anomaly_type, actual_p_pu, expected_p_pu,
                     wind_speed, wind_bin, lost_mwh, lost_eur, market_price, run_id)
                    VALUES
                    (:windfarm_id, :hour, :anomaly_type, :actual_p_pu, :expected_p_pu,
                     :wind_speed, :wind_bin, :lost_mwh, :lost_eur, :market_price, :run_id)
                """),
                chunk,
            )

    # ─── Query helpers ─────────────────────────────────────────

    async def get_odi_metrics(
        self, windfarm_id: int, year: Optional[int] = None
    ) -> List[dict]:
        """Read ODI metrics from performance_summaries."""
        query = select(PerformanceSummary).where(
            PerformanceSummary.windfarm_id == windfarm_id,
        )
        if year:
            query = query.where(PerformanceSummary.year == year)
        query = query.order_by(PerformanceSummary.year, PerformanceSummary.month.nullslast())

        result = await self.db.execute(query)
        return [
            {
                "period_type": s.period_type,
                "year": s.year,
                "month": s.month,
                "odi_pct_underperf": float(s.odi_pct_underperf) if s.odi_pct_underperf else None,
                "odi_pct_loss_mwh": float(s.odi_pct_loss_mwh) if s.odi_pct_loss_mwh else None,
                "odi_pct_loss_eur": float(s.odi_pct_loss_eur) if s.odi_pct_loss_eur else None,
                "lost_mwh": float(s.lost_mwh) if s.lost_mwh else None,
                "lost_eur": float(s.lost_eur) if s.lost_eur else None,
                "total_hours": s.total_hours,
                "underperf_hours": s.underperf_hours,
                "long_run_count": s.long_run_count,
                "max_run_hours": s.max_run_hours,
            }
            for s in result.scalars().all()
        ]
