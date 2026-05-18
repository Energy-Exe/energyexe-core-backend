"""Power curve service — Modules 1 (data cleaning) and 2 (curve construction).

Builds empirical power curves from hourly wind speed + generation data.
Produces P50 (q50) and P10 (q90) capability curves per windfarm per year,
plus an overall clean curve used by downstream modules.

Computation uses pandas/numpy in-memory after pulling data via SQL.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.power_curve_bin import PowerCurveBin
from app.models.windfarm import Windfarm

logger = structlog.get_logger(__name__)

# ─── Configuration defaults ─────────────────────────────────────
WIND_MIN_ALLOWED = 0.0
WIND_MAX_ALLOWED = 40.0
P_PU_MIN_ALLOWED = -0.05
P_PU_MAX_ALLOWED = 1.20
WIND_MIN_FOR_CURVE = 2.0
WIND_MAX_FOR_CURVE = 25.0
BIN_WIDTH = 1.0
MIN_SAMPLES_PER_BIN = 30
OVERPERF_MAD_K = 1.5
CEILING_PU = 1.02


class PowerCurveService:
    """Builds and stores empirical power curves for wind farms."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ─── Main entry point ──────────────────────────────────────

    async def build_power_curves(
        self,
        windfarm_id: int,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        df_preloaded: Optional[pd.DataFrame] = None,
    ) -> dict:
        """Build and store all power curves for a windfarm.

        Args:
            df_preloaded: If provided, skip data loading (already loaded by orchestrator).

        Returns summary dict with curve counts and data quality stats.
        """
        if df_preloaded is not None:
            df = df_preloaded
        else:
            # Get rated capacity
            wf_result = await self.db.execute(
                select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
            )
            rated_mw = wf_result.scalar_one_or_none()
            if not rated_mw or rated_mw <= 0:
                logger.warning("power_curve_no_rated_mw", windfarm_id=windfarm_id)
                return {"error": "No rated capacity for windfarm"}

            # Load hourly data
            df = await self._load_hourly_data(windfarm_id, start_year, end_year, rated_mw)

        if df.empty:
            logger.warning("power_curve_no_data", windfarm_id=windfarm_id)
            return {"error": "No hourly data available"}

        # Module 1: Clean
        df_clean, df_curve = self.apply_hard_filters(df)
        if df_curve.empty:
            logger.warning("power_curve_no_data_after_filter", windfarm_id=windfarm_id)
            return {"error": "No data remaining after plausibility filters"}

        # Module 2: Build curves
        summary = await self._build_and_store_curves(windfarm_id, df_curve)

        summary["raw_rows"] = len(df)
        summary["clean_rows"] = len(df_clean)
        summary["curve_rows"] = len(df_curve)
        return summary

    # ─── Module 1: Data loading ────────────────────────────────

    async def _load_hourly_data(
        self,
        windfarm_id: int,
        start_year: Optional[int],
        end_year: Optional[int],
        rated_mw: float,
    ) -> pd.DataFrame:
        """Load and join generation + weather + price data. Compute p_pu.

        Performs three single-table aggregations in SQL (one per table) and
        merges them in pandas. This avoids pathological query planner estimates
        caused by 3-way nested subquery joins (observed >15min query times).
        Each sub-query uses a single windfarm_id index scan.
        """
        year_filter_gen = ""
        year_filter_wx = ""
        year_filter_px = ""
        gen_params: Dict[str, Any] = {"wf_id": windfarm_id}
        if start_year:
            year_filter_gen += " AND EXTRACT(YEAR FROM hour) >= :start_year"
            year_filter_wx += " AND EXTRACT(YEAR FROM hour) >= :start_year"
            year_filter_px += " AND EXTRACT(YEAR FROM hour) >= :start_year"
            gen_params["start_year"] = start_year
        if end_year:
            year_filter_gen += " AND EXTRACT(YEAR FROM hour) <= :end_year"
            year_filter_wx += " AND EXTRACT(YEAR FROM hour) <= :end_year"
            year_filter_px += " AND EXTRACT(YEAR FROM hour) <= :end_year"
            gen_params["end_year"] = end_year

        # 1) Generation — aggregate across units (the real multi-row case)
        gen_q = text(f"""
            SELECT hour,
                   SUM(generation_mwh) AS generation_mwh,
                   BOOL_OR(is_ramp_up) AS any_ramp_up
            FROM generation_data
            WHERE windfarm_id = :wf_id
              AND generation_mwh IS NOT NULL
              {year_filter_gen}
            GROUP BY hour
            HAVING BOOL_OR(is_ramp_up) = false
        """)
        gen_rows = (await self.db.execute(gen_q, gen_params)).fetchall()
        if not gen_rows:
            return pd.DataFrame()
        df_gen = pd.DataFrame(gen_rows, columns=["hour", "generation_mwh", "any_ramp_up"])
        df_gen["generation_mwh"] = df_gen["generation_mwh"].astype(float)

        # 2) Weather — AVG across any duplicates (usually 1 row per hour)
        wx_q = text(f"""
            SELECT hour, AVG(wind_speed_100m) AS wind_speed
            FROM weather_data
            WHERE windfarm_id = :wf_id
              AND wind_speed_100m IS NOT NULL
              {year_filter_wx}
            GROUP BY hour
        """)
        wx_rows = (await self.db.execute(wx_q, gen_params)).fetchall()
        df_wx = pd.DataFrame(wx_rows, columns=["hour", "wind_speed"]) if wx_rows else pd.DataFrame({"hour": pd.Series([], dtype="datetime64[ns, UTC]"), "wind_speed": pd.Series([], dtype="float64")})
        if not df_wx.empty:
            df_wx["wind_speed"] = df_wx["wind_speed"].astype(float)

        # 3) Price — AVG across sources (some farms have ENTSOE + ELEXON)
        px_q = text(f"""
            SELECT hour, AVG(day_ahead_price) AS market_price
            FROM price_data
            WHERE windfarm_id = :wf_id
              AND day_ahead_price IS NOT NULL
              {year_filter_px}
            GROUP BY hour
        """)
        px_rows = (await self.db.execute(px_q, gen_params)).fetchall()
        df_px = pd.DataFrame(px_rows, columns=["hour", "market_price"]) if px_rows else pd.DataFrame({"hour": pd.Series([], dtype="datetime64[ns, UTC]"), "market_price": pd.Series([], dtype="float64")})
        if not df_px.empty:
            df_px["market_price"] = pd.to_numeric(df_px["market_price"], errors="coerce")

        # Merge: inner-join gen+weather (both required), left-join price (optional)
        df = df_gen.merge(df_wx, on="hour", how="inner")
        df = df.merge(df_px, on="hour", how="left")

        if df.empty:
            return pd.DataFrame()

        df["year"] = pd.to_datetime(df["hour"]).dt.year.astype(int)
        df["p_pu"] = df["generation_mwh"] / float(rated_mw)
        # Defensive dedup (should be unnecessary after SQL GROUP BYs)
        df = df.drop_duplicates(subset=["hour"], keep="first").reset_index(drop=True)
        df = df.sort_values("hour").reset_index(drop=True)
        return df[["hour", "year", "generation_mwh", "wind_speed", "market_price", "p_pu"]]

    # ─── Module 1: Hard plausibility filters ───────────────────

    @staticmethod
    def apply_hard_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Apply hard plausibility filters.

        Returns:
            (df_clean, df_curve) — df_clean passes hard filters (full wind range),
            df_curve is the subset within the curve wind range (2-25 m/s).
        """
        mask = (
            df["wind_speed"].between(WIND_MIN_ALLOWED, WIND_MAX_ALLOWED)
            & df["p_pu"].between(P_PU_MIN_ALLOWED, P_PU_MAX_ALLOWED)
            & df["wind_speed"].notna()
            & df["p_pu"].notna()
        )
        df_clean = df.loc[mask].copy()

        curve_mask = df_clean["wind_speed"].between(WIND_MIN_FOR_CURVE, WIND_MAX_FOR_CURVE)
        df_curve = df_clean.loc[curve_mask].copy()

        return df_clean, df_curve

    # ─── Module 2: Bin statistics ──────────────────────────────

    @staticmethod
    def compute_bin_stats(
        df: pd.DataFrame,
        bin_width: float = BIN_WIDTH,
        min_samples: int = MIN_SAMPLES_PER_BIN,
    ) -> pd.DataFrame:
        """Compute per-bin statistics: q50, q90, MAD, mean, count.

        Pure pandas function — no DB access, fully testable.
        """
        bins = np.arange(WIND_MIN_FOR_CURVE, WIND_MAX_FOR_CURVE + bin_width, bin_width)
        df = df.copy()
        df["wind_bin"] = pd.cut(df["wind_speed"], bins=bins, right=False, include_lowest=True)

        grouped = df.dropna(subset=["wind_bin", "p_pu"]).groupby("wind_bin", observed=False)["p_pu"]

        stats = pd.DataFrame({
            "sample_count": grouped.count(),
            "q50_pu": grouped.median(),
            "q90_pu": grouped.quantile(0.90),
            "mean_pu": grouped.mean(),
        }).reset_index()

        # MAD: median absolute deviation
        mad_vals = grouped.apply(lambda x: float(np.nanmedian(np.abs(x - np.nanmedian(x)))))
        mad_vals.name = "mad_pu"
        stats = stats.merge(mad_vals, on="wind_bin", how="left")

        # Filter by minimum samples
        stats = stats[stats["sample_count"] >= min_samples].copy()

        # Extract bin center as float
        stats["wind_bin_center"] = stats["wind_bin"].apply(
            lambda iv: float(iv.left + iv.right) / 2 if pd.notna(iv) else np.nan
        )
        stats["wind_bin_left"] = stats["wind_bin"].apply(
            lambda iv: float(iv.left) if pd.notna(iv) else np.nan
        )

        return stats

    # ─── Module 2: Overperformance flagging ────────────────────

    @staticmethod
    def flag_overperformance(
        df: pd.DataFrame, yearly_stats: pd.DataFrame
    ) -> pd.Series:
        """Flag overperforming hours for removal.

        A row is overperforming if EITHER:
        - p_pu > q90_bin + 1.5 * MAD_bin (evaluated against that row's year stats)
        - p_pu > 1.02 (absolute ceiling)

        Returns boolean Series (True = overperforming).
        """
        flag = pd.Series(False, index=df.index)

        # Absolute ceiling
        flag |= df["p_pu"] > CEILING_PU

        # Statistical ceiling per year per bin
        bins = np.arange(WIND_MIN_FOR_CURVE, WIND_MAX_FOR_CURVE + BIN_WIDTH, BIN_WIDTH)
        df_binned = df.copy()
        df_binned["wind_bin"] = pd.cut(df_binned["wind_speed"], bins=bins, right=False, include_lowest=True)

        for year in df_binned["year"].unique():
            year_stats = yearly_stats[yearly_stats.get("year", pd.Series()) == year] if "year" in yearly_stats.columns else yearly_stats
            if year_stats.empty:
                continue

            year_mask = df_binned["year"] == year
            for _, bin_row in year_stats.iterrows():
                q90 = bin_row.get("q90_pu")
                mad = bin_row.get("mad_pu")
                bin_interval = bin_row.get("wind_bin")
                if pd.isna(q90) or pd.isna(mad) or pd.isna(bin_interval):
                    continue

                bin_mask = year_mask & (df_binned["wind_bin"] == bin_interval)
                threshold = q90 + OVERPERF_MAD_K * mad
                flag.loc[bin_mask & (df_binned["p_pu"] > threshold)] = True

        return flag

    # ─── Module 2: Build and store all curves ──────────────────

    async def _build_and_store_curves(
        self, windfarm_id: int, df_curve: pd.DataFrame
    ) -> dict:
        """Build yearly raw, yearly capability, and overall clean curves."""
        bins = np.arange(WIND_MIN_FOR_CURVE, WIND_MAX_FOR_CURVE + BIN_WIDTH, BIN_WIDTH)
        df_curve = df_curve.copy()
        df_curve["wind_bin"] = pd.cut(df_curve["wind_speed"], bins=bins, right=False, include_lowest=True)

        years = sorted(df_curve["year"].unique())
        all_yearly_raw: List[pd.DataFrame] = []
        all_yearly_capability: List[pd.DataFrame] = []

        # Step 1: Yearly raw curves
        for year in years:
            year_df = df_curve[df_curve["year"] == year]
            raw_stats = self.compute_bin_stats(year_df)
            if raw_stats.empty:
                continue
            raw_stats["year"] = year
            all_yearly_raw.append(raw_stats)

        if not all_yearly_raw:
            return {"error": "No yearly curves produced"}

        yearly_raw = pd.concat(all_yearly_raw, ignore_index=True)

        # Step 2: Flag overperformance using yearly raw stats
        ovp_flag = self.flag_overperformance(df_curve, yearly_raw)
        df_no_over = df_curve[~ovp_flag].copy()
        overperf_pct = float(ovp_flag.sum()) / max(len(df_curve), 1) * 100

        # Step 3: Yearly capability curves (after overperformance removal)
        for year in years:
            year_df = df_no_over[df_no_over["year"] == year]
            cap_stats = self.compute_bin_stats(year_df)
            if cap_stats.empty:
                continue
            cap_stats["year"] = year
            all_yearly_capability.append(cap_stats)

        # Step 4: Overall clean curve
        overall_clean = self.compute_bin_stats(df_no_over)

        # Step 5: Store all curves
        await self._delete_existing_curves(windfarm_id)

        stored = 0
        for raw_df in all_yearly_raw:
            stored += await self._store_bins(windfarm_id, int(raw_df["year"].iloc[0]), "raw", raw_df)
        for cap_df in all_yearly_capability:
            stored += await self._store_bins(windfarm_id, int(cap_df["year"].iloc[0]), "capability", cap_df)
        stored += await self._store_bins(windfarm_id, None, "overall_clean", overall_clean)

        await self.db.flush()

        return {
            "years": years,
            "overperformance_removed_pct": round(overperf_pct, 2),
            "bins_stored": stored,
        }

    async def _store_bins(
        self,
        windfarm_id: int,
        year: Optional[int],
        curve_type: str,
        stats_df: pd.DataFrame,
    ) -> int:
        """Insert bin rows into power_curve_bins."""
        count = 0
        for _, row in stats_df.iterrows():
            wind_bin_val = row.get("wind_bin_left", row.get("wind_bin"))
            if pd.isna(wind_bin_val):
                continue
            pcb = PowerCurveBin(
                windfarm_id=windfarm_id,
                year=year,
                curve_type=curve_type,
                wind_bin=float(wind_bin_val),
                q50_pu=float(row["q50_pu"]) if pd.notna(row.get("q50_pu")) else None,
                q90_pu=float(row["q90_pu"]) if pd.notna(row.get("q90_pu")) else None,
                mean_pu=float(row["mean_pu"]) if pd.notna(row.get("mean_pu")) else None,
                mad_pu=float(row["mad_pu"]) if pd.notna(row.get("mad_pu")) else None,
                sample_count=int(row["sample_count"]) if pd.notna(row.get("sample_count")) else 0,
            )
            self.db.add(pcb)
            count += 1
        return count

    async def _delete_existing_curves(self, windfarm_id: int) -> None:
        """Delete all existing curves for this windfarm (idempotent rebuild)."""
        await self.db.execute(
            delete(PowerCurveBin).where(PowerCurveBin.windfarm_id == windfarm_id)
        )

    # ─── Read stored curves ────────────────────────────────────

    async def get_power_curve(
        self,
        windfarm_id: int,
        year: Optional[int] = None,
        curve_type: str = "overall_clean",
    ) -> List[dict]:
        """Read stored power curve bins."""
        query = select(PowerCurveBin).where(
            PowerCurveBin.windfarm_id == windfarm_id,
            PowerCurveBin.curve_type == curve_type,
        )
        if year is not None:
            query = query.where(PowerCurveBin.year == year)
        else:
            query = query.where(PowerCurveBin.year.is_(None))
        query = query.order_by(PowerCurveBin.wind_bin)

        result = await self.db.execute(query)
        bins = result.scalars().all()
        return [
            {
                "wind_bin": float(b.wind_bin),
                "q50_pu": float(b.q50_pu) if b.q50_pu else None,
                "q90_pu": float(b.q90_pu) if b.q90_pu else None,
                "mean_pu": float(b.mean_pu) if b.mean_pu else None,
                "mad_pu": float(b.mad_pu) if b.mad_pu else None,
                "sample_count": b.sample_count,
            }
            for b in bins
        ]
