"""Degradation analysis service — Module 5.

Estimates whether there is a statistically significant long-run trend in
operational performance — i.e. whether the turbine is degrading (or recovering)
over time, after accounting for wind variability and seasonal effects.

Method: compute per-hour residuals vs yearly capability curve in operational
wind range (4-14 m/s), remove the seasonal component via additive
decomposition (period = 8760 observations), then fit an OLS trend on
year_fraction vs deseasonalised residual.
"""

from datetime import date
from typing import Dict, Optional

import numpy as np
import pandas as pd
import structlog
from scipy import stats as scipy_stats
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.degradation_result import DegradationResult
from app.models.power_curve_bin import PowerCurveBin
from app.models.windfarm import Windfarm

try:
    from statsmodels.tsa.seasonal import seasonal_decompose

    HAS_STATSMODELS = True
except ImportError:  # pragma: no cover - statsmodels is a hard dependency
    HAS_STATSMODELS = False

logger = structlog.get_logger(__name__)

# ─── Configuration ─────────────────────────────────────────────
OP_WIND_MIN = 4.0  # Operational wind range min (m/s)
OP_WIND_MAX = 14.0  # Operational wind range max (m/s)
MIN_MEDIAN_PU_FOR_OPERATIONAL = 0.10  # Skip bins where reference is too low
MIN_FIT_HOURS = 100  # Skip fit if fewer than this many qualifying hours
SEASONAL_PERIOD_HOURS = 8760  # One calendar-year cycle, in observations


# ─── Module-level helpers (pure compute, testable) ─────────────


def remove_seasonal_component(series: pd.Series, period: int = SEASONAL_PERIOD_HOURS) -> pd.Series:
    """Subtract additive seasonal component from a series.

    Mirrors the reference pipeline (`energyexe_pipeline_full.py:323-334`):
    when statsmodels is unavailable or the series is shorter than two
    periods, return the series unchanged. Otherwise fill gaps then
    decompose and subtract the seasonal piece.
    """
    if not HAS_STATSMODELS or len(series) < 2 * period:
        return series
    try:
        result = seasonal_decompose(
            series.interpolate().ffill().bfill(),
            model="additive",
            period=period,
            extrapolate_trend="freq",
        )
        return series - result.seasonal
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("seasonal_decompose_failed", error=str(exc))
        return series


class DegradationService:
    """Analyses long-run performance degradation trends."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ─── Main entry ────────────────────────────────────────────

    async def analyze_degradation(
        self,
        windfarm_id: int,
        reference: str = "q50",
        pipeline_run_id: Optional[int] = None,
    ) -> Optional[dict]:
        """Run degradation analysis for a windfarm.

        Args:
            reference: 'q50' (P50) or 'q90' (P10) capability curve reference.

        Returns summary dict or None if insufficient data.
        """
        rated_mw = await self._get_rated_mw(windfarm_id)
        if not rated_mw:
            return None

        yearly_curves = await self._load_yearly_capability(windfarm_id, reference)
        if not yearly_curves:
            logger.warning("degradation_no_curves", windfarm_id=windfarm_id)
            return None

        from app.services.power_curve_service import PowerCurveService

        pcs = PowerCurveService(self.db)
        df = await pcs._load_hourly_data(windfarm_id, None, None, float(rated_mw))
        if df.empty:
            return None

        return await self._run_from_df(windfarm_id, df, yearly_curves, reference, pipeline_run_id)

    async def analyze_degradation_from_df(
        self,
        windfarm_id: int,
        df: pd.DataFrame,
        reference: str = "q50",
        pipeline_run_id: Optional[int] = None,
        n_constraint_hours_excluded: Optional[int] = None,
    ) -> Optional[dict]:
        """Run degradation analysis using a pre-loaded hourly DataFrame.

        Args:
            n_constraint_hours_excluded: how many hours the orchestrator
                dropped from ``df`` because they fell inside an active
                structural constraint. Persisted on the result row for
                downstream reporting; does not affect the fit itself.
        """
        yearly_curves = await self._load_yearly_capability(windfarm_id, reference)
        if not yearly_curves:
            return None
        return await self._run_from_df(
            windfarm_id,
            df,
            yearly_curves,
            reference,
            pipeline_run_id,
            n_constraint_hours_excluded=n_constraint_hours_excluded,
        )

    async def _run_from_df(
        self,
        windfarm_id: int,
        df: pd.DataFrame,
        yearly_curves: Dict[int, Dict[float, float]],
        reference: str,
        pipeline_run_id: Optional[int],
        n_constraint_hours_excluded: Optional[int] = None,
    ) -> Optional[dict]:
        # The operational floor is always q50 (spec line 998). When fitting the
        # q90 reference, load the q50 curve so the floor is applied against it
        # rather than against q90 (which would admit low-wind hours the spec
        # excludes — see issue #80).
        floor_curves = None
        if reference != "q50":
            floor_curves = await self._load_yearly_capability(windfarm_id, "q50")

        residuals = self.compute_residuals(df, yearly_curves, floor_curves=floor_curves)
        if residuals.empty or len(residuals) < MIN_FIT_HOURS:
            logger.warning(
                "degradation_insufficient_data",
                windfarm_id=windfarm_id,
                reference=reference,
                hours=len(residuals),
            )
            return None

        trend = self.fit_degradation_trend(residuals)
        if trend is None:
            return None

        analysis_start = date(int(residuals["year"].min()), 1, 1)
        analysis_end = date(int(residuals["year"].max()), 12, 31)

        await self._store_result(
            windfarm_id,
            reference,
            trend,
            analysis_start,
            analysis_end,
            trend["n"],
            pipeline_run_id,
            n_constraint_hours_excluded=n_constraint_hours_excluded,
        )

        return {
            "reference": reference,
            "slope_pct_per_year": trend["slope_pct"],
            "slope_pu_per_year": trend["slope"],
            "r_squared": trend["r2"],
            "p_value": trend["p_value"],
            "ci_95": trend["ci95"],
            "ci_95_pct": trend["ci95_pct"],
            "baseline_cap_pu": trend["baseline_cap_pu"],
            "n_constraint_hours_excluded": n_constraint_hours_excluded,
            "data_points": trend["n"],
            "analysis_range": f"{analysis_start} to {analysis_end}",
        }

    # ─── Pure computation (testable) ───────────────────────────

    @staticmethod
    def compute_residuals(
        df: pd.DataFrame,
        yearly_curves: Dict[int, Dict[float, float]],
        op_wind_min: float = OP_WIND_MIN,
        op_wind_max: float = OP_WIND_MAX,
        min_median_pu: float = MIN_MEDIAN_PU_FOR_OPERATIONAL,
        floor_curves: Optional[Dict[int, Dict[float, float]]] = None,
    ) -> pd.DataFrame:
        """Compute per-hour residual_pu = p_pu - reference_bin_pu.

        Filters to the operational wind range and to bins where the **q50**
        capability is above ``min_median_pu``. Returns hourly rows (one per
        surviving hour) with columns: ``hour, year, year_fraction, wind_speed,
        wind_bin, ref_pu, p_pu, residual_pu``.

        The residual is taken against the *active* reference (``yearly_curves``,
        q50 or q90). The operational floor, however, is **always** the q50
        curve — matching the reference pipeline, which filters on ``q50_bin``
        for both references (``energyexe_pipeline_full.py:998``). When fitting
        q90, pass the q50 curve as ``floor_curves``; for the q50 fit it is
        ``None`` and the active curve doubles as its own floor.

        year_fraction follows the reference at line 1001:
            year + (dayofyear - 1) / 365.25
        """
        out = df.copy()

        out = out[(out["wind_speed"] >= op_wind_min) & (out["wind_speed"] <= op_wind_max)].copy()
        if out.empty:
            return pd.DataFrame()

        out["wind_bin"] = np.floor(out["wind_speed"]).astype(float)

        # Residual reference: the active curve (q50 or q90).
        out["ref_pu"] = [
            yearly_curves.get(int(y), {}).get(b) for y, b in zip(out["year"], out["wind_bin"])
        ]
        # Operational floor: always q50 (spec line 998). floor_curves carries
        # the q50 curve when fitting q90; otherwise reuse the active curve.
        floor = floor_curves if floor_curves is not None else yearly_curves
        out["floor_pu"] = [
            floor.get(int(y), {}).get(b) for y, b in zip(out["year"], out["wind_bin"])
        ]
        out = out[
            out["ref_pu"].notna() & out["floor_pu"].notna() & (out["floor_pu"] >= min_median_pu)
        ].copy()
        if out.empty:
            return pd.DataFrame()

        out["ref_pu"] = out["ref_pu"].astype(float)
        out["residual_pu"] = out["p_pu"].astype(float) - out["ref_pu"]

        ts = pd.to_datetime(out["hour"])
        out["year_fraction"] = ts.dt.year + (ts.dt.dayofyear - 1) / 365.25

        cols = [
            "hour",
            "year",
            "year_fraction",
            "wind_speed",
            "wind_bin",
            "ref_pu",
            "p_pu",
            "residual_pu",
        ]
        return out[cols].reset_index(drop=True)

    @staticmethod
    def fit_degradation_trend(
        residuals: pd.DataFrame,
        seasonal_period: int = SEASONAL_PERIOD_HOURS,
    ) -> Optional[dict]:
        """Fit OLS on (year_fraction, residual_deseasonalised).

        Returns a summary dict (slope, intercept, r2, p_value, std_err, ci95,
        slope_pct, baseline_cap_pu, n) or None when there is not enough data.
        Mirrors spec :1019-1064.
        """
        if residuals.empty or len(residuals) < MIN_FIT_HOURS:
            return None

        df_fit = residuals.sort_values("hour").reset_index(drop=True)

        # Deseasonalise: index by hour so seasonal_decompose can align.
        # Spec uses time_col-indexed series, period=8760 observations.
        series = df_fit.set_index("hour")["residual_pu"]
        deseasonalised = remove_seasonal_component(series, period=seasonal_period)
        df_fit["residual_deseasonalised"] = deseasonalised.values

        x = df_fit["year_fraction"].to_numpy(dtype=float)
        y = df_fit["residual_deseasonalised"].to_numpy(dtype=float)
        mask = np.isfinite(x) & np.isfinite(y)
        x, y = x[mask], y[mask]
        n = int(len(x))
        if n < 2:
            return None

        ssx = float(np.sum((x - x.mean()) ** 2))
        if ssx == 0:
            return None

        slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(x, y)

        ci95 = None
        if n >= 3 and std_err > 0 and np.isfinite(std_err):
            t_crit = float(scipy_stats.t.ppf(0.975, df=n - 2))
            ci95 = (
                float(slope - t_crit * std_err),
                float(slope + t_crit * std_err),
            )

        # Hours-weighted median of ref_pu in the first year of df_fit
        # (spec :1050-1052). df_fit has one row per surviving hour, so a
        # wind bin with many hours contributes that many identical ref_pu
        # values to the median — i.e. naturally weighted toward bins the
        # windfarm actually operates in.
        first_year = int(df_fit["year_fraction"].min())
        baseline_df = df_fit[df_fit["year_fraction"].between(first_year, first_year + 1)]
        if not baseline_df.empty:
            baseline_cap_pu = float(baseline_df["ref_pu"].median())
        else:
            baseline_cap_pu = float("nan")

        # No hardcoded fallback. The old `baseline_cap_pu = 0.35` silently
        # resurrected Bug-C (a fabricated denominator). If the first-year
        # baseline is invalid we cannot express slope as a % of capability, so
        # skip this fit rather than report a meaningless slope_pct (issue #80).
        if not np.isfinite(baseline_cap_pu) or baseline_cap_pu <= 0:
            logger.warning(
                "degradation_baseline_invalid_skip",
                computed=baseline_cap_pu,
                first_year=first_year,
                first_year_rows=len(baseline_df),
            )
            return None

        slope_pct = float(slope / baseline_cap_pu * 100)
        ci95_pct = None
        if ci95 is not None:
            ci95_pct = (
                float(ci95[0] / baseline_cap_pu * 100),
                float(ci95[1] / baseline_cap_pu * 100),
            )

        return {
            "slope": float(slope),
            "intercept": float(intercept),
            "r2": float(r_value**2),
            "p_value": float(p_value),
            "std_err": float(std_err),
            "ci95": ci95,
            "ci95_pct": ci95_pct,
            "slope_pct": slope_pct,
            "baseline_cap_pu": baseline_cap_pu,
            "n": n,
        }

    # ─── Data loading ──────────────────────────────────────────

    async def _get_rated_mw(self, windfarm_id: int) -> Optional[float]:
        result = await self.db.execute(
            select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        )
        val = result.scalar_one_or_none()
        return float(val) if val else None

    async def _load_yearly_capability(
        self, windfarm_id: int, reference: str
    ) -> Dict[int, Dict[float, float]]:
        """Load yearly capability curves as {year: {wind_bin: ref_pu}}."""
        col = "q50_pu" if reference == "q50" else "q90_pu"

        result = await self.db.execute(
            select(PowerCurveBin).where(
                PowerCurveBin.windfarm_id == windfarm_id,
                PowerCurveBin.curve_type == "capability",
            )
        )
        bins = result.scalars().all()

        curves: Dict[int, Dict[float, float]] = {}
        for b in bins:
            if b.year is None:
                continue
            val = float(getattr(b, col)) if getattr(b, col) is not None else None
            if val is not None:
                curves.setdefault(b.year, {})[float(b.wind_bin)] = val
        return curves

    # ─── Storage ───────────────────────────────────────────────

    async def _store_result(
        self,
        windfarm_id: int,
        reference: str,
        trend: dict,
        analysis_start: date,
        analysis_end: date,
        data_points: int,
        pipeline_run_id: Optional[int],
        n_constraint_hours_excluded: Optional[int] = None,
    ) -> None:
        """Store or update degradation result."""
        await self.db.execute(
            delete(DegradationResult).where(
                DegradationResult.windfarm_id == windfarm_id,
                DegradationResult.reference_curve == reference,
            )
        )

        dr = DegradationResult(
            windfarm_id=windfarm_id,
            reference_curve=reference,
            analysis_start=analysis_start,
            analysis_end=analysis_end,
            data_points=data_points,
            slope_pu_per_year=trend["slope"],
            slope_pct_per_year=trend["slope_pct"],
            intercept=trend["intercept"],
            r_squared=trend["r2"],
            p_value=trend["p_value"],
            ci_lower_95=trend["ci95"][0] if trend["ci95"] else None,
            ci_upper_95=trend["ci95"][1] if trend["ci95"] else None,
            ci_lower_95_pct=trend["ci95_pct"][0] if trend["ci95_pct"] else None,
            ci_upper_95_pct=trend["ci95_pct"][1] if trend["ci95_pct"] else None,
            baseline_cap_pu=trend["baseline_cap_pu"],
            n_constraint_hours_excluded=n_constraint_hours_excluded,
            pipeline_run_id=pipeline_run_id,
        )
        self.db.add(dr)
