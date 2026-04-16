"""Performance pipeline orchestrator — runs Modules 1-6 in sequence.

Also contains Module 6 (Commercial Reporting) logic: constraint proxy
timeseries and PPA scenario analysis.
"""

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.import_job_execution import ImportJobExecution, ImportJobStatus
from app.models.performance_summary import PerformanceSummary
from app.models.power_curve_bin import PowerCurveBin
from app.models.windfarm import Windfarm
from app.services.degradation_service import DegradationService
from app.services.performance_anomaly_service import PerformanceAnomalyService
from app.services.power_curve_service import PowerCurveService
from app.services.wind_normalisation_service import WindNormalisationService

logger = structlog.get_logger(__name__)


class PerformancePipelineService:
    """Orchestrates the full 6-module performance pipeline."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ─── Batch runner ──────────────────────────────────────────

    async def run_pipeline_batch(
        self, windfarm_ids: Optional[List[int]] = None
    ) -> dict:
        """Run pipeline for all/specified windfarms as a tracked import job."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        job = ImportJobExecution(
            job_name="performance-pipeline",
            source="SYSTEM",
            job_type="scheduled",
            import_start_date=now - timedelta(days=365 * 10),
            import_end_date=now,
            status=ImportJobStatus.RUNNING,
            started_at=now,
        )
        self.db.add(job)
        await self.db.flush()

        try:
            if not windfarm_ids:
                result = await self.db.execute(
                    select(Windfarm.id).where(Windfarm.status == "operational")
                )
                windfarm_ids = [r[0] for r in result.fetchall()]

            results = {}
            for wf_id in windfarm_ids:
                try:
                    wf_result = await self.run_pipeline(wf_id, pipeline_run_id=job.id)
                    results[wf_id] = wf_result
                except Exception as e:
                    logger.error("pipeline_windfarm_error", windfarm_id=wf_id, error=str(e))
                    results[wf_id] = {"error": str(e)}

            succeeded = sum(1 for r in results.values() if "error" not in r)
            job.mark_success(records_imported=succeeded)
            await self.db.commit()

            logger.info(
                "performance_pipeline_complete",
                windfarms=len(windfarm_ids),
                succeeded=succeeded,
            )
            return {
                "job_id": job.id,
                "windfarms_processed": len(windfarm_ids),
                "succeeded": succeeded,
                "failed": len(windfarm_ids) - succeeded,
            }

        except Exception as e:
            job.mark_failed(str(e))
            await self.db.commit()
            raise

    # ─── Single windfarm pipeline ──────────────────────────────

    async def run_pipeline(
        self,
        windfarm_id: int,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        pipeline_run_id: Optional[int] = None,
    ) -> dict:
        """Execute modules 1-6 in order for one windfarm.

        Optimized: loads hourly data ONCE and passes it to all modules.
        """
        import pandas as pd

        result: Dict[str, Any] = {"windfarm_id": windfarm_id}

        # Get rated capacity
        wf_result = await self.db.execute(
            select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        )
        rated_mw = wf_result.scalar_one_or_none()
        if not rated_mw or rated_mw <= 0:
            return {"windfarm_id": windfarm_id, "error": "No rated capacity"}

        # ── SINGLE DATA LOAD ── reused by all modules
        pcs = PowerCurveService(self.db)
        df_all = await pcs._load_hourly_data(windfarm_id, start_year, end_year, float(rated_mw))
        if df_all.empty:
            return {"windfarm_id": windfarm_id, "error": "No hourly data"}

        # Module 1+2: Power curves — pass pre-loaded data (avoids second SQL query).
        # If this fails, the whole pipeline fails (everything else depends on curves).
        curves = await pcs.build_power_curves(windfarm_id, start_year, end_year, df_preloaded=df_all)
        result["power_curves"] = curves
        if "error" in curves:
            return result

        years = [int(y) for y in curves.get("years", [])]
        if not years:
            result["error"] = "No years with data"
            return result

        # Module 3: Anomaly detection — each year in its own SAVEPOINT so one bad
        # year doesn't poison the whole transaction.
        anomaly_svc = PerformanceAnomalyService(self.db)
        anomaly_results: Dict[int, Any] = {}
        for year in years:
            try:
                async with self.db.begin_nested():
                    df_year = df_all[df_all["year"] == year].copy()
                    if df_year.empty:
                        anomaly_results[year] = {"error": "No data for year"}
                        continue
                    ar = await anomaly_svc.detect_anomalies_from_df(
                        windfarm_id, year, df_year, float(rated_mw), pipeline_run_id
                    )
                    anomaly_results[year] = ar
            except Exception as e:
                logger.error("pipeline_anomaly_error", windfarm_id=windfarm_id, year=year, error=str(e))
                anomaly_results[year] = {"error": str(e)}
        result["anomaly_detection"] = anomaly_results

        # Module 4: Wind normalisation — each reference in its own SAVEPOINT.
        norm_svc = WindNormalisationService(self.db)
        norm_out: Dict[str, Any] = {}
        for ref, key in [("q50", "p50"), ("q90", "p10")]:
            try:
                async with self.db.begin_nested():
                    norm_out[key] = await norm_svc.compute_normalisation_from_df(
                        windfarm_id, df_all, float(rated_mw), ref, pipeline_run_id
                    )
            except Exception as e:
                logger.error("pipeline_normalisation_error", windfarm_id=windfarm_id, ref=ref, error=str(e))
                norm_out[key] = {"error": str(e)}
        result["wind_normalisation"] = norm_out

        # Module 5: Degradation — each reference in its own SAVEPOINT.
        deg_svc = DegradationService(self.db)
        deg_out: Dict[str, Any] = {}
        for ref, key in [("q50", "p50"), ("q90", "p10")]:
            try:
                async with self.db.begin_nested():
                    deg_out[key] = await deg_svc.analyze_degradation_from_df(
                        windfarm_id, df_all, ref, pipeline_run_id
                    )
            except Exception as e:
                logger.error("pipeline_degradation_error", windfarm_id=windfarm_id, ref=ref, error=str(e))
                deg_out[key] = {"error": str(e)}
        result["degradation"] = deg_out

        # Module 6: Commercial metrics — each year in its own SAVEPOINT.
        commercial_ok = 0
        for year in years:
            try:
                async with self.db.begin_nested():
                    await self._compute_commercial_metrics(windfarm_id, year, pipeline_run_id)
                commercial_ok += 1
            except Exception as e:
                logger.error("pipeline_commercial_error", windfarm_id=windfarm_id, year=year, error=str(e))
        result["commercial"] = {"years_computed": commercial_ok}

        # Final flush — surfaces any remaining issues loudly instead of silently rolling back at commit.
        await self.db.flush()
        return result

    # ─── Module 6: Commercial metrics ──────────────────────────

    async def _compute_commercial_metrics(
        self, windfarm_id: int, year: int, pipeline_run_id: Optional[int] = None
    ) -> None:
        """Compute constraint proxy and lost value for a year.

        constraint_proxy_mwh = sum((q90_bin - q50_bin) * rated_mw) per qualifying hour
        lost_value_eur = constraint_proxy_mwh * avg_market_price
        """
        # Get rated MW
        wf_result = await self.db.execute(
            select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        )
        rated_mw = wf_result.scalar_one_or_none()
        if not rated_mw:
            return

        # Load overall_clean curve for (q90 - q50) gap
        result = await self.db.execute(
            select(PowerCurveBin).where(
                PowerCurveBin.windfarm_id == windfarm_id,
                PowerCurveBin.curve_type == "overall_clean",
            )
        )
        bins = result.scalars().all()
        gap_by_bin = {}
        for b in bins:
            if b.q90_pu and b.q50_pu:
                gap = float(b.q90_pu) - float(b.q50_pu)
                if gap > 0:
                    gap_by_bin[float(b.wind_bin)] = gap

        if not gap_by_bin:
            return

        # Reuse power curve service's robust pandas-merge loader (avoids 3-table SQL join plan).
        from app.services.power_curve_service import PowerCurveService
        import pandas as pd
        pcs = PowerCurveService(self.db)
        df = await pcs._load_hourly_data(windfarm_id, year, year, float(rated_mw))
        if df.empty:
            return

        df["wind_bin"] = df["wind_speed"].apply(lambda v: float(int(v)) if pd.notna(v) else None)
        df = df.dropna(subset=["wind_bin"])
        grouped = df.groupby("wind_bin").agg(
            hours=("hour", "count"),
            avg_price=("market_price", "mean"),
        ).reset_index()

        total_proxy_mwh = 0.0
        total_proxy_hours = 0
        total_price_sum = 0.0
        for _, row in grouped.iterrows():
            wbin = float(row["wind_bin"])
            gap = gap_by_bin.get(wbin, 0)
            if gap > 0:
                hours = int(row["hours"])
                proxy = gap * float(rated_mw) * hours
                total_proxy_mwh += proxy
                total_proxy_hours += hours
                if pd.notna(row["avg_price"]):
                    total_price_sum += float(row["avg_price"]) * hours

        avg_price = total_price_sum / max(total_proxy_hours, 1)
        lost_value = total_proxy_mwh * avg_price

        # Update yearly summary — UPDATE then INSERT fallback (NULL month defeats ON CONFLICT).
        params = {
            "wf_id": windfarm_id,
            "year": year,
            "proxy_mwh": round(total_proxy_mwh, 3),
            "lost_value": round(lost_value, 2),
            "pipeline_run_id": pipeline_run_id,
        }
        updated = await self.db.execute(
            text("""
                UPDATE performance_summaries
                SET constraint_proxy_mwh = :proxy_mwh,
                    lost_value_eur = :lost_value,
                    pipeline_run_id = COALESCE(:pipeline_run_id, pipeline_run_id),
                    updated_at = NOW()
                WHERE windfarm_id = :wf_id
                  AND period_type = 'year'
                  AND year = :year
                  AND month IS NULL
            """),
            params,
        )
        if updated.rowcount == 0:
            await self.db.execute(
                text("""
                    INSERT INTO performance_summaries
                      (windfarm_id, period_type, year, month,
                       constraint_proxy_mwh, lost_value_eur, pipeline_run_id)
                    VALUES
                      (:wf_id, 'year', :year, NULL,
                       :proxy_mwh, :lost_value, :pipeline_run_id)
                """),
                params,
            )

    # ─── PPA scenario analysis ─────────────────────────────────

    async def run_ppa_scenarios(
        self,
        windfarm_id: int,
        year: int,
        price_scenarios: List[float],
    ) -> List[dict]:
        """Run PPA price scenario analysis for a year.

        For each price: revenue = sum(generation_mwh) * price.
        Returns list of scenario results.
        """
        # Get actual generation — SUM across all units per hour then across hours.
        # Exclude hours where ANY unit was ramping up (keeps semantics consistent).
        query = text("""
            WITH h AS (
                SELECT hour, SUM(generation_mwh) as hourly_gen, BOOL_OR(is_ramp_up) as any_ramp_up
                FROM generation_data
                WHERE windfarm_id = :wf_id
                  AND EXTRACT(YEAR FROM hour) = :year
                GROUP BY hour
                HAVING BOOL_OR(is_ramp_up) = false
            )
            SELECT COALESCE(SUM(hourly_gen), 0) as total_mwh FROM h
        """)
        result = await self.db.execute(query, {"wf_id": windfarm_id, "year": int(year)})
        row = result.fetchone()
        actual_mwh = float(row.total_mwh) if row and row.total_mwh else 0

        # Get P50 target
        from app.models.p50_target import P50Target
        p50_result = await self.db.execute(
            select(P50Target.p50_target_gwh).where(P50Target.windfarm_id == windfarm_id).limit(1)
        )
        p50_gwh = p50_result.scalar_one_or_none()
        p50_mwh = float(p50_gwh) * 1000 if p50_gwh else actual_mwh

        scenarios = []
        for price in price_scenarios:
            revenue = actual_mwh * price
            p50_revenue = p50_mwh * price
            gap = revenue - p50_revenue
            value_1pct = 0.01 * actual_mwh * price

            scenarios.append({
                "ppa_eur_per_mwh": price,
                "actual_mwh": round(actual_mwh, 1),
                "revenue_eur": round(revenue, 2),
                "revenue_vs_p50_eur": round(gap, 2),
                "value_of_1pct_eur_per_year": round(value_1pct, 2),
            })

        return scenarios
