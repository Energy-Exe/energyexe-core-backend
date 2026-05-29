"""Performance pipeline orchestrator — runs Modules 1-6 in sequence.

Also contains Module 6 (Commercial Reporting) logic: constraint proxy
timeseries and PPA scenario analysis.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.import_job_execution import ImportJobExecution, ImportJobStatus
from app.models.performance_summary import PerformanceSummary
from app.models.power_curve_bin import PowerCurveBin
from app.models.windfarm import Windfarm
from app.services.degradation_service import DegradationService
from app.services.generation_concentration_service import GenerationConcentrationService
from app.services.performance_anomaly_service import PerformanceAnomalyService
from app.services.power_curve_service import PowerCurveService
from app.services.wind_normalisation_service import WindNormalisationService

logger = structlog.get_logger(__name__)


class PerformancePipelineService:
    """Orchestrates the full 6-module performance pipeline."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ─── Batch runner ──────────────────────────────────────────

    async def run_pipeline_batch(self, windfarm_ids: Optional[List[int]] = None) -> dict:
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

        # Module 1b: Structural constraint detection — runs BEFORE Module 2 so
        # the capability / overall_clean curves are built from constraint-
        # cleaned data (issue #81; spec: df_curve_clean replaces df_curve from
        # Module 2 onward). Detection writes pending_review flags for analyst
        # review, but only CONFIRMED flags (issue #79) actually clean the
        # curves and the Modules 3/4/5 sample. Until an analyst confirms a
        # flag, nothing is masked and curves match the spec's no-constraint
        # path exactly.
        import numpy as np

        from app.services.structural_constraint_detection_service import (
            StructuralConstraintDetectionService,
            build_constraint_mask,
        )

        n_constraint_hours_excluded = 0
        df_for_curves = df_all
        try:
            detector = StructuralConstraintDetectionService(self.db)
            async with self.db.begin_nested():
                df_for_detect = df_all.copy()
                df_for_detect["wind_bin"] = np.floor(df_for_detect["wind_speed"]).astype(float)
                detect_out = await detector.detect_constraints(
                    windfarm_id, df_for_detect, pipeline_run_id=pipeline_run_id
                )
            result["structural_constraints"] = detect_out

            active_periods = await detector.load_active_periods(windfarm_id)
            if active_periods:
                mask = build_constraint_mask(df_all, active_periods)
                n_constraint_hours_excluded = int(mask.sum())
                df_for_curves = df_all[~mask].reset_index(drop=True)

            logger.info(
                "module_1b_complete",
                windfarm_id=windfarm_id,
                runs_detected=detect_out.get("runs_detected", 0),
                total_constrained_hours=detect_out.get("total_constrained_hours", 0),
                active_periods=len(active_periods),
                hours_masked_from_curves_and_downstream=n_constraint_hours_excluded,
            )
        except Exception as e:
            logger.error("pipeline_module_1b_error", windfarm_id=windfarm_id, error=str(e))
            result["structural_constraints"] = {"error": str(e)}
            df_for_curves = df_all

        # Module 1+2: Power curves — built from the constraint-cleaned sample
        # (issue #81). `return_df_no_over=True` so Modules 3 and 5 fit on the
        # same overperformance-cleaned sample the reference pipeline uses (spec
        # :968-1008); here that sample is already constraint-cleaned, so the
        # downstream modules need no further masking.
        curves = await pcs.build_power_curves(
            windfarm_id,
            start_year,
            end_year,
            df_preloaded=df_for_curves,
            return_df_no_over=True,
        )
        result["power_curves"] = curves
        if "error" in curves:
            return result

        # Extract df_no_over and remove from the response dict (large DF;
        # don't bloat the orchestrator return shape or logs with it).
        df_no_over = curves.pop("df_no_over", None)
        if df_no_over is None or df_no_over.empty:
            # Defensive fallback: shouldn't happen given the empty checks
            # above, but keeps a single clear failure path.
            df_no_over = df_for_curves

        years = [int(y) for y in curves.get("years", [])]
        if not years:
            result["error"] = "No years with data"
            return result

        logger.info(
            "module_2_complete",
            windfarm_id=windfarm_id,
            years=years,
            bins_stored=curves.get("bins_stored"),
            overperformance_removed_pct=curves.get("overperformance_removed_pct"),
            raw_rows=curves.get("raw_rows"),
            clean_rows=curves.get("clean_rows"),
            curve_rows=curves.get("curve_rows"),
        )

        # Module 3: Anomaly detection — each year in its own SAVEPOINT so one bad
        # year doesn't poison the whole transaction. Uses df_no_over to match
        # the reference pipeline's sample (FX1), with constraint hours masked
        # out (FX2).
        anomaly_svc = PerformanceAnomalyService(self.db)
        anomaly_results: Dict[int, Any] = {}
        for year in years:
            try:
                async with self.db.begin_nested():
                    df_year = df_no_over[df_no_over["year"] == year].copy()
                    if df_year.empty:
                        anomaly_results[year] = {"error": "No data for year"}
                        continue
                    ar = await anomaly_svc.detect_anomalies_from_df(
                        windfarm_id, year, df_year, float(rated_mw), pipeline_run_id
                    )
                    anomaly_results[year] = ar
            except Exception as e:
                logger.error(
                    "pipeline_anomaly_error", windfarm_id=windfarm_id, year=year, error=str(e)
                )
                anomaly_results[year] = {"error": str(e)}
        result["anomaly_detection"] = anomaly_results

        # Summarise across years for easy log scraping during baseline / regression work.
        ok_years = {
            y: r for y, r in anomaly_results.items() if isinstance(r, dict) and "error" not in r
        }
        logger.info(
            "module_3_complete",
            windfarm_id=windfarm_id,
            years_ok=list(ok_years.keys()),
            years_failed=[
                y for y, r in anomaly_results.items() if isinstance(r, dict) and "error" in r
            ],
            total_underperf_hours=sum(
                int(r.get("underperf_hours") or 0) for r in ok_years.values()
            ),
            total_lost_mwh=round(sum(float(r.get("lost_mwh") or 0) for r in ok_years.values()), 3),
            total_lost_eur=round(sum(float(r.get("lost_eur") or 0) for r in ok_years.values()), 2),
        )

        # Module 3f: Constraint loss summary — confirmed-constraint hours are
        # masked out of the ODI accounting above, so their infrastructure loss
        # is attributed here, priced against overall_clean Q50 (issue #82).
        # No-op until an analyst confirms a flag.
        try:
            from app.services.constraint_loss_service import ConstraintLossService

            async with self.db.begin_nested():
                cls_out = await ConstraintLossService(self.db).compute_and_store(
                    windfarm_id, df_all, float(rated_mw), pipeline_run_id=pipeline_run_id
                )
            result["constraint_loss"] = cls_out
            if cls_out.get("periods"):
                logger.info(
                    "module_3f_complete",
                    windfarm_id=windfarm_id,
                    periods=cls_out["periods"],
                    total_lost_mwh=cls_out["total_lost_mwh"],
                    total_lost_eur=cls_out["total_lost_eur"],
                )
        except Exception as e:
            logger.error("pipeline_constraint_loss_error", windfarm_id=windfarm_id, error=str(e))
            result["constraint_loss"] = {"error": str(e)}

        # Module 4: Wind normalisation — each reference in its own SAVEPOINT.
        # Uses df_no_over to match the reference pipeline's sample (FX1).
        norm_svc = WindNormalisationService(self.db)
        norm_out: Dict[str, Any] = {}
        for ref, key in [("q50", "p50"), ("q90", "p10")]:
            try:
                async with self.db.begin_nested():
                    norm_out[key] = await norm_svc.compute_normalisation_from_df(
                        windfarm_id, df_no_over, float(rated_mw), ref, pipeline_run_id
                    )
            except Exception as e:
                logger.error(
                    "pipeline_normalisation_error", windfarm_id=windfarm_id, ref=ref, error=str(e)
                )
                norm_out[key] = {"error": str(e)}
        result["wind_normalisation"] = norm_out

        logger.info(
            "module_4_complete",
            windfarm_id=windfarm_id,
            p50_status="ok"
            if "error" not in norm_out.get("p50", {})
            else norm_out["p50"].get("error"),
            p50_qualifying_hours=norm_out.get("p50", {}).get("qualifying_hours"),
            p50_years_computed=norm_out.get("p50", {}).get("years_computed"),
            p10_status="ok"
            if "error" not in norm_out.get("p10", {})
            else norm_out["p10"].get("error"),
            p10_qualifying_hours=norm_out.get("p10", {}).get("qualifying_hours"),
            p10_years_computed=norm_out.get("p10", {}).get("years_computed"),
        )

        # Module 5: Degradation — each reference in its own SAVEPOINT. Uses
        # df_no_over to match the reference pipeline's sample (FX1), with
        # constraint hours masked out (FX2). Records how many hours were
        # excluded on the degradation_results row for reporting.
        deg_svc = DegradationService(self.db)
        deg_out: Dict[str, Any] = {}
        for ref, key in [("q50", "p50"), ("q90", "p10")]:
            try:
                async with self.db.begin_nested():
                    deg_out[key] = await deg_svc.analyze_degradation_from_df(
                        windfarm_id,
                        df_no_over,
                        ref,
                        pipeline_run_id,
                        n_constraint_hours_excluded=n_constraint_hours_excluded,
                    )
            except Exception as e:
                logger.error(
                    "pipeline_degradation_error", windfarm_id=windfarm_id, ref=ref, error=str(e)
                )
                deg_out[key] = {"error": str(e)}
        result["degradation"] = deg_out

        # Module 5 is the bug-fix focus area — log all the key metrics so before/after
        # snapshots can be diffed straight from log scrape during Milestone A rollout.
        logger.info(
            "module_5_complete",
            windfarm_id=windfarm_id,
            p50_slope_pct=(deg_out.get("p50") or {}).get("slope_pct_per_year"),
            p50_ci_95=(deg_out.get("p50") or {}).get("ci_95"),
            p50_r_squared=(deg_out.get("p50") or {}).get("r_squared"),
            p50_p_value=(deg_out.get("p50") or {}).get("p_value"),
            p50_data_points=(deg_out.get("p50") or {}).get("data_points"),
            p10_slope_pct=(deg_out.get("p10") or {}).get("slope_pct_per_year"),
            p10_ci_95=(deg_out.get("p10") or {}).get("ci_95"),
            p10_r_squared=(deg_out.get("p10") or {}).get("r_squared"),
            p10_data_points=(deg_out.get("p10") or {}).get("data_points"),
        )

        # Module 6: Commercial metrics — each year in its own SAVEPOINT.
        commercial_ok = 0
        for year in years:
            try:
                async with self.db.begin_nested():
                    await self._compute_commercial_metrics(windfarm_id, year, pipeline_run_id)
                commercial_ok += 1
            except Exception as e:
                logger.error(
                    "pipeline_commercial_error", windfarm_id=windfarm_id, year=year, error=str(e)
                )
        result["commercial"] = {"years_computed": commercial_ok}

        logger.info(
            "module_6_complete",
            windfarm_id=windfarm_id,
            years_attempted=len(years),
            years_computed=commercial_ok,
            years_failed=len(years) - commercial_ok,
        )

        # Spec item 3: Generation Concentration — runs after commercial because
        # it reuses the same hourly (gen, price) join. Each year in its own
        # SAVEPOINT so a single bad year doesn't poison the rest.
        concentration_svc = GenerationConcentrationService(self.db)
        concentration_results: Dict[int, Any] = {}
        for year in years:
            try:
                async with self.db.begin_nested():
                    cr = await concentration_svc.compute_for_windfarm(
                        windfarm_id,
                        year,
                        df_preloaded=df_all,
                        pipeline_run_id=pipeline_run_id,
                    )
                    concentration_results[year] = cr
            except Exception as e:
                logger.error(
                    "pipeline_concentration_error",
                    windfarm_id=windfarm_id,
                    year=year,
                    error=str(e),
                )
                concentration_results[year] = {"error": str(e)}
        result["generation_concentration"] = concentration_results

        # Refresh peer aggregates that include this windfarm so downstream
        # vs-zone API responses reflect the latest values. Best-effort —
        # crashes here would mask the per-windfarm pipeline result.
        try:
            from app.services.peer_aggregate_service import PeerAggregateService

            agg_svc = PeerAggregateService(self.db)
            async with self.db.begin_nested():
                await agg_svc.refresh_for_windfarm(windfarm_id, years)
        except Exception as e:
            logger.warning(
                "pipeline_peer_aggregate_refresh_failed",
                windfarm_id=windfarm_id,
                error=str(e),
            )

        # Final flush — surfaces any remaining issues loudly instead of silently rolling back at commit.
        await self.db.flush()
        return result

    # ─── Module 6: Commercial metrics ──────────────────────────

    async def _compute_commercial_metrics(
        self, windfarm_id: int, year: int, pipeline_run_id: Optional[int] = None
    ) -> None:
        """Compute commercial metrics for a year.

        Persists onto ``performance_summaries`` (period_type='year'):
        - constraint_proxy_mwh = sum((q90_bin - q50_bin) * rated_mw) per qualifying hour
        - lost_value_eur = constraint_proxy_mwh * avg_market_price
        - contract_revenue_eur = sum(actual_mwh × price), where price is the
          active PPA price if the windfarm has a fixed-price contract for
          this year, else the hourly market_price (NaN-filled with the
          windfarm/year mean, matching spec :270-280).
        - contract_revenue_vs_p50_target_eur = contract_revenue - target_revenue
          where target_revenue = p50_target_mwh × avg_price.
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

        # Reuse power curve service's robust pandas-merge loader.
        import pandas as pd

        from app.services.power_curve_service import PowerCurveService

        pcs = PowerCurveService(self.db)
        df = await pcs._load_hourly_data(windfarm_id, year, year, float(rated_mw))
        if df.empty:
            return

        df["wind_bin"] = df["wind_speed"].apply(lambda v: float(int(v)) if pd.notna(v) else None)
        df = df.dropna(subset=["wind_bin"])

        # Resolve effective price per hour: PPA if active, else market_price.
        ppa_price = await self._get_active_ppa_price(windfarm_id, year)
        if ppa_price is not None:
            df["effective_price"] = float(ppa_price)
        else:
            n_nan = int(df["market_price"].isna().sum())
            mean_price = float(df["market_price"].mean()) if len(df) else 0.0
            if n_nan > 0 and mean_price > 0:
                # Match spec: fill NaN price with mean; log if appreciable
                if n_nan / max(len(df), 1) > 0.05:
                    logger.warning(
                        "commercial_nan_price_fill",
                        windfarm_id=windfarm_id,
                        year=year,
                        nan_hours=n_nan,
                        total_hours=len(df),
                        mean_price=mean_price,
                    )
                df = df.assign(effective_price=df["market_price"].fillna(mean_price))
            else:
                df = df.assign(effective_price=df["market_price"])

        # Aggregate constraint proxy per wind_bin
        grouped = (
            df.groupby("wind_bin")
            .agg(
                hours=("hour", "count"),
                avg_price=("effective_price", "mean"),
            )
            .reset_index()
        )

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

        avg_price_in_proxy_hours = total_price_sum / max(total_proxy_hours, 1)
        lost_value = total_proxy_mwh * avg_price_in_proxy_hours

        # Contract revenue: price-weighted sum across ALL valid hours
        actual_mwh = float(df["generation_mwh"].sum())
        contract_revenue = float((df["generation_mwh"] * df["effective_price"]).sum(skipna=True))
        avg_year_price = float(df["effective_price"].mean(skipna=True)) if len(df) else 0.0

        # P50 target → target revenue
        p50_mwh = await self._get_p50_target_mwh(windfarm_id, year)
        if p50_mwh is not None and avg_year_price > 0:
            target_revenue = float(p50_mwh) * avg_year_price
            revenue_vs_target = contract_revenue - target_revenue
        else:
            revenue_vs_target = None

        params = {
            "wf_id": windfarm_id,
            "year": year,
            "proxy_mwh": round(total_proxy_mwh, 3),
            "lost_value": round(lost_value, 2),
            "contract_revenue": round(contract_revenue, 2),
            "contract_revenue_vs_p50": (
                round(revenue_vs_target, 2) if revenue_vs_target is not None else None
            ),
            "pipeline_run_id": pipeline_run_id,
        }
        updated = await self.db.execute(
            text(
                """
                UPDATE performance_summaries
                SET constraint_proxy_mwh = :proxy_mwh,
                    lost_value_eur = :lost_value,
                    contract_revenue_eur = :contract_revenue,
                    contract_revenue_vs_p50_target_eur = :contract_revenue_vs_p50,
                    pipeline_run_id = COALESCE(:pipeline_run_id, pipeline_run_id),
                    updated_at = NOW()
                WHERE windfarm_id = :wf_id
                  AND period_type = 'year'
                  AND year = :year
                  AND month IS NULL
                """
            ),
            params,
        )
        if updated.rowcount == 0:
            await self.db.execute(
                text(
                    """
                    INSERT INTO performance_summaries
                      (windfarm_id, period_type, year, month,
                       constraint_proxy_mwh, lost_value_eur,
                       contract_revenue_eur, contract_revenue_vs_p50_target_eur,
                       pipeline_run_id)
                    VALUES
                      (:wf_id, 'year', :year, NULL,
                       :proxy_mwh, :lost_value,
                       :contract_revenue, :contract_revenue_vs_p50,
                       :pipeline_run_id)
                    """
                ),
                params,
            )

    async def _get_active_ppa_price(self, windfarm_id: int, year: int) -> Optional[float]:
        """Return the active PPA price for the given year, if a fixed-price
        contract covers it. Returns None for merchant/spot exposure.
        """
        from datetime import date as _date

        from app.models.ppa import PPA

        year_start = _date(year, 1, 1)
        year_end = _date(year, 12, 31)
        result = await self.db.execute(
            select(PPA.ppa_price_eur_mwh)
            .where(
                PPA.windfarm_id == windfarm_id,
                PPA.contract_type == "fixed_price",
                PPA.ppa_price_eur_mwh.isnot(None),
                (PPA.ppa_start_date.is_(None)) | (PPA.ppa_start_date <= year_end),
                (PPA.ppa_end_date.is_(None)) | (PPA.ppa_end_date >= year_start),
            )
            .order_by(PPA.ppa_start_date.desc())
            .limit(1)
        )
        val = result.scalar_one_or_none()
        return float(val) if val is not None else None

    async def _get_p50_target_mwh(self, windfarm_id: int, year: int) -> Optional[float]:
        """Return the active P50 target in MWh for the given year."""
        from datetime import date as _date

        from app.models.p50_target import P50Target

        year_start = _date(year, 1, 1)
        year_end = _date(year, 12, 31)
        result = await self.db.execute(
            select(P50Target.p50_target_volume_gwh)
            .where(
                P50Target.windfarm_id == windfarm_id,
                P50Target.p50_target_start_date <= year_end,
                (P50Target.p50_target_end_date.is_(None))
                | (P50Target.p50_target_end_date >= year_start),
            )
            .order_by(P50Target.p50_target_start_date.desc())
            .limit(1)
        )
        gwh = result.scalar_one_or_none()
        if gwh is None:
            return None
        return float(gwh) * 1000.0

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
        query = text(
            """
            WITH h AS (
                SELECT hour, SUM(generation_mwh) as hourly_gen, BOOL_OR(is_ramp_up) as any_ramp_up
                FROM generation_data
                WHERE windfarm_id = :wf_id
                  AND EXTRACT(YEAR FROM hour) = :year
                GROUP BY hour
                HAVING BOOL_OR(is_ramp_up) = false
            )
            SELECT COALESCE(SUM(hourly_gen), 0) as total_mwh FROM h
        """
        )
        result = await self.db.execute(query, {"wf_id": windfarm_id, "year": int(year)})
        row = result.fetchone()
        actual_mwh = float(row.total_mwh) if row and row.total_mwh else 0

        # Get P50 target — pick the most-recent target whose date range covers
        # `year` (or the most recent regardless if none cover the year).
        from datetime import date as _date

        from app.models.p50_target import P50Target

        year_start = _date(year, 1, 1)
        year_end = _date(year, 12, 31)
        p50_result = await self.db.execute(
            select(P50Target.p50_target_volume_gwh)
            .where(
                P50Target.windfarm_id == windfarm_id,
                P50Target.p50_target_start_date <= year_end,
                (P50Target.p50_target_end_date.is_(None))
                | (P50Target.p50_target_end_date >= year_start),
            )
            .order_by(P50Target.p50_target_start_date.desc())
            .limit(1)
        )
        p50_gwh = p50_result.scalar_one_or_none()
        if p50_gwh is None:
            # Fall back to most recent target regardless of date range
            p50_result = await self.db.execute(
                select(P50Target.p50_target_volume_gwh)
                .where(P50Target.windfarm_id == windfarm_id)
                .order_by(P50Target.p50_target_start_date.desc())
                .limit(1)
            )
            p50_gwh = p50_result.scalar_one_or_none()
        p50_mwh = float(p50_gwh) * 1000 if p50_gwh else actual_mwh

        # Resolve base PPA price if any scenario matches the windfarm's
        # actual contracted price — that scenario becomes the base for
        # the uplift column (spec :1184-1194).
        base_price = await self._get_active_ppa_price(windfarm_id, year)
        base_revenue: Optional[float] = None
        if base_price is not None and base_price in price_scenarios:
            base_revenue = actual_mwh * float(base_price)

        scenarios = []
        for price in price_scenarios:
            revenue = actual_mwh * price
            p50_revenue = p50_mwh * price
            gap = revenue - p50_revenue
            value_1pct = 0.01 * actual_mwh * price
            is_base = base_price is not None and price == base_price
            uplift = round(revenue - base_revenue, 2) if base_revenue is not None else None

            scenarios.append(
                {
                    "ppa_eur_per_mwh": price,
                    "actual_mwh": round(actual_mwh, 1),
                    "revenue_eur": round(revenue, 2),
                    "revenue_vs_p50_eur": round(gap, 2),
                    "value_of_1pct_eur_per_year": round(value_1pct, 2),
                    "is_base": is_base,
                    "revenue_uplift_vs_base_eur": uplift,
                }
            )

        return scenarios
