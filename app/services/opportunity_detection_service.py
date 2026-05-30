"""Opportunity detection service — implements 6 schemas (OPS-01..MKT-03).

Runs all detection logic, calculations, and job execution in a single service.
Static methods for severity/branch/suppression are pure functions for easy testing.
"""

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import structlog
from sqlalchemy import and_, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.import_job_execution import ImportJobExecution, ImportJobStatus
from app.models.opportunity import Opportunity, OpportunityStatus, SchemaCode, Severity
from app.models.ppa import PPA
from app.models.windfarm import Windfarm
from app.services.price_analytics_service import PriceAnalyticsService

logger = structlog.get_logger(__name__)

# --- Configurable thresholds ---
# ODI definition is TBD. Using availability_pct as placeholder.
ODI_THRESHOLD_PCT = 95.0  # Availability % below which a month is "low-ODI"
OPS02_SEASONAL_GAP_CONFIRMED_PP = 8.0  # Capacity factor gap in pp
OPS02_SEASONAL_GAP_MARGINAL_PP = 4.0
MKT01_GAP_CONFIRMED_PP = 10.0
MKT01_GAP_INDICATIVE_PP = 5.0
MKT01_GAP_WATCH_PP = 2.0
MKT03_CI_CONFIRMED = 1.20
MKT03_CI_INDICATIVE = 1.10
MKT03_CI_WATCH = 1.05
CURTAILMENT_SUPPRESSION_PCT = 15.0
LONG_PPA_YEARS = 5


class OpportunityDetectionService:
    """Detects opportunities across 6 schemas for wind farm assets."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.price_analytics = PriceAnalyticsService(db)

    # ─── Job runner ────────────────────────────────────────────────

    async def run_detection_job(
        self, windfarm_ids: Optional[List[int]] = None, period_months: int = 24
    ) -> dict:
        """Run opportunity detection as a tracked import job."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        job = ImportJobExecution(
            job_name="opportunity-detection",
            source="SYSTEM",
            job_type="scheduled",
            import_start_date=now - timedelta(days=period_months * 30),
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

            opportunities = await self.detect_all(windfarm_ids, period_months, job.id)

            job.mark_success(records_imported=len(opportunities))
            await self.db.commit()

            logger.info(
                "opportunity_detection_complete",
                windfarms=len(windfarm_ids),
                opportunities=len(opportunities),
            )
            return {
                "job_id": job.id,
                "windfarms_scanned": len(windfarm_ids),
                "opportunities_created": len(opportunities),
            }

        except Exception as e:
            job.mark_failed(str(e))
            await self.db.commit()
            logger.error("opportunity_detection_failed", error=str(e))
            raise

    # ─── Orchestrator ──────────────────────────────────────────────

    async def detect_all(
        self,
        windfarm_ids: List[int],
        period_months: int = 24,
        detection_run_id: Optional[int] = None,
    ) -> List[Opportunity]:
        """Run all 6 schemas for given windfarms, respecting dependency order."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        period_start = now - timedelta(days=period_months * 30)
        period_end = now

        # Supersede previous ACTIVE opportunities for these windfarms
        await self.db.execute(
            update(Opportunity)
            .where(
                and_(
                    Opportunity.windfarm_id.in_(windfarm_ids),
                    Opportunity.status == OpportunityStatus.ACTIVE,
                )
            )
            .values(status=OpportunityStatus.SUPERSEDED, updated_at=now)
        )

        all_opportunities: List[Opportunity] = []

        for wf_id in windfarm_ids:
            try:
                wf_opps = await self._detect_windfarm(
                    wf_id, period_start, period_end, detection_run_id
                )
                all_opportunities.extend(wf_opps)
            except Exception as e:
                logger.error(
                    "opportunity_detection_windfarm_error", windfarm_id=wf_id, error=str(e)
                )
                continue

        await self.db.flush()
        return all_opportunities

    async def _detect_windfarm(
        self,
        windfarm_id: int,
        period_start: datetime,
        period_end: datetime,
        detection_run_id: Optional[int],
    ) -> List[Opportunity]:
        """Run all schemas for a single windfarm in dependency order."""
        opps: List[Opportunity] = []

        # Load PPA info for suppression checks
        ppa_info = await self._load_ppa_info(windfarm_id)

        # OPS-01: Volatile disruptions
        ops01 = await self._detect_ops01(
            windfarm_id, period_start, period_end, ppa_info, detection_run_id
        )
        if ops01:
            opps.append(ops01)

        # OPS-02: Performance seasonality
        ops02 = await self._detect_ops02(
            windfarm_id, period_start, period_end, ppa_info, detection_run_id
        )
        if ops02:
            opps.append(ops02)

        # OPS-03: Misaligned contracting (only if OPS-01 triggered)
        if ops01:
            ops03 = await self._detect_ops03(
                windfarm_id, period_start, period_end, ppa_info, ops01, detection_run_id
            )
            if ops03:
                opps.append(ops03)

        # MKT-01: Low capture rates
        mkt01 = await self._detect_mkt01(
            windfarm_id, period_start, period_end, ppa_info, detection_run_id
        )
        if mkt01:
            opps.append(mkt01)

        # MKT-03: High cannibalisation (independent of MKT-01)
        mkt03 = await self._detect_mkt03(
            windfarm_id, period_start, period_end, ppa_info, detection_run_id
        )
        if mkt03:
            opps.append(mkt03)

        # MKT-02: Storage opportunity (only if MKT-01 triggered)
        if mkt01:
            mkt02 = await self._detect_mkt02(
                windfarm_id, period_start, period_end, ppa_info, mkt01, detection_run_id
            )
            if mkt02:
                opps.append(mkt02)

        return opps

    async def _run_registry(
        self,
        windfarm_id: int,
        period_start: datetime,
        period_end: datetime,
        detection_run_id: Optional[int],
    ) -> List[Opportunity]:
        """Registry-based detection seam (NOT the live path yet — see #90/#93).

        Builds a ``DetectionContext`` and delegates to ``run_for_windfarm``, the
        single ORM-build / persist point. With the registry empty (#90) this is a
        tested no-op returning ``[]``; #92/#93 register the six detectors here and
        flip ``_detect_windfarm`` to call this instead of the legacy inline
        detectors. Kept separate so that cutover is a one-line change and the
        characterization snapshot (#91) stays byte-identical until then.
        """
        from app.services.opportunity_schemas.context import DetectionContext
        from app.services.opportunity_schemas.registry import run_for_windfarm

        ctx = DetectionContext(
            db=self.db,
            windfarm=windfarm_id,
            period_start=period_start,
            period_end=period_end,
        )
        return await run_for_windfarm(ctx, detection_run_id=detection_run_id)

    # ─── Schema detectors ──────────────────────────────────────────

    async def _detect_ops01(
        self,
        windfarm_id: int,
        period_start: datetime,
        period_end: datetime,
        ppa_info: dict,
        detection_run_id: Optional[int],
    ) -> Optional[Opportunity]:
        """OPS-01: Volatile disruption periods."""
        monthly = await self._calc_monthly_availability(windfarm_id, period_start, period_end)
        if not monthly:
            return None

        low_months = [m for m in monthly if m["availability_pct"] < ODI_THRESHOLD_PCT]
        severity = self.determine_ops01_severity(len(low_months))
        if severity is None:
            return None

        # Gather data slots
        data_slots = {
            "odi_pct": round(sum(m["availability_pct"] for m in monthly) / len(monthly), 2)
            if monthly
            else None,
            "odi_months_below_threshold": len(low_months),
            "odi_threshold": ODI_THRESHOLD_PCT,
            "period": f"{period_start.date()} to {period_end.date()}",
            "disruption_month_list": [m["month"] for m in low_months],
            "ppa_status": ppa_info.get("ppa_status"),
        }
        missing = []
        if not ppa_info.get("ppa_status"):
            missing.append("ppa_status")
        missing.extend(["peer_odi_p50", "maintenance_schedule", "wind_resource_index"])

        # Branch selection
        years_affected = len(set(m["month"][:4] for m in low_months))
        has_spot = ppa_info.get("contract_type") in (None, "merchant", "indexed")
        branch = self.select_ops01_branch(low_months, years_affected, has_spot)

        # Suppression
        suppression = self.check_ops01_suppression(ppa_info, data_slots)
        if suppression:
            return None

        # Graceful degradation: without wind_resource_index, can't confirm operational cause
        if severity == Severity.CONFIRMED and "wind_resource_index" in missing:
            severity = Severity.INDICATIVE

        opp = Opportunity(
            windfarm_id=windfarm_id,
            schema_code=SchemaCode.OPS_01,
            severity=severity,
            branch=branch,
            status=OpportunityStatus.ACTIVE,
            data_slots=data_slots,
            missing_slots=missing,
            detection_period_start=period_start,
            detection_period_end=period_end,
            detection_run_id=detection_run_id,
        )
        self.db.add(opp)
        await self.db.flush()
        return opp

    async def _detect_ops02(
        self,
        windfarm_id: int,
        period_start: datetime,
        period_end: datetime,
        ppa_info: dict,
        detection_run_id: Optional[int],
    ) -> Optional[Opportunity]:
        """OPS-02: Performance seasonality."""
        seasonal = await self._calc_seasonal_capture(windfarm_id, period_start, period_end)
        if (
            not seasonal
            or seasonal.get("high_wind_cf") is None
            or seasonal.get("low_wind_cf") is None
        ):
            return None

        gap_pp = (seasonal["low_wind_cf"] - seasonal["high_wind_cf"]) * 100
        if gap_pp <= 0:
            return None  # No inversion — high-wind season performs better (expected)

        years_observed = seasonal.get("years_with_inversion", 0)
        severity = self.determine_ops02_severity(gap_pp, years_observed)
        if severity is None:
            return None

        data_slots = {
            "high_wind_season_capture": round(seasonal["high_wind_cf"] * 100, 2),
            "low_wind_season_capture": round(seasonal["low_wind_cf"] * 100, 2),
            "seasonal_gap_pp": round(gap_pp, 2),
            "years_with_inversion": years_observed,
            "period": f"{period_start.date()} to {period_end.date()}",
        }
        missing = [
            "wind_resource_index_monthly",
            "turbine_scatter_spread",
            "cannibalisation_index_seasonal",
            "maintenance_calendar",
            "revenue_uplift_potential_eur",
        ]

        # Without wind resource index, can't confirm operational cause
        if "wind_resource_index_monthly" in missing and severity != Severity.WATCH:
            severity = Severity.WATCH

        branch = "C"  # Default to data-limited without turbine scatter or maintenance data
        missing_set = set(missing)
        if "turbine_scatter_spread" not in missing_set:
            branch = "A"
        elif "maintenance_calendar" not in missing_set:
            branch = "B"

        opp = Opportunity(
            windfarm_id=windfarm_id,
            schema_code=SchemaCode.OPS_02,
            severity=severity,
            branch=branch,
            status=OpportunityStatus.ACTIVE,
            data_slots=data_slots,
            missing_slots=missing,
            detection_period_start=period_start,
            detection_period_end=period_end,
            detection_run_id=detection_run_id,
        )
        self.db.add(opp)
        await self.db.flush()
        return opp

    async def _detect_ops03(
        self,
        windfarm_id: int,
        period_start: datetime,
        period_end: datetime,
        ppa_info: dict,
        ops01: Opportunity,
        detection_run_id: Optional[int],
    ) -> Optional[Opportunity]:
        """OPS-03: Misaligned contracting strategies. Only fires if OPS-01 triggered."""
        contract_type = ppa_info.get("contract_type")
        has_penalties = ppa_info.get("has_availability_penalties")

        data_slots = {
            "odi_pct": ops01.data_slots.get("odi_pct"),
            "contract_type": contract_type,
            "has_availability_penalties": has_penalties,
            "period": f"{period_start.date()} to {period_end.date()}",
            "ppa_status": ppa_info.get("ppa_status"),
        }
        missing = []
        if contract_type is None:
            missing.append("contract_type")
        if has_penalties is None:
            missing.append("contract_penalty_clauses")
        missing.extend(
            [
                "oem_response_time",
                "am_location",
                "peer_odi_p50",
                "insource_benchmark",
                "asset_age_years",
            ]
        )

        # Suppression: if contract has ODI-linked availability guarantees
        if has_penalties is True:
            return None

        # Severity
        if contract_type and has_penalties is False and ops01.severity == Severity.CONFIRMED:
            severity = Severity.CONFIRMED
        elif contract_type and ops01.severity in (Severity.CONFIRMED, Severity.INDICATIVE):
            severity = Severity.INDICATIVE
        else:
            severity = Severity.WATCH

        # Branch
        if contract_type and has_penalties is False:
            branch = "A"  # Incentive misalignment
        elif contract_type is None:
            branch = "C"  # Data-limited
        else:
            branch = "C"  # Default

        opp = Opportunity(
            windfarm_id=windfarm_id,
            schema_code=SchemaCode.OPS_03,
            severity=severity,
            branch=branch,
            status=OpportunityStatus.ACTIVE,
            data_slots=data_slots,
            missing_slots=missing,
            triggered_by_id=ops01.id,
            detection_period_start=period_start,
            detection_period_end=period_end,
            detection_run_id=detection_run_id,
        )
        self.db.add(opp)
        await self.db.flush()
        return opp

    async def _detect_mkt01(
        self,
        windfarm_id: int,
        period_start: datetime,
        period_end: datetime,
        ppa_info: dict,
        detection_run_id: Optional[int],
    ) -> Optional[Opportunity]:
        """MKT-01: Low capture rates — contracting."""
        gap_data = await self._calc_capture_rate_gap(windfarm_id, period_start, period_end)
        if gap_data is None:
            return None

        gap_pp = gap_data["gap_pp"]
        severity = self.determine_mkt01_severity(gap_pp)
        if severity is None:
            return None

        # Suppression
        suppression = self.check_mkt01_suppression(ppa_info, gap_data)
        if suppression:
            return None

        # CI for branch selection
        ci_data = await self._calc_cannibalisation_index(windfarm_id, period_start, period_end)
        ci = ci_data.get("ci_latest") if ci_data else None

        data_slots = {
            "capture_rate": gap_data.get("capture_rate"),
            "zone_avg_capture": gap_data.get("zone_avg"),
            "gap_pp": round(gap_pp, 2),
            "price_zone": gap_data.get("bidzone_code"),
            "ppa_status": ppa_info.get("ppa_status"),
            "cannibalisation_index": round(ci, 4) if ci else None,
            "ppa_expiry_date": str(ppa_info.get("ppa_end_date"))
            if ppa_info.get("ppa_end_date")
            else None,
            "period": f"{period_start.date()} to {period_end.date()}",
        }
        missing = []
        if ci is None:
            missing.append("cannibalisation_index")
        missing.extend(
            ["pcc_slope", "peer_capture_p50", "revenue_impact_eur", "high_wind_capture_delta"]
        )
        if not ppa_info.get("ppa_end_date"):
            missing.append("ppa_expiry_date")

        # Reclassification: if CI is dominant driver, reclassify to MKT-03
        if ci and ci > MKT03_CI_CONFIRMED:
            # MKT-03 will handle this — skip MKT-01
            return None

        # Branch selection
        branch = self.select_mkt01_branch(ci, ppa_info)

        opp = Opportunity(
            windfarm_id=windfarm_id,
            schema_code=SchemaCode.MKT_01,
            severity=severity,
            branch=branch,
            status=OpportunityStatus.ACTIVE,
            data_slots=data_slots,
            missing_slots=missing,
            detection_period_start=period_start,
            detection_period_end=period_end,
            detection_run_id=detection_run_id,
        )
        self.db.add(opp)
        await self.db.flush()
        return opp

    async def _detect_mkt02(
        self,
        windfarm_id: int,
        period_start: datetime,
        period_end: datetime,
        ppa_info: dict,
        mkt01: Opportunity,
        detection_run_id: Optional[int],
    ) -> Optional[Opportunity]:
        """MKT-02: Low capture rates — storage. Only fires if MKT-01 triggered."""
        # We don't have BESS/MFRR data yet, so this fires at WATCH with graceful degradation
        data_slots = {
            "storage_present": False,  # Assumed — no BESS data
            "price_zone": mkt01.data_slots.get("price_zone"),
            "mkt01_severity": mkt01.severity,
            "ppa_status": ppa_info.get("ppa_status"),
            "period": f"{period_start.date()} to {period_end.date()}",
        }
        missing = [
            "intraday_price_spread",
            "mfrr_eligible",
            "grid_headroom_mw",
            "bess_revenue_potential_eur",
            "optimal_bess_size_mwh",
        ]

        # Severity follows MKT-01 but capped due to missing data
        if mkt01.severity == Severity.CONFIRMED:
            severity = Severity.INDICATIVE  # Downgrade: no storage data
        else:
            severity = Severity.WATCH

        branch = "C"  # Feasibility-limited — no grid/BESS data

        opp = Opportunity(
            windfarm_id=windfarm_id,
            schema_code=SchemaCode.MKT_02,
            severity=severity,
            branch=branch,
            status=OpportunityStatus.ACTIVE,
            data_slots=data_slots,
            missing_slots=missing,
            triggered_by_id=mkt01.id,
            detection_period_start=period_start,
            detection_period_end=period_end,
            detection_run_id=detection_run_id,
        )
        self.db.add(opp)
        await self.db.flush()
        return opp

    async def _detect_mkt03(
        self,
        windfarm_id: int,
        period_start: datetime,
        period_end: datetime,
        ppa_info: dict,
        detection_run_id: Optional[int],
    ) -> Optional[Opportunity]:
        """MKT-03: High cannibalisation rates."""
        ci_data = await self._calc_cannibalisation_index(windfarm_id, period_start, period_end)
        if not ci_data or ci_data.get("ci_latest") is None:
            return None

        ci = ci_data["ci_latest"]
        years_sustained = ci_data.get("years_above_threshold", 0)
        severity = self.determine_mkt03_severity(ci, years_sustained)
        if severity is None:
            return None

        # Suppression: long-dated fixed PPA
        if self.check_mkt03_suppression(ppa_info):
            return None

        data_slots = {
            "cannibalisation_index": round(ci, 4),
            "price_zone": ci_data.get("bidzone_code"),
            "ci_values_by_year": ci_data.get("ci_by_year"),
            "ci_trend_yoy": ci_data.get("ci_trend"),
            "ppa_status": ppa_info.get("ppa_status"),
            "period": f"{period_start.date()} to {period_end.date()}",
        }
        missing = [
            "zone_renewable_penetration_pct",
            "peer_zone_ci",
            "portfolio_zone_correlation",
            "revenue_impact_eur",
            "alternative_zone_assets",
        ]

        # Graceful degradation: without CI trend, downgrade
        if ci_data.get("ci_trend") is None and severity == Severity.CONFIRMED:
            severity = Severity.INDICATIVE

        # Branch
        branch = self.select_mkt03_branch(ci_data)

        opp = Opportunity(
            windfarm_id=windfarm_id,
            schema_code=SchemaCode.MKT_03,
            severity=severity,
            branch=branch,
            status=OpportunityStatus.ACTIVE,
            data_slots=data_slots,
            missing_slots=missing,
            detection_period_start=period_start,
            detection_period_end=period_end,
            detection_run_id=detection_run_id,
        )
        self.db.add(opp)
        await self.db.flush()
        return opp

    # ─── Calculations ──────────────────────────────────────────────

    async def _calc_monthly_availability(
        self, windfarm_id: int, start: datetime, end: datetime
    ) -> List[dict]:
        """Calculate monthly ODI metrics.

        First tries real ODI from performance_summaries (power-curve-based,
        Module 3 pipeline). Falls back to simple availability proxy if pipeline
        hasn't run yet.
        """
        # Try real ODI from performance pipeline
        try:
            from app.models.performance_summary import PerformanceSummary

            result = await self.db.execute(
                select(PerformanceSummary)
                .where(
                    PerformanceSummary.windfarm_id == windfarm_id,
                    PerformanceSummary.period_type == "month",
                    PerformanceSummary.year >= start.year,
                    PerformanceSummary.year <= end.year,
                    PerformanceSummary.odi_pct_underperf.isnot(None),
                )
                .order_by(PerformanceSummary.year, PerformanceSummary.month)
            )
            summaries = result.scalars().all()
            if summaries:
                return [
                    {
                        "month": f"{s.year}-{s.month:02d}",
                        "gen_hours": (s.total_hours or 0) - (s.underperf_hours or 0),
                        "total_hours": s.total_hours or 0,
                        "availability_pct": 100.0 - float(s.odi_pct_underperf or 0),
                    }
                    for s in summaries
                ]
        except Exception:
            pass  # Fall back to proxy

        # Fallback: simple availability proxy (hours with generation > 0)
        query = text(
            """
            WITH monthly AS (
                SELECT
                    TO_CHAR(hour, 'YYYY-MM') as month,
                    COUNT(*) FILTER (WHERE generation_mwh > 0) as gen_hours,
                    COUNT(*) as total_hours
                FROM generation_data
                WHERE windfarm_id = :wf_id
                  AND hour >= :start AND hour < :end
                  AND is_ramp_up = false
                GROUP BY TO_CHAR(hour, 'YYYY-MM')
                ORDER BY month
            )
            SELECT month, gen_hours, total_hours,
                   ROUND(gen_hours * 100.0 / NULLIF(total_hours, 0), 2) as availability_pct
            FROM monthly
        """
        )
        result = await self.db.execute(query, {"wf_id": windfarm_id, "start": start, "end": end})
        rows = result.fetchall()
        return [
            {
                "month": r.month,
                "gen_hours": r.gen_hours,
                "total_hours": r.total_hours,
                "availability_pct": float(r.availability_pct) if r.availability_pct else 0.0,
            }
            for r in rows
        ]

    async def _calc_capture_rate_gap(
        self, windfarm_id: int, start: datetime, end: datetime
    ) -> Optional[dict]:
        """Calculate capture rate gap vs zone average using existing PriceAnalyticsService."""
        try:
            cr_data = await self.price_analytics.calculate_capture_rate(
                windfarm_id=windfarm_id,
                start_date=start,
                end_date=end,
                aggregation="year",
            )
        except Exception as e:
            logger.warning("opportunity_capture_rate_error", windfarm_id=windfarm_id, error=str(e))
            return None

        wf_capture = cr_data.get("overall", {}).get("capture_rate")
        if wf_capture is None:
            return None

        # Get zone average
        wf_result = await self.db.execute(
            select(Windfarm.bidzone_id).where(Windfarm.id == windfarm_id)
        )
        bidzone_id = wf_result.scalar_one_or_none()
        if not bidzone_id:
            return None

        try:
            zone_data = await self.price_analytics.compare_capture_rates_by_bidzone(
                bidzone_id=bidzone_id,
                start_date=start,
                end_date=end,
            )
        except Exception as e:
            logger.warning("opportunity_zone_capture_error", bidzone_id=bidzone_id, error=str(e))
            return None

        zone_avg = zone_data.get("zone_average_capture_rate")
        if zone_avg is None:
            return None

        gap_pp = (zone_avg - wf_capture) * 100  # Positive = underperforming

        # Get bidzone code for data_slots
        from app.models.bidzone import Bidzone

        bz_result = await self.db.execute(select(Bidzone.code).where(Bidzone.id == bidzone_id))
        bz_code = bz_result.scalar_one_or_none()

        return {
            "capture_rate": round(wf_capture, 4),
            "zone_avg": round(zone_avg, 4),
            "gap_pp": round(gap_pp, 2),
            "bidzone_code": bz_code,
        }

    async def _calc_cannibalisation_index(
        self, windfarm_id: int, start: datetime, end: datetime
    ) -> Optional[dict]:
        """Calculate cannibalisation index = 1/capture_rate per year."""
        try:
            cr_data = await self.price_analytics.calculate_capture_rate(
                windfarm_id=windfarm_id,
                start_date=start,
                end_date=end,
                aggregation="year",
            )
        except Exception:
            return None

        periods = cr_data.get("periods", [])
        if not periods:
            return None

        ci_by_year = {}
        for p in periods:
            cr = p.get("capture_rate")
            if cr and cr > 0:
                year = p["period"][:4] if p.get("period") else None
                if year:
                    ci_by_year[year] = round(1.0 / cr, 4)

        if not ci_by_year:
            return None

        sorted_years = sorted(ci_by_year.keys())
        ci_latest = ci_by_year[sorted_years[-1]]

        # CI trend: positive = worsening
        ci_trend = None
        if len(sorted_years) >= 2:
            first_ci = ci_by_year[sorted_years[0]]
            last_ci = ci_by_year[sorted_years[-1]]
            ci_trend = round(last_ci - first_ci, 4)

        years_above = sum(1 for v in ci_by_year.values() if v >= MKT03_CI_WATCH)

        # Get bidzone code
        wf_result = await self.db.execute(
            select(Windfarm.bidzone_id).where(Windfarm.id == windfarm_id)
        )
        bidzone_id = wf_result.scalar_one_or_none()
        bz_code = None
        if bidzone_id:
            from app.models.bidzone import Bidzone

            bz_result = await self.db.execute(select(Bidzone.code).where(Bidzone.id == bidzone_id))
            bz_code = bz_result.scalar_one_or_none()

        return {
            "ci_latest": ci_latest,
            "ci_by_year": ci_by_year,
            "ci_trend": ci_trend,
            "years_above_threshold": years_above,
            "bidzone_code": bz_code,
        }

    async def _calc_seasonal_capture(
        self, windfarm_id: int, start: datetime, end: datetime
    ) -> Optional[dict]:
        """Compare high-wind vs low-wind season capacity factors."""
        # High-wind months: Oct-Mar, Low-wind: Apr-Sep (Northern hemisphere default)
        query = text(
            """
            WITH seasonal AS (
                SELECT
                    EXTRACT(YEAR FROM hour) as year,
                    CASE
                        WHEN EXTRACT(MONTH FROM hour) IN (10,11,12,1,2,3) THEN 'high'
                        ELSE 'low'
                    END as season,
                    AVG(capacity_factor) as avg_cf
                FROM generation_data
                WHERE windfarm_id = :wf_id
                  AND hour >= :start AND hour < :end
                  AND capacity_factor IS NOT NULL
                  AND is_ramp_up = false
                GROUP BY EXTRACT(YEAR FROM hour),
                    CASE WHEN EXTRACT(MONTH FROM hour) IN (10,11,12,1,2,3) THEN 'high' ELSE 'low' END
            )
            SELECT season, AVG(avg_cf) as overall_cf,
                   COUNT(*) FILTER (WHERE season = 'high') as high_count
            FROM seasonal
            GROUP BY season
        """
        )
        result = await self.db.execute(query, {"wf_id": windfarm_id, "start": start, "end": end})
        rows = {r.season: float(r.overall_cf) if r.overall_cf else None for r in result.fetchall()}

        if "high" not in rows or "low" not in rows:
            return None

        # Count years where inversion exists (low > high)
        inv_query = text(
            """
            WITH yearly_seasonal AS (
                SELECT
                    EXTRACT(YEAR FROM hour) as year,
                    AVG(capacity_factor) FILTER (WHERE EXTRACT(MONTH FROM hour) IN (10,11,12,1,2,3)) as high_cf,
                    AVG(capacity_factor) FILTER (WHERE EXTRACT(MONTH FROM hour) IN (4,5,6,7,8,9)) as low_cf
                FROM generation_data
                WHERE windfarm_id = :wf_id
                  AND hour >= :start AND hour < :end
                  AND capacity_factor IS NOT NULL
                  AND is_ramp_up = false
                GROUP BY EXTRACT(YEAR FROM hour)
            )
            SELECT COUNT(*) as years_inverted
            FROM yearly_seasonal
            WHERE low_cf > high_cf
        """
        )
        inv_result = await self.db.execute(
            inv_query, {"wf_id": windfarm_id, "start": start, "end": end}
        )
        inv_row = inv_result.fetchone()

        return {
            "high_wind_cf": rows.get("high"),
            "low_wind_cf": rows.get("low"),
            "years_with_inversion": inv_row.years_inverted if inv_row else 0,
        }

    async def _load_ppa_info(self, windfarm_id: int) -> dict:
        """Load PPA info for suppression and branch selection."""
        result = await self.db.execute(
            select(PPA)
            .where(PPA.windfarm_id == windfarm_id)
            .order_by(PPA.ppa_end_date.desc().nullslast())
        )
        ppa = result.scalars().first()
        if not ppa:
            return {}
        return {
            "ppa_buyer": ppa.ppa_buyer,
            "ppa_size_mw": float(ppa.ppa_size_mw) if ppa.ppa_size_mw else None,
            "ppa_start_date": ppa.ppa_start_date,
            "ppa_end_date": ppa.ppa_end_date,
            "ppa_duration_years": ppa.ppa_duration_years,
            "contract_type": ppa.contract_type,
            "ppa_status": ppa.ppa_status,
            "ppa_price_eur_mwh": float(ppa.ppa_price_eur_mwh) if ppa.ppa_price_eur_mwh else None,
            "has_availability_penalties": ppa.has_availability_penalties,
        }

    # ─── Pure logic (static, testable without DB) ──────────────────

    @staticmethod
    def determine_ops01_severity(months_below_threshold: int) -> Optional[str]:
        """OPS-01 severity: 3+ months = CONFIRMED, 2 = INDICATIVE, 1 = WATCH."""
        if months_below_threshold >= 3:
            return Severity.CONFIRMED
        elif months_below_threshold == 2:
            return Severity.INDICATIVE
        elif months_below_threshold == 1:
            return Severity.WATCH
        return None

    @staticmethod
    def determine_ops02_severity(gap_pp: float, years_observed: int) -> Optional[str]:
        """OPS-02 severity based on seasonal gap and years observed."""
        if gap_pp >= OPS02_SEASONAL_GAP_CONFIRMED_PP and years_observed >= 2:
            return Severity.CONFIRMED
        elif gap_pp >= OPS02_SEASONAL_GAP_CONFIRMED_PP or years_observed >= 1:
            return Severity.INDICATIVE
        elif gap_pp >= OPS02_SEASONAL_GAP_MARGINAL_PP:
            return Severity.WATCH
        return None

    @staticmethod
    def determine_mkt01_severity(gap_pp: float) -> Optional[str]:
        """MKT-01 severity: >10pp = CONFIRMED, 5-10 = INDICATIVE, 2-5 = WATCH."""
        if gap_pp > MKT01_GAP_CONFIRMED_PP:
            return Severity.CONFIRMED
        elif gap_pp > MKT01_GAP_INDICATIVE_PP:
            return Severity.INDICATIVE
        elif gap_pp > MKT01_GAP_WATCH_PP:
            return Severity.WATCH
        return None

    @staticmethod
    def determine_mkt03_severity(ci: float, years_sustained: int) -> Optional[str]:
        """MKT-03 severity based on cannibalisation index and sustained years."""
        if ci >= MKT03_CI_CONFIRMED and years_sustained >= 2:
            return Severity.CONFIRMED
        elif ci >= MKT03_CI_INDICATIVE:
            return Severity.INDICATIVE
        elif ci >= MKT03_CI_WATCH:
            return Severity.WATCH
        return None

    @staticmethod
    def select_ops01_branch(
        low_months: List[dict], years_affected: int, has_spot_exposure: bool
    ) -> str:
        """Select OPS-01 root cause branch."""
        if has_spot_exposure and len(low_months) >= 2:
            return "C"  # Exposure-amplified
        if years_affected >= 2:
            return "B"  # Structural/recurring
        return "A"  # Event-driven

    @staticmethod
    def select_mkt01_branch(ci: Optional[float], ppa_info: dict) -> str:
        """Select MKT-01 root cause branch."""
        # Branch A: Profile mismatch (high CI)
        if ci and ci >= MKT03_CI_WATCH:
            return "A"
        # Branch B: PPA structure (expiry within 24 months)
        ppa_end = ppa_info.get("ppa_end_date")
        if ppa_end and isinstance(ppa_end, date):
            months_to_expiry = (ppa_end - date.today()).days / 30
            if months_to_expiry <= 24:
                return "B"
        # Branch C: Zone dynamics
        return "C"

    @staticmethod
    def select_mkt03_branch(ci_data: dict) -> str:
        """Select MKT-03 root cause branch."""
        ci_trend = ci_data.get("ci_trend")
        # Branch A: Zone structural (worsening trend)
        if ci_trend is not None and ci_trend > 0.02:
            return "A"
        # Branch B would need portfolio_zone_correlation (missing)
        # Branch C: Asset-level anomaly (default)
        return "C"

    @staticmethod
    def check_ops01_suppression(ppa_info: dict, data_slots: dict) -> Optional[str]:
        """Check OPS-01 suppression conditions."""
        # Downgrade if fixed PPA limits revenue impact
        if (
            ppa_info.get("contract_type") == "fixed_price"
            and ppa_info.get("ppa_duration_years", 0) >= LONG_PPA_YEARS
        ):
            return None  # Don't suppress but would downgrade — handled in detector
        return None

    @staticmethod
    def check_mkt01_suppression(ppa_info: dict, gap_data: dict) -> Optional[str]:
        """Check MKT-01 suppression conditions."""
        # Suppress if PPA is fixed-price and long-dated
        if (
            ppa_info.get("contract_type") == "fixed_price"
            and ppa_info.get("ppa_duration_years", 0) >= LONG_PPA_YEARS
            and ppa_info.get("ppa_status") == "active"
        ):
            return "Fixed-price PPA with >5yr duration — market exposure locked"
        return None

    @staticmethod
    def check_mkt03_suppression(ppa_info: dict) -> bool:
        """Check MKT-03 suppression: long-dated fixed PPA."""
        if (
            ppa_info.get("contract_type") == "fixed_price"
            and ppa_info.get("ppa_duration_years", 0) >= LONG_PPA_YEARS
            and ppa_info.get("ppa_status") == "active"
        ):
            return True
        return False
