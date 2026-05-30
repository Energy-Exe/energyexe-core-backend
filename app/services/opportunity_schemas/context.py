"""DetectionContext + DetectorResult — the per-windfarm data + return contract.

Every detector in the ``opportunity_schemas`` package receives a single
``DetectionContext`` and returns ``Optional[DetectorResult]``. The context is
built once per windfarm by the orchestrator and exposes **memoized async
accessors** that wrap the upstream queries each detector needs. Today MKT-01 and
MKT-03 in the legacy monolith redundantly recompute the capture rate /
cannibalisation index per detector; centralising the queries here means one DB
hit per accessor per windfarm.

The accessor bodies are copied verbatim (query text + result shape) from the
legacy ``OpportunityDetectionService._calc_*`` / ``_load_ppa_info`` methods
(``app/services/opportunity_detection_service.py``). The legacy methods are left
in place — detector migration happens in later issues (#92/#93).

Test-injection contract
========================
Detector tests (issues #92–#112) MUST be able to run without a live Postgres.
``DetectionContext`` memoizes every accessor result into ``self._cache`` keyed by
a stable string. The constructor accepts an optional ``prefetched`` dict that
pre-populates ``self._cache``; any key present there short-circuits the DB query
entirely. So a DB-free test does::

    ctx = DetectionContext(
        db=AsyncMock(),
        windfarm=fake_wf,
        period_start=start,
        period_end=end,
        prefetched={
            "capture_rate": {"capture_rate": 0.62, "zone_avg": 0.69, "gap_pp": 7.0},
            "monthly_performance": [...],
        },
    )
    result = await ctx.load_capture_rate()   # returns the injected dict, no DB

Cache keys (stable — downstream tests depend on these):
    "ppa_info", "monthly_performance", "capture_rate", "cannibalisation_index".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.opportunity import SchemaCode, Severity


@dataclass
class DetectorResult:
    """The return type of every detector's ``detect(ctx)`` entrypoint.

    A detector returns ``None`` to mean "no finding". A non-None
    ``DetectorResult`` is converted into an ``Opportunity`` ORM row by the
    orchestrator (the sole place that touches the DB / wires ``triggered_by_id``).

    Fields:
        schema_code: which schema produced this finding.
        severity: the severity tier. A suppressed finding uses
            ``Severity.SUPPRESSED`` and should set ``suppression_reason``.
        branch: optional root-cause branch label (e.g. "A"/"B"/"C").
        data_slots: computed metrics surfaced on the opportunity.
        missing_slots: data slots the detector could not populate
            (graceful-degradation tracking).
        suppression_reason: human-readable reason when severity is SUPPRESSED.
    """

    schema_code: SchemaCode
    severity: Severity
    branch: Optional[str] = None
    data_slots: dict = field(default_factory=dict)
    missing_slots: list = field(default_factory=list)
    suppression_reason: Optional[str] = None


class DetectionContext:
    """Per-windfarm data context with memoized async accessors.

    Constructed once per windfarm by the orchestrator. Each ``load_*`` accessor
    runs its query at most once and caches the result in ``self._cache``; pass
    ``prefetched`` to inject values for DB-free tests (see module docstring).
    """

    def __init__(
        self,
        db: AsyncSession,
        windfarm: Any,
        period_start: datetime,
        period_end: datetime,
        prefetched: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Build a context.

        Args:
            db: async session (may be a mock in tests).
            windfarm: the Windfarm ORM object, or a bare windfarm id (int). The
                ``windfarm_id`` property normalizes either form.
            period_start: detection period start (datetime, used as a bind param).
            period_end: detection period end (datetime, used as a bind param).
            prefetched: optional pre-seeded cache; keys present here are returned
                by their accessor without any DB access.
        """
        self.db = db
        self.windfarm = windfarm
        self.period_start = period_start
        self.period_end = period_end
        self._cache: Dict[str, Any] = dict(prefetched) if prefetched else {}

    @property
    def windfarm_id(self) -> int:
        """Windfarm id, whether constructed with an ORM object or a bare int."""
        wf = self.windfarm
        if isinstance(wf, int):
            return wf
        return wf.id

    # ─── Memoized accessors (query text copied from legacy _calc_*) ─────────

    async def load_ppa_info(self) -> dict:
        """Load PPA info for suppression / branch selection.

        Mirrors legacy ``_load_ppa_info``: latest PPA (by end date) for the
        windfarm, or ``{}`` when none exists.
        """
        if "ppa_info" in self._cache:
            return self._cache["ppa_info"]

        from app.models.ppa import PPA

        result = await self.db.execute(
            select(PPA)
            .where(PPA.windfarm_id == self.windfarm_id)
            .order_by(PPA.ppa_end_date.desc().nullslast())
        )
        ppa = result.scalars().first()
        if not ppa:
            self._cache["ppa_info"] = {}
            return self._cache["ppa_info"]

        self._cache["ppa_info"] = {
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
        return self._cache["ppa_info"]

    async def load_monthly_performance(self) -> List[dict]:
        """Monthly ODI metrics (mirrors legacy ``_calc_monthly_availability``).

        First tries real ODI from ``performance_summaries`` (power-curve-based,
        Module 3 pipeline); falls back to a simple availability proxy (hours with
        generation > 0) if the pipeline hasn't run. Same rows/keys as the legacy
        method: ``month``, ``gen_hours``, ``total_hours``, ``availability_pct``.
        """
        if "monthly_performance" in self._cache:
            return self._cache["monthly_performance"]

        windfarm_id = self.windfarm_id
        start = self.period_start
        end = self.period_end

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
                self._cache["monthly_performance"] = [
                    {
                        "month": f"{s.year}-{s.month:02d}",
                        "gen_hours": (s.total_hours or 0) - (s.underperf_hours or 0),
                        "total_hours": s.total_hours or 0,
                        "availability_pct": 100.0 - float(s.odi_pct_underperf or 0),
                    }
                    for s in summaries
                ]
                return self._cache["monthly_performance"]
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
        self._cache["monthly_performance"] = [
            {
                "month": r.month,
                "gen_hours": r.gen_hours,
                "total_hours": r.total_hours,
                "availability_pct": float(r.availability_pct) if r.availability_pct else 0.0,
            }
            for r in rows
        ]
        return self._cache["monthly_performance"]

    async def load_capture_rate(self) -> Optional[dict]:
        """Capture rate gap vs zone average (mirrors ``_calc_capture_rate_gap``).

        Returns ``None`` if the windfarm capture rate, its bidzone, or the zone
        average are unavailable. Otherwise a dict with ``capture_rate``,
        ``zone_avg``, ``gap_pp`` (positive = underperforming), ``bidzone_code``.
        """
        if "capture_rate" in self._cache:
            return self._cache["capture_rate"]

        self._cache["capture_rate"] = await self._compute_capture_rate()
        return self._cache["capture_rate"]

    async def _compute_capture_rate(self) -> Optional[dict]:
        import structlog

        from app.models.windfarm import Windfarm

        logger = structlog.get_logger(__name__)
        windfarm_id = self.windfarm_id
        start = self.period_start
        end = self.period_end
        price_analytics = self._price_analytics()

        try:
            cr_data = await price_analytics.calculate_capture_rate(
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

        wf_result = await self.db.execute(
            select(Windfarm.bidzone_id).where(Windfarm.id == windfarm_id)
        )
        bidzone_id = wf_result.scalar_one_or_none()
        if not bidzone_id:
            return None

        try:
            zone_data = await price_analytics.compare_capture_rates_by_bidzone(
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

        from app.models.bidzone import Bidzone

        bz_result = await self.db.execute(select(Bidzone.code).where(Bidzone.id == bidzone_id))
        bz_code = bz_result.scalar_one_or_none()

        return {
            "capture_rate": round(wf_capture, 4),
            "zone_avg": round(zone_avg, 4),
            "gap_pp": round(gap_pp, 2),
            "bidzone_code": bz_code,
        }

    async def load_cannibalisation_index(self) -> Optional[dict]:
        """Cannibalisation index = 1/capture_rate per year.

        Mirrors legacy ``_calc_cannibalisation_index``. Returns ``None`` when no
        positive yearly capture rate exists. Otherwise a dict with ``ci_latest``,
        ``ci_by_year``, ``ci_trend`` (positive = worsening),
        ``years_above_threshold``, ``bidzone_code``.
        """
        if "cannibalisation_index" in self._cache:
            return self._cache["cannibalisation_index"]

        self._cache["cannibalisation_index"] = await self._compute_cannibalisation_index()
        return self._cache["cannibalisation_index"]

    async def _compute_cannibalisation_index(self) -> Optional[dict]:
        from app.models.windfarm import Windfarm
        from app.services.opportunity_detection_service import MKT03_CI_WATCH

        windfarm_id = self.windfarm_id
        start = self.period_start
        end = self.period_end
        price_analytics = self._price_analytics()

        try:
            cr_data = await price_analytics.calculate_capture_rate(
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

        ci_by_year: Dict[str, float] = {}
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

    # ─── Accessors deferred to later issues ────────────────────────────────
    #
    # The following memoized accessors are part of the DetectionContext surface
    # per the plan but are added by their respective downstream issues (each
    # alongside the detector that needs it). They are intentionally NOT stubbed
    # here to avoid shipping broken bodies:
    #
    #   load_degradation_result()          — added by #99  (OPS-04 turbine degradation)
    #   load_structural_constraint_flags() — added by #103 (OPS-08 structural constraint)
    #   load_generation_gaps()             — added by #109 (DQ-01 generation data gaps)
    #   compute_zone_opex_median(location_type) — added by #108 (FIN-02/03 OPEX overrun)
    #
    # ────────────────────────────────────────────────────────────────────────

    def _price_analytics(self):
        """Lazily build a PriceAnalyticsService bound to this context's session."""
        from app.services.price_analytics_service import PriceAnalyticsService

        if not hasattr(self, "_price_analytics_svc"):
            self._price_analytics_svc = PriceAnalyticsService(self.db)
        return self._price_analytics_svc
