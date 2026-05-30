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
    "ppa_info", "monthly_performance", "capture_rate", "cannibalisation_index",
    "seasonal_capture", "curtailment_pct", "degradation_result",
    "norm_index_series", "turbine_start_dates".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
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

    async def load_norm_index_series(self) -> Optional[List[dict]]:
        """Monthly empirical-P50 normalised-index series (issue #101, OPS-06).

        Reads ``performance_summaries.norm_index_p50`` (the Module 4 wind-normalised
        index, where 100 means "performing exactly at the empirical P50 reference")
        for this windfarm, ordered chronologically. Each row is::

            {"month": "YYYY-MM", "norm_index_p50": float}

        Rows whose ``norm_index_p50`` is ``NULL`` or ``0`` are **dropped**: a zero /
        missing index is a data gap (no usable normalisation that month), not
        underperformance, per the OPS-06 spec. The remaining rows preserve
        chronological order so OPS-06 can count consecutive months.

        Returns ``None`` when no usable rows exist (so OPS-06 simply does not fire).
        Cache key: ``"norm_index_series"`` (inject via ``prefetched`` for DB-free
        tests). The injected value may be either the list-of-dicts shape above OR a
        bare list of floats — :func:`ops06_persistent_underperformance.detect`
        normalises both.
        """
        if "norm_index_series" in self._cache:
            return self._cache["norm_index_series"]

        self._cache["norm_index_series"] = await self._compute_norm_index_series()
        return self._cache["norm_index_series"]

    async def _compute_norm_index_series(self) -> Optional[List[dict]]:
        try:
            from app.models.performance_summary import PerformanceSummary

            result = await self.db.execute(
                select(PerformanceSummary)
                .where(
                    PerformanceSummary.windfarm_id == self.windfarm_id,
                    PerformanceSummary.period_type == "month",
                    PerformanceSummary.norm_index_p50.isnot(None),
                )
                .order_by(PerformanceSummary.year, PerformanceSummary.month)
            )
            summaries = result.scalars().all()
        except Exception:
            return None

        rows = [
            {
                "month": f"{s.year}-{s.month:02d}",
                "norm_index_p50": float(s.norm_index_p50),
            }
            for s in summaries
            # 0 / NULL = data gap, not underperformance (OPS-06 spec).
            if s.norm_index_p50 is not None and float(s.norm_index_p50) != 0.0
        ]
        if not rows:
            return None
        return rows

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

    async def load_curtailment_pct(self) -> Optional[float]:
        """Grid-curtailment percentage over the detection window (issue #94).

        Used by MKT-01 suppression: a capture-rate shortfall driven by grid
        curtailment (>15%) is grid-driven, not a contracting problem.

        Defined as::

            curtailment_pct = curtailed / (curtailed + generation) * 100

        where ``curtailed`` = ``SUM(generation_data.curtailed_mwh)`` and
        ``generation`` = ``SUM(generation_data.generation_mwh)`` over the window.
        Returns ``None`` when no clean curtailment data is reachable (no rows, or
        ``curtailed + generation == 0``); a ``None`` simply means suppression
        won't trigger. Cache key: ``"curtailment_pct"`` (inject via
        ``prefetched`` for DB-free tests).
        """
        if "curtailment_pct" in self._cache:
            return self._cache["curtailment_pct"]

        self._cache["curtailment_pct"] = await self._compute_curtailment_pct()
        return self._cache["curtailment_pct"]

    async def _compute_curtailment_pct(self) -> Optional[float]:
        query = text(
            """
            SELECT
                COALESCE(SUM(curtailed_mwh), 0) AS curtailed,
                COALESCE(SUM(generation_mwh), 0) AS generation
            FROM generation_data
            WHERE windfarm_id = :wf_id
              AND hour >= :start AND hour < :end
        """
        )
        try:
            result = await self.db.execute(
                query,
                {"wf_id": self.windfarm_id, "start": self.period_start, "end": self.period_end},
            )
            row = result.fetchone()
        except Exception:
            return None

        if row is None:
            return None

        curtailed = float(row.curtailed or 0)
        generation = float(row.generation or 0)
        denominator = curtailed + generation
        if denominator <= 0:
            return None
        return curtailed / denominator * 100

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

    async def load_seasonal_capture(self) -> Optional[dict]:
        """High-wind vs low-wind season capacity factors.

        Mirrors legacy ``_calc_seasonal_capture``. Returns ``None`` unless both a
        high-wind ('high') and low-wind ('low') seasonal average exist; otherwise
        a dict with ``high_wind_cf``, ``low_wind_cf``, ``years_with_inversion``.

        Cache key: ``"seasonal_capture"`` (inject via ``prefetched`` for DB-free
        tests). Copied verbatim from the legacy ``_calc_seasonal_capture`` query
        text / result shape; OPS-02 is the sole consumer.
        """
        if "seasonal_capture" in self._cache:
            return self._cache["seasonal_capture"]

        self._cache["seasonal_capture"] = await self._compute_seasonal_capture()
        return self._cache["seasonal_capture"]

    async def _compute_seasonal_capture(self) -> Optional[dict]:
        windfarm_id = self.windfarm_id
        start = self.period_start
        end = self.period_end

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

    async def load_degradation_result(self) -> Optional[dict]:
        """Latest degradation OLS result for this windfarm (issue #99, OPS-04).

        Reads the most-recent ``degradation_results`` row for the windfarm — the
        Module 5 OLS regression of normalized output against time. Used by OPS-04
        (turbine degradation): a significant negative ``slope_pct_per_year`` is the
        degradation signal.

        "Latest" = highest ``pipeline_run_id`` (most recent pipeline run), with
        ``id`` as a stable tie-breaker. The ``q50`` (P50) reference curve is
        preferred over ``q90`` (P10); when only ``q90`` exists it is used.

        Returns a plain dict (DB-free injectable via ``prefetched`` cache key
        ``"degradation_result"``) with the fields OPS-04 needs::

            {
                "slope_pct_per_year": float | None,
                "p_value": float | None,
                "r_squared": float | None,
                "ci_lower_95_pct": float | None,
                "ci_upper_95_pct": float | None,
                "n_constraint_hours_excluded": int | None,
                "baseline_cap_pu": float | None,
                "reference_curve": str,
                "analysis_start": date | None,
                "analysis_end": date | None,
                "data_points": int | None,
            }

        Returns ``None`` when no degradation row exists for the windfarm (so
        OPS-04 simply does not fire). Cache key: ``"degradation_result"``.
        """
        if "degradation_result" in self._cache:
            return self._cache["degradation_result"]

        self._cache["degradation_result"] = await self._compute_degradation_result()
        return self._cache["degradation_result"]

    async def _compute_degradation_result(self) -> Optional[dict]:
        from app.models.degradation_result import DegradationResult

        try:
            result = await self.db.execute(
                select(DegradationResult)
                .where(DegradationResult.windfarm_id == self.windfarm_id)
                .order_by(
                    # Prefer the q50 (P50) reference curve, then the most recent
                    # pipeline run, then a stable id tie-breaker.
                    (DegradationResult.reference_curve == "q50").desc(),
                    DegradationResult.pipeline_run_id.desc().nullslast(),
                    DegradationResult.id.desc(),
                )
            )
            row = result.scalars().first()
        except Exception:
            return None

        if row is None:
            return None

        def _f(v: Any) -> Optional[float]:
            return float(v) if v is not None else None

        return {
            "slope_pct_per_year": _f(row.slope_pct_per_year),
            "p_value": _f(row.p_value),
            "r_squared": _f(row.r_squared),
            "ci_lower_95_pct": _f(row.ci_lower_95_pct),
            "ci_upper_95_pct": _f(row.ci_upper_95_pct),
            "n_constraint_hours_excluded": row.n_constraint_hours_excluded,
            "baseline_cap_pu": _f(row.baseline_cap_pu),
            "reference_curve": row.reference_curve,
            "analysis_start": row.analysis_start,
            "analysis_end": row.analysis_end,
            "data_points": row.data_points,
        }

    async def load_turbine_start_dates(self) -> Optional[List[date]]:
        """Commissioning ``start_date`` of every turbine on this windfarm (OPS-07).

        Reads ``turbine_units.start_date`` for the windfarm's turbines (the
        per-turbine commissioning date). OPS-07 (fleet-age / end-of-life risk)
        compares each turbine's age against the 25-year design life, so it needs
        the raw start dates rather than an aggregate.

        Returns a list of :class:`datetime.date` (turbines with a NULL
        ``start_date`` are dropped — an unknown commissioning date contributes no
        age signal). Returns ``None`` when the windfarm has no turbine rows at all
        (or none with a usable ``start_date``), so OPS-07 simply does not fire.

        None-safe: any access failure (no session, detached relationship in a
        DB-free test) resolves to ``None``. Cache key: ``"turbine_start_dates"``
        (inject via ``prefetched`` for DB-free tests — the injected value may be a
        list of ``date`` objects, or ``None``).
        """
        if "turbine_start_dates" in self._cache:
            return self._cache["turbine_start_dates"]

        self._cache["turbine_start_dates"] = await self._compute_turbine_start_dates()
        return self._cache["turbine_start_dates"]

    async def _compute_turbine_start_dates(self) -> Optional[List[date]]:
        from app.models.turbine_unit import TurbineUnit

        try:
            result = await self.db.execute(
                select(TurbineUnit.start_date).where(
                    TurbineUnit.windfarm_id == self.windfarm_id,
                    TurbineUnit.start_date.isnot(None),
                )
            )
            rows = result.scalars().all()
        except Exception:
            return None

        start_dates = [d for d in rows if d is not None]
        if not start_dates:
            return None
        return start_dates

    # ─── Accessors deferred to later issues ────────────────────────────────
    #
    # The following memoized accessors are part of the DetectionContext surface
    # per the plan but are added by their respective downstream issues (each
    # alongside the detector that needs it). They are intentionally NOT stubbed
    # here to avoid shipping broken bodies:
    #
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
