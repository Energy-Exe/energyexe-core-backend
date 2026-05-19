"""Map page performance-scores aggregation (client-ui #44b).

Returns per-windfarm 5-bucket performance scores for the map page, comparing
each windfarm's wind-normalisation index and capture ratio against the
bidzone peer aggregates already cached by `PeerAggregateService`.

Three views the FE colours by:
- Generation: `norm_index_p50` from `performance_summaries`
- Commercial: `capture_ratio` from `generation_concentration_summaries`
- Financial: served from a separate endpoint (`/map/financial-metrics`)

The bucket boundaries follow the spec's 5-stop ramp:
- bucket 1 (Underperforming):    value < p10
- bucket 2 (Below benchmark):    p10  <= value < midpoint(p10, p50)
- bucket 3 (On benchmark):       midpoint(p10, p50) <= value < midpoint(p50, p90)
- bucket 4 (Above benchmark):    midpoint(p50, p90) <= value < p90
- bucket 5 (Outperforming):      value >= p90

A windfarm is treated as "has data" only when both the metric row exists AND
the peer aggregate has at least 3 windfarms (smaller peer groups produce
unreliable percentile thresholds).
"""

from datetime import date
from typing import Dict, List, Optional, Tuple

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.bidzone import Bidzone
from app.models.country import Country
from app.models.generation_concentration_summary import GenerationConcentrationSummary
from app.models.peer_group_aggregate import PeerGroupAggregate
from app.models.performance_summary import PerformanceSummary
from app.models.windfarm import Windfarm
from app.schemas.map import (
    MapCoverage,
    MapFinancialMetric,
    MapFinancialMetricsResponse,
    MapPerformanceScore,
    MapPerformanceScoresResponse,
    MapStatePayload,
)

logger = structlog.get_logger(__name__)


MIN_PEER_COUNT = 3
ASYMMETRIC_THRESHOLD = 0.15

# Metric keys in `peer_group_aggregates` for each view (must match
# `peer_aggregate_service.METRIC_SOURCES`).
GENERATION_METRIC_KEY = "wind_norm_index_p50"
COMMERCIAL_METRIC_KEY = "concentration_capture_ratio"


class MapPerformanceService:
    """Compose map performance scores from existing summary + peer tables."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_scores(
        self,
        windfarm_ids: Optional[List[int]],
        year: int,
        month: Optional[int] = None,
    ) -> MapPerformanceScoresResponse:
        """Return bucketed scores + coverage indicators for the map page.

        Args:
            windfarm_ids: When None, defaults to all operational windfarms.
            year: Calendar year the FE is viewing.
            month: Optional month (None for yearly aggregation).
        """
        period_type = "month" if month else "year"

        windfarms = await self._load_windfarms(windfarm_ids)
        if not windfarms:
            return MapPerformanceScoresResponse(
                period_type=period_type,
                period_year=year,
                period_month=month,
                scores=[],
                coverage=_empty_coverage(),
            )

        wf_ids = [wf.id for wf in windfarms]
        norm_rows = await self._load_norm_indexes(wf_ids, year, month)
        capture_rows = await self._load_capture_ratios(wf_ids, year, month)

        bidzone_ids = sorted({wf.bidzone_id for wf in windfarms if wf.bidzone_id})
        peer_thresholds = await self._load_peer_thresholds(bidzone_ids, year, month)

        scores: List[MapPerformanceScore] = []
        for wf in windfarms:
            norm_value = norm_rows.get(wf.id)
            capture_value = capture_rows.get(wf.id)

            gen_bucket, has_gen = _bucket_for(
                norm_value,
                peer_thresholds.get((wf.bidzone_id, GENERATION_METRIC_KEY)),
            )
            com_bucket, has_com = _bucket_for(
                capture_value,
                peer_thresholds.get((wf.bidzone_id, COMMERCIAL_METRIC_KEY)),
            )

            scores.append(
                MapPerformanceScore(
                    windfarm_id=wf.id,
                    bidzone_id=wf.bidzone_id,
                    bidzone_code=wf.bidzone.code if wf.bidzone else None,
                    country_code=wf.country.code if wf.country else None,
                    commercial_value=capture_value,
                    commercial_bucket=com_bucket,
                    has_commercial_data=has_com,
                    generation_value=norm_value,
                    generation_bucket=gen_bucket,
                    has_generation_data=has_gen,
                    period_type=period_type,
                    period_year=year,
                    period_month=month,
                )
            )

        coverage = _compute_coverage(scores, windfarms)

        return MapPerformanceScoresResponse(
            period_type=period_type,
            period_year=year,
            period_month=month,
            scores=scores,
            coverage=coverage,
        )

    async def get_financial_metrics(
        self,
        windfarm_ids: Optional[List[int]],
        year: int,
        display_currency: str = "EUR",
    ) -> MapFinancialMetricsResponse:
        """Batch financial ratios (rev/MWh, opex/MWh, EBITDA margin) per WF.

        Delegates to `FinancialDataService.calculate_financial_ratios` per WF
        then picks the row whose period overlaps the requested year. Wind
        farms without reported data return `has_data=False` — the FE renders
        them as hollow dashed markers.
        """
        from app.services.financial_data_service import FinancialDataService

        windfarms = await self._load_windfarms(windfarm_ids)
        if not windfarms:
            return MapFinancialMetricsResponse(
                period_type="year",
                period_year=year,
                metrics=[],
                total_count=0,
                with_data_count=0,
            )

        financial_svc = FinancialDataService(self.db)
        metrics: List[MapFinancialMetric] = []
        with_data = 0

        for wf in windfarms:
            try:
                rows = await financial_svc.calculate_financial_ratios(
                    wf.id, display_currency=display_currency
                )
            except Exception as exc:
                logger.warning(
                    "map_financial_ratios_failed",
                    windfarm_id=wf.id,
                    error=str(exc),
                )
                rows = []

            chosen = _pick_year_row(rows, year)
            if chosen is None:
                metrics.append(MapFinancialMetric(windfarm_id=wf.id, has_data=False))
                continue

            with_data += 1
            metrics.append(
                MapFinancialMetric(
                    windfarm_id=wf.id,
                    has_data=True,
                    ebitda_margin=_as_float(getattr(chosen, "ebitda_margin", None)),
                    revenue_per_mwh=_as_float(getattr(chosen, "revenue_per_mwh", None)),
                    opex_per_mwh=_as_float(getattr(chosen, "opex_per_mwh", None)),
                    period_start=getattr(chosen, "period_start", None),
                    period_end=getattr(chosen, "period_end", None),
                    currency=getattr(chosen, "display_currency", None)
                    or getattr(chosen, "currency", None),
                )
            )

        return MapFinancialMetricsResponse(
            period_type="year",
            period_year=year,
            metrics=metrics,
            total_count=len(windfarms),
            with_data_count=with_data,
        )

    def build_interpretation_prompt(
        self,
        state: MapStatePayload,
        scores: Optional[List[MapPerformanceScore]] = None,
    ) -> str:
        """Compose a deterministic prompt for the AI interpretation panel.

        Embeds the visible windfarm count, score distribution, top/bottom 5
        outliers, and filter snapshot so the brain agent has grounded data
        rather than open-ended access.
        """
        view = state.view
        scope_lines = [
            f"You are interpreting the EnergyExe wind-farm map for a portfolio analyst.",
            f"View: {view}",
            f"Period: {state.period_type} {state.period_year}"
            + (f"-{state.period_month:02d}" if state.period_month else ""),
            f"Wind farms in view: {len(state.windfarm_ids)}",
        ]
        if state.filters:
            filt = state.filters
            applied = []
            if filt.countries:
                applied.append(f"countries={','.join(filt.countries)}")
            if filt.types:
                applied.append(f"types={','.join(filt.types)}")
            if filt.zones:
                applied.append(f"zones={','.join(filt.zones)}")
            if filt.statuses:
                applied.append(f"statuses={','.join(filt.statuses)}")
            if filt.capacity_min is not None or filt.capacity_max is not None:
                lo = _fmt_capacity(filt.capacity_min, default="0")
                hi = _fmt_capacity(filt.capacity_max, default="∞")
                applied.append(f"capacity={lo}-{hi}MW")
            if applied:
                scope_lines.append("Filters: " + "; ".join(applied))

        if scores:
            value_attr = "generation_value" if view == "generation" else "commercial_value"
            scored = [(s.windfarm_id, getattr(s, value_attr)) for s in scores
                      if getattr(s, value_attr) is not None]
            if scored:
                scored.sort(key=lambda x: x[1])
                bottom = scored[:5]
                top = scored[-5:][::-1]
                scope_lines.append(
                    f"{view.capitalize()} score range: "
                    f"{scored[0][1]:.2f}–{scored[-1][1]:.2f}"
                )
                scope_lines.append(
                    "Lowest 5 wind farms (id, value): "
                    + ", ".join(f"({wid}, {v:.2f})" for wid, v in bottom)
                )
                scope_lines.append(
                    "Highest 5 wind farms (id, value): "
                    + ", ".join(f"({wid}, {v:.2f})" for wid, v in top)
                )

        scope_lines.append(
            "\nProduce 3–5 short paragraphs interpreting the current view. "
            "Call out outliers, peer-group concentrations, and asymmetries. "
            "Cite specific wind-farm ids only when discussing them. "
            "Do not invent numbers beyond what is supplied above — use the "
            "available MCP tools if you need extra context."
        )

        return "\n".join(scope_lines)

    # ─── Internal loaders ─────────────────────────────────────────

    async def _load_windfarms(
        self, windfarm_ids: Optional[List[int]]
    ) -> List[Windfarm]:
        stmt = (
            select(Windfarm)
            .options(selectinload(Windfarm.bidzone), selectinload(Windfarm.country))
        )
        if windfarm_ids is not None:
            stmt = stmt.where(Windfarm.id.in_(windfarm_ids))
        else:
            stmt = stmt.where(Windfarm.status == "operational")
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def _load_norm_indexes(
        self, wf_ids: List[int], year: int, month: Optional[int]
    ) -> Dict[int, float]:
        period_type = "month" if month else "year"
        stmt = select(
            PerformanceSummary.windfarm_id,
            PerformanceSummary.norm_index_p50,
        ).where(
            PerformanceSummary.windfarm_id.in_(wf_ids),
            PerformanceSummary.period_type == period_type,
            PerformanceSummary.year == year,
            PerformanceSummary.norm_index_p50.isnot(None),
        )
        if month is None:
            stmt = stmt.where(PerformanceSummary.month.is_(None))
        else:
            stmt = stmt.where(PerformanceSummary.month == month)
        rows = (await self.db.execute(stmt)).all()
        return {wf_id: float(v) for wf_id, v in rows if v is not None}

    async def _load_capture_ratios(
        self, wf_ids: List[int], year: int, month: Optional[int]
    ) -> Dict[int, float]:
        period_type = "month" if month else "year"
        stmt = select(
            GenerationConcentrationSummary.windfarm_id,
            GenerationConcentrationSummary.capture_ratio,
        ).where(
            GenerationConcentrationSummary.windfarm_id.in_(wf_ids),
            GenerationConcentrationSummary.period_type == period_type,
            GenerationConcentrationSummary.year == year,
            GenerationConcentrationSummary.capture_ratio.isnot(None),
        )
        if month is None:
            stmt = stmt.where(GenerationConcentrationSummary.month.is_(None))
        else:
            stmt = stmt.where(GenerationConcentrationSummary.month == month)
        rows = (await self.db.execute(stmt)).all()
        return {wf_id: float(v) for wf_id, v in rows if v is not None}

    async def _load_peer_thresholds(
        self, bidzone_ids: List[int], year: int, month: Optional[int]
    ) -> Dict[Tuple[Optional[int], str], "_PeerThresholds"]:
        """Bulk-load peer aggregates for each (bidzone, metric) pair.

        Returns a dict keyed by (bidzone_id, metric_key) → thresholds. Returns
        empty when bidzone_ids is empty.
        """
        if not bidzone_ids:
            return {}

        period_type = "month" if month else "year"
        stmt = select(PeerGroupAggregate).where(
            PeerGroupAggregate.group_type == "bidzone",
            PeerGroupAggregate.group_id.in_(bidzone_ids),
            PeerGroupAggregate.metric_key.in_(
                [GENERATION_METRIC_KEY, COMMERCIAL_METRIC_KEY]
            ),
            PeerGroupAggregate.period_type == period_type,
            PeerGroupAggregate.year == year,
        )
        if month is None:
            stmt = stmt.where(PeerGroupAggregate.month.is_(None))
        else:
            stmt = stmt.where(PeerGroupAggregate.month == month)

        result = await self.db.execute(stmt)
        out: Dict[Tuple[Optional[int], str], _PeerThresholds] = {}
        for row in result.scalars().all():
            out[(row.group_id, row.metric_key)] = _PeerThresholds(
                p10=_as_float(row.p10_value),
                p50=_as_float(row.p50_value),
                p90=_as_float(row.p90_value),
                count=row.windfarm_count,
            )
        return out


# ─── Pure helpers (no DB access) ──────────────────────────────────


class _PeerThresholds:
    __slots__ = ("p10", "p50", "p90", "count")

    def __init__(self, p10, p50, p90, count):
        self.p10 = p10
        self.p50 = p50
        self.p90 = p90
        self.count = count


def _bucket_for(
    value: Optional[float], thresholds: Optional[_PeerThresholds]
) -> Tuple[Optional[int], bool]:
    """Return (bucket 1..5, has_data) for a value against peer thresholds."""
    if value is None or thresholds is None:
        return None, False
    if thresholds.count < MIN_PEER_COUNT:
        return None, False
    if thresholds.p10 is None or thresholds.p50 is None or thresholds.p90 is None:
        return None, False

    p10, p50, p90 = thresholds.p10, thresholds.p50, thresholds.p90
    mid_low = (p10 + p50) / 2
    mid_high = (p50 + p90) / 2

    if value < p10:
        return 1, True
    if value < mid_low:
        return 2, True
    if value < mid_high:
        return 3, True
    if value < p90:
        return 4, True
    return 5, True


def _compute_coverage(
    scores: List[MapPerformanceScore],
    windfarms: List[Windfarm],
) -> MapCoverage:
    total = len(scores)
    commercial = sum(1 for s in scores if s.has_commercial_data)
    generation = sum(1 for s in scores if s.has_generation_data)

    # NO + UK split — country code is ISO 3166-1 alpha-3 (NOR, GBR).
    no_total = 0
    no_with = 0
    uk_total = 0
    uk_with = 0
    for s in scores:
        if s.country_code == "NOR":
            no_total += 1
            if s.has_generation_data:
                no_with += 1
        elif s.country_code == "GBR":
            uk_total += 1
            if s.has_generation_data:
                uk_with += 1

    no_pct = (no_with / no_total) if no_total else 0.0
    uk_pct = (uk_with / uk_total) if uk_total else 0.0
    asymmetric = bool(no_total and uk_total and abs(no_pct - uk_pct) >= ASYMMETRIC_THRESHOLD)

    return MapCoverage(
        total_count=total,
        commercial_count=commercial,
        generation_count=generation,
        no_count=no_total,
        no_with_generation_data=no_with,
        no_coverage_pct=round(no_pct, 4),
        uk_count=uk_total,
        uk_with_generation_data=uk_with,
        uk_coverage_pct=round(uk_pct, 4),
        asymmetric=asymmetric,
    )


def _empty_coverage() -> MapCoverage:
    return MapCoverage(
        total_count=0,
        commercial_count=0,
        generation_count=0,
        no_count=0,
        no_with_generation_data=0,
        no_coverage_pct=0.0,
        uk_count=0,
        uk_with_generation_data=0,
        uk_coverage_pct=0.0,
        asymmetric=False,
    )


def _fmt_capacity(value: Optional[float], default: str) -> str:
    if value is None:
        return default
    if float(value).is_integer():
        return str(int(value))
    return f"{value:g}"


def _as_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_year_row(rows, year: int):
    """Pick the financial-ratios row whose period covers (or is closest to) `year`."""
    if not rows:
        return None
    # Prefer the row whose period covers the target year
    for r in rows:
        ps = getattr(r, "period_start", None)
        pe = getattr(r, "period_end", None)
        if isinstance(ps, date) and isinstance(pe, date):
            if ps.year <= year <= pe.year:
                return r
    # Fall back to the most recent period
    def _end(r):
        pe = getattr(r, "period_end", None)
        return pe if isinstance(pe, date) else date(1900, 1, 1)
    return sorted(rows, key=_end, reverse=True)[0]
