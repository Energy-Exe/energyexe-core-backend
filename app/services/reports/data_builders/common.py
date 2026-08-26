"""Shared window/metric helpers for report data builders.

Extracted from the digest builders (EPR-87) so the Opportunity report can
compute the same calendar-aware comparison windows, generation totals and
per-window metrics without importing digest-specific code. Semantics are
unchanged — the digest module re-exports these under its original names.
"""

from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

import structlog
from sqlalchemy import func, select

from app.models.bidzone import Bidzone
from app.models.financial_data import FinancialData
from app.models.generation_data import GenerationData
from app.models.performance_summary import PerformanceSummary
from app.models.windfarm_financial_entity import WindfarmFinancialEntity
from app.services.financial_opex_metrics import OpexMetrics, opex_metrics_for_windfarms
from app.services.generation_coverage import (  # noqa: F401  (re-exported; digest imports it)
    generation_data_through,
    month_end,
)
from app.services.reports.context import ReportContext

logger = structlog.get_logger()

# Relative change below this renders as "flat" rather than an arrow.
FLAT_TOLERANCE = 0.005

# A window clipped by less than this is not worth a coverage caveat — every
# window ending "today" is short a day or two of import lag.
COVERAGE_NOTE_MIN_DAYS = 7


# ── period arithmetic ───────────────────────────────────────────────────


def shift_months(d: date, months: int) -> date:
    """Shift a first-of-month date by whole months."""
    total = d.year * 12 + (d.month - 1) + months
    return date(total // 12, total % 12 + 1, 1)


def is_calendar_aligned(start: date, end: date) -> bool:
    return start.day == 1 and end == month_end(end)


def months_spanned(start: date, end: date) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month) + 1


def previous_window(start: date, end: date) -> tuple[date, date]:
    """The immediately preceding period of the same length."""
    if is_calendar_aligned(start, end):
        months = months_spanned(start, end)
        prev_start = shift_months(start, -months)
        return prev_start, start - timedelta(days=1)
    length = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    return prev_end - timedelta(days=length - 1), prev_end


def yoy_window(start: date, end: date) -> tuple[date, date]:
    """The same period one year earlier."""
    if is_calendar_aligned(start, end):
        yoy_start = shift_months(start, -12)
        yoy_end_month = shift_months(end.replace(day=1), -12)
        return yoy_start, month_end(yoy_end_month)

    def _minus_year(d: date) -> date:
        try:
            return d.replace(year=d.year - 1)
        except ValueError:  # 29 Feb
            return d.replace(year=d.year - 1, day=28)

    return _minus_year(start), _minus_year(end)


def period_label(start: date, end: date) -> str:
    """Human label for a window: 'Jul 2026', 'Q3 2026', '2026', or the dates."""
    if is_calendar_aligned(start, end):
        months = months_spanned(start, end)
        if months == 1:
            return f"{start:%b %Y}"
        if months == 3 and start.month in (1, 4, 7, 10):
            return f"Q{(start.month - 1) // 3 + 1} {start.year}"
        if months == 12 and start.month == 1:
            return str(start.year)
    return f"{start:%d %b %Y} – {end:%d %b %Y}"


def utc_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """Half-open UTC window [start 00:00, end+1d 00:00).

    Bounds MUST be tz-aware: asyncpg encodes naive datetimes for timestamptz
    in the process's LOCAL timezone, silently shifting the window on any
    machine not running in UTC.
    """
    return (
        datetime.combine(start, time.min, tzinfo=timezone.utc),
        datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc),
    )


# ── deltas / formatting ─────────────────────────────────────────────────


def direction(current: Optional[float], comparison: Optional[float]) -> Optional[str]:
    if current is None or comparison is None:
        return None
    if comparison == 0:
        return "flat" if current == 0 else ("up" if current > 0 else "down")
    rel = (current - comparison) / abs(comparison)
    if abs(rel) < FLAT_TOLERANCE:
        return "flat"
    return "up" if rel > 0 else "down"


def delta_pct(current: Optional[float], comparison: Optional[float]) -> Optional[float]:
    if current is None or comparison is None or comparison == 0:
        return None
    return round((current - comparison) / abs(comparison) * 100, 1)


def fmt(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{decimals}f}"


# ── coverage ────────────────────────────────────────────────────────────


def capacity_factor_pct(
    generation_mwh: Optional[float], capacity_mw: Optional[float], covered_hours: Optional[int]
) -> Optional[float]:
    """Capacity factor over the hours that actually carry a reading (EPR-111).

    Dividing by the *requested* window instead understates the factor in
    proportion to the empty tail — a farm whose data stops five months into a
    twelve-month window reads 15% where it ran at 40%.
    """
    if generation_mwh is None or not capacity_mw or not covered_hours:
        return None
    return generation_mwh / (capacity_mw * covered_hours) * 100


def coverage_note(
    requested_end: date, data_through: Optional[date], eff_start: date, eff_end: date
) -> Optional[str]:
    """Caption for a window clipped short by missing data, or None if the gap
    is small enough to be ordinary import lag."""
    if data_through is None or (requested_end - data_through).days < COVERAGE_NOTE_MIN_DAYS:
        return None
    return (
        f"Generation data available through {data_through:%d %b %Y} — metrics and deltas "
        f"cover {eff_start:%d %b} – {eff_end:%d %b %Y}, not the full reporting period."
    )


# ── shared window metrics ───────────────────────────────────────────────


async def generation_totals(ctx: ReportContext, start: date, end: date) -> dict:
    """SUM of generation + curtailment over the window, via the windfarm stamp."""
    window_start, window_end = utc_bounds(start, end)
    result = await ctx.db.execute(
        select(
            func.sum(func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh)),
            func.sum(GenerationData.curtailed_mwh),
            # Hours that carry a reading — COUNT over the coalesced value, not
            # over `hour`, so a row present with both columns NULL does not
            # inflate coverage (it is the capacity-factor denominator).
            func.count(func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh)),
        ).where(
            GenerationData.windfarm_id == ctx.windfarm_id,
            GenerationData.hour >= window_start,
            GenerationData.hour < window_end,
        )
    )
    gen_mwh, curtailed_mwh, hours = result.one()
    return {
        "generation_mwh": float(gen_mwh) if gen_mwh is not None else None,
        "curtailed_mwh": float(curtailed_mwh) if curtailed_mwh is not None else None,
        "hours_with_data": int(hours or 0),
    }


async def effective_window(
    ctx: ReportContext, start: date, end: date
) -> tuple[date, date, Optional[date]]:
    """The requested window clipped to the last day that has generation data.

    A report window may run past the data — Norwegian (NVE) farms in particular
    lag by months — and comparing a part-covered window against a fully covered
    one reads as a collapse that never happened (EPR-111). Returns
    ``(start, effective_end, data_through)`` where ``data_through`` is set only
    when the window was actually clipped.

    The probe is the shared ``generation_data_through`` (EPR-126) — the same
    definition the nightly detection uses to clip its own window, so the
    report and the persisted findings agree on where the data ends. Monthly
    sources (EIA / ENERGISTYRELSEN) clip to their last month's end.
    """
    window_start, window_end = utc_bounds(start, end)
    coverage = await generation_data_through(ctx.db, ctx.windfarm_id, window_start, window_end)
    if coverage is None:
        # No data at all in the window — leave it alone; the caller's
        # empty-comparison guard renders the metrics as n/a.
        return start, end, None
    if coverage.last_day >= end:
        return start, end, None
    return start, coverage.last_day, coverage.last_day


async def bidzone_names(ctx: ReportContext) -> dict[str, str]:
    """Bidzone code → display name ('10YNO-2--------T' → 'NO2'), for EPR-110.

    83 rows; one query per report run beats a per-finding lookup.
    """
    result = await ctx.db.execute(select(Bidzone.code, Bidzone.name))
    return {code: name for code, name in result.all() if code and name}


async def monthly_summaries(ctx: ReportContext, start: date, end: date) -> list[PerformanceSummary]:
    """Monthly performance_summaries rows whose month falls inside the window."""
    result = await ctx.db.execute(
        select(PerformanceSummary).where(
            PerformanceSummary.windfarm_id == ctx.windfarm_id,
            PerformanceSummary.period_type == "month",
            PerformanceSummary.year >= start.year,
            PerformanceSummary.year <= end.year,
        )
    )
    return [
        row
        for row in result.scalars().all()
        if start.replace(day=1) <= date(row.year, row.month or 1, 1) <= end
    ]


async def summary_stats(ctx: ReportContext, start: date, end: date) -> dict:
    """Aggregates over the window's monthly performance_summaries rows."""
    rows = await monthly_summaries(ctx, start, end)
    p50_values = [float(r.norm_ratio_p50) for r in rows if r.norm_ratio_p50 is not None]
    expected_values = [float(r.expected_mwh) for r in rows if r.expected_mwh is not None]
    lost_eur_values = [float(r.lost_eur) for r in rows if r.lost_eur is not None]
    return {
        "p50_attainment_pct": (sum(p50_values) / len(p50_values)) * 100 if p50_values else None,
        "expected_mwh": sum(expected_values) if expected_values else None,
        "lost_eur": sum(lost_eur_values) if lost_eur_values else None,
        "months_covered": len(rows),
    }


async def capture_rate_pct(ctx: ReportContext, start: date, end: date) -> Optional[float]:
    try:
        from app.services.price_analytics_service import PriceAnalyticsService

        capture = await PriceAnalyticsService(ctx.db).calculate_capture_rate(
            windfarm_id=ctx.windfarm_id,
            start_date=datetime.combine(start, time.min),
            end_date=datetime.combine(end, time.max),
            aggregation="month",
        )
        rate = capture.get("overall", {}).get("capture_rate")
        # Service returns a ratio (achieved / market average) — display as %.
        return float(rate) * 100 if rate is not None else None
    except Exception as exc:  # price data is optional — the row degrades to n/a
        logger.warning(
            "report_capture_rate_failed",
            report_id=ctx.report_id,
            windfarm_id=ctx.windfarm_id,
            error=str(exc),
        )
        return None


async def financials_for(ctx: ReportContext, as_of: date) -> Optional[FinancialData]:
    """The most recent reported fiscal period starting on or before ``as_of``.

    Financials are annual filings — for monthly/quarterly windows the same FY
    row can back several windows, which honestly renders as a flat delta.
    """
    result = await ctx.db.execute(
        select(FinancialData)
        .join(
            WindfarmFinancialEntity,
            WindfarmFinancialEntity.financial_entity_id == FinancialData.financial_entity_id,
        )
        .where(
            WindfarmFinancialEntity.windfarm_id == ctx.windfarm_id,
            FinancialData.period_start <= as_of,
        )
        .order_by(FinancialData.period_end.desc())
        .limit(1)
    )
    return result.scalars().first()


async def opex_metrics_for(ctx: ReportContext, as_of: date) -> Optional[OpexMetrics]:
    """The latest usable filing's OPEX/MWh, in the farm's own filing currency.

    Single-farm views follow the Financial tab convention (filing currency,
    metered ``generation_data`` denominator over the filing's own period, ramp-up
    excluded) via ``app.services.financial_opex_metrics`` — the same definition
    the FIN-02/03 detectors use, so a digest never shows a different "Opex / MWh"
    than the finding next to it. Synthetic filings are kept, as on the Financial
    tab. ``None`` when the farm has no usable filing or the lookup fails (logged).
    """
    if ctx.windfarm_id is None:
        return None
    try:
        metrics = await opex_metrics_for_windfarms(
            ctx.db,
            windfarm_ids=[ctx.windfarm_id],
            as_of=as_of,
            display_currency=None,
            max_rows=1,
            include_synthetic=True,
        )
    except Exception as exc:  # financials are optional — the row degrades to n/a
        logger.warning(
            "report_opex_metrics_failed",
            report_id=ctx.report_id,
            windfarm_id=ctx.windfarm_id,
            error=str(exc),
        )
        return None
    return metrics.get(ctx.windfarm_id)


async def window_metrics(ctx: ReportContext, start: date, end: date) -> dict:
    gen = await generation_totals(ctx, start, end)
    stats = await summary_stats(ctx, start, end)
    capture = await capture_rate_pct(ctx, start, end)
    fin = await financials_for(ctx, end)
    opex = await opex_metrics_for(ctx, end)

    capacity_mw = ctx.windfarm.nameplate_capacity_mw if ctx.windfarm is not None else None
    capacity_factor = capacity_factor_pct(
        gen["generation_mwh"], capacity_mw, gen["hours_with_data"]
    )

    ebitda_margin = None
    opex_per_mwh = None
    opex_unit = None
    opex_fiscal_year = None
    fin_label = None
    if fin is not None:
        fin_label = f"FY {fin.period_end.year}" + (f", {fin.currency}" if fin.currency else "")
        if (
            fin.ebitda is not None
            and fin.total_revenue is not None
            and float(fin.total_revenue) > 0
        ):
            ebitda_margin = float(fin.ebitda) / float(fin.total_revenue) * 100
    if opex is not None:
        # Metered-denominator ratio wins; the filing row above is only a fallback
        # for the EBITDA margin when no usable OPEX filing exists.
        opex_per_mwh = opex.opex_per_mwh
        opex_unit = opex.currency  # the row label already says "/ MWh"
        opex_fiscal_year = opex.period_end.year
        fin_label = f"FY {opex.period_end.year}, {opex.currency}"
        if opex.ebitda_margin_pct is not None:
            ebitda_margin = opex.ebitda_margin_pct

    return {
        "generation_gwh": (
            gen["generation_mwh"] / 1000 if gen["generation_mwh"] is not None else None
        ),
        "curtailed_gwh": (
            gen["curtailed_mwh"] / 1000 if gen["curtailed_mwh"] is not None else None
        ),
        "hours_with_data": gen["hours_with_data"],
        "capacity_factor_pct": capacity_factor,
        "p50_attainment_pct": stats["p50_attainment_pct"],
        "expected_gwh": (
            stats["expected_mwh"] / 1000 if stats["expected_mwh"] is not None else None
        ),
        "lost_eur": stats["lost_eur"],
        "months_covered": stats["months_covered"],
        "capture_rate_pct": capture,
        "ebitda_margin_pct": ebitda_margin,
        "opex_per_mwh": opex_per_mwh,
        "opex_unit": opex_unit,
        "opex_fiscal_year": opex_fiscal_year,
        "financials_label": fin_label,
    }


# ── generation chart series ─────────────────────────────────────────────


async def generation_series(
    ctx: ReportContext, start: date, end: date, granularity: str
) -> list[dict]:
    window_start, window_end = utc_bounds(start, end)
    bucket = func.date_trunc(granularity, GenerationData.hour)
    result = await ctx.db.execute(
        select(
            bucket.label("bucket"),
            func.sum(func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh)),
        )
        .where(
            GenerationData.windfarm_id == ctx.windfarm_id,
            GenerationData.hour >= window_start,
            GenerationData.hour < window_end,
        )
        .group_by("bucket")
        .order_by("bucket")
    )
    label_fmt = "%d %b" if granularity == "day" else "%b %Y"
    return [
        {"label": b.strftime(label_fmt), "gwh": round(float(total or 0) / 1000, 3)}
        for b, total in result.all()
    ]


async def build_generation_chart_data(ctx: ReportContext) -> dict:
    """chart_embed payload: the client re-renders the platform's own generation
    chart from these params; the compact series here is for the PDF renderer."""
    start, end = ctx.period_start, ctx.period_end
    prev_start, prev_end = previous_window(start, end)
    # Daily bars for a one-month window, monthly bars for anything longer.
    granularity = "day" if months_spanned(start, end) <= 1 else "month"

    current_series = await generation_series(ctx, start, end, granularity)
    previous_series = await generation_series(ctx, prev_start, prev_end, granularity)

    return {
        "chart_key": "windfarm_generation",
        "windfarm_id": ctx.windfarm_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "aggregation": "daily" if granularity == "day" else "monthly",
        "series": {
            "unit": "GWh",
            "current": {"label": period_label(start, end), "points": current_series},
            "previous": {
                "label": period_label(prev_start, prev_end),
                "points": previous_series,
            },
        },
    }
