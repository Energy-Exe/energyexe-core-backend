"""Shared window/metric helpers for report data builders.

Extracted from the digest builders (EPR-87) so the Opportunity report can
compute the same calendar-aware comparison windows, generation totals and
per-window metrics without importing digest-specific code. Semantics are
unchanged — the digest module re-exports these under its original names.
"""

import calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

import structlog
from sqlalchemy import func, select

from app.models.financial_data import FinancialData
from app.models.generation_data import GenerationData
from app.models.performance_summary import PerformanceSummary
from app.models.windfarm_financial_entity import WindfarmFinancialEntity
from app.services.reports.context import ReportContext

logger = structlog.get_logger()

# Relative change below this renders as "flat" rather than an arrow.
FLAT_TOLERANCE = 0.005


# ── period arithmetic ───────────────────────────────────────────────────


def month_end(d: date) -> date:
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


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


# ── shared window metrics ───────────────────────────────────────────────


async def generation_totals(ctx: ReportContext, start: date, end: date) -> dict:
    """SUM of generation + curtailment over the window, via the windfarm stamp."""
    window_start, window_end = utc_bounds(start, end)
    result = await ctx.db.execute(
        select(
            func.sum(func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh)),
            func.sum(GenerationData.curtailed_mwh),
            func.count(GenerationData.hour),
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


async def window_metrics(ctx: ReportContext, start: date, end: date) -> dict:
    gen = await generation_totals(ctx, start, end)
    stats = await summary_stats(ctx, start, end)
    capture = await capture_rate_pct(ctx, start, end)
    fin = await financials_for(ctx, end)

    capacity_mw = ctx.windfarm.nameplate_capacity_mw if ctx.windfarm is not None else None
    window_hours = ((end - start).days + 1) * 24
    capacity_factor = None
    if gen["generation_mwh"] is not None and capacity_mw and window_hours:
        capacity_factor = gen["generation_mwh"] / (capacity_mw * window_hours) * 100

    ebitda_margin = None
    opex_per_mwh = None
    fin_label = None
    if fin is not None:
        fin_label = f"FY {fin.period_end.year}" + (f", {fin.currency}" if fin.currency else "")
        if fin.ebitda is not None and fin.total_revenue:
            ebitda_margin = float(fin.ebitda) / float(fin.total_revenue) * 100
        if fin.total_operating_expenses is not None and fin.reported_generation_gwh:
            opex_per_mwh = abs(float(fin.total_operating_expenses)) / (
                float(fin.reported_generation_gwh) * 1000
            )

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
