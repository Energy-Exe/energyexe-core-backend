"""Periodic Digest data builders (EPR-87).

Change-focused: every metric is computed for the report window, the previous
period, and the same period last year, with directional deltas. Comparison
windows are calendar-aware (a full-month window steps back whole months, not a
fixed day count). Missing comparison data degrades honestly — the column is
still emitted with "n/a" values, or dropped entirely when a whole snapshot has
no data (e.g. no detection history before the engine launched).
"""

import calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional

import structlog
from sqlalchemy import Date, cast, func, select

from app.models.financial_data import FinancialData
from app.models.generation_data import GenerationData
from app.models.opportunity import Opportunity, SchemaCode
from app.models.performance_summary import PerformanceSummary
from app.models.windfarm_financial_entity import WindfarmFinancialEntity
from app.services.opportunity_schemas.registry import SCHEMA_STATUS
from app.services.reports.context import ReportContext

logger = structlog.get_logger()

# Findings older than this are treated as no longer reflecting the engine's
# state at a snapshot date (the engine re-runs and supersedes frequently).
_SNAPSHOT_RECENCY_DAYS = 60

# Relative change below this renders as "flat" rather than an arrow.
_FLAT_TOLERANCE = 0.005


# ── period arithmetic ───────────────────────────────────────────────────


def _month_end(d: date) -> date:
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


def _shift_months(d: date, months: int) -> date:
    """Shift a first-of-month date by whole months."""
    total = d.year * 12 + (d.month - 1) + months
    return date(total // 12, total % 12 + 1, 1)


def _is_calendar_aligned(start: date, end: date) -> bool:
    return start.day == 1 and end == _month_end(end)


def _months_spanned(start: date, end: date) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month) + 1


def previous_window(start: date, end: date) -> tuple[date, date]:
    """The immediately preceding period of the same length."""
    if _is_calendar_aligned(start, end):
        months = _months_spanned(start, end)
        prev_start = _shift_months(start, -months)
        return prev_start, start - timedelta(days=1)
    length = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    return prev_end - timedelta(days=length - 1), prev_end


def yoy_window(start: date, end: date) -> tuple[date, date]:
    """The same period one year earlier."""
    if _is_calendar_aligned(start, end):
        yoy_start = _shift_months(start, -12)
        yoy_end_month = _shift_months(end.replace(day=1), -12)
        return yoy_start, _month_end(yoy_end_month)

    def _minus_year(d: date) -> date:
        try:
            return d.replace(year=d.year - 1)
        except ValueError:  # 29 Feb
            return d.replace(year=d.year - 1, day=28)

    return _minus_year(start), _minus_year(end)


def period_label(start: date, end: date) -> str:
    """Human label for a window: 'Jul 2026', 'Q3 2026', '2026', or the dates."""
    if _is_calendar_aligned(start, end):
        months = _months_spanned(start, end)
        if months == 1:
            return f"{start:%b %Y}"
        if months == 3 and start.month in (1, 4, 7, 10):
            return f"Q{(start.month - 1) // 3 + 1} {start.year}"
        if months == 12 and start.month == 1:
            return str(start.year)
    return f"{start:%d %b %Y} – {end:%d %b %Y}"


# ── shared window metrics ───────────────────────────────────────────────


def _utc_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """Half-open UTC window [start 00:00, end+1d 00:00).

    Bounds MUST be tz-aware: asyncpg encodes naive datetimes for timestamptz
    in the process's LOCAL timezone, silently shifting the window on any
    machine not running in UTC.
    """
    return (
        datetime.combine(start, time.min, tzinfo=timezone.utc),
        datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc),
    )


async def _generation_totals(ctx: ReportContext, start: date, end: date) -> dict:
    """SUM of generation + curtailment over the window, via the windfarm stamp."""
    window_start, window_end = _utc_bounds(start, end)
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


async def _summary_stats(ctx: ReportContext, start: date, end: date) -> dict:
    """Monthly performance_summaries rows overlapping the window."""
    result = await ctx.db.execute(
        select(PerformanceSummary).where(
            PerformanceSummary.windfarm_id == ctx.windfarm_id,
            PerformanceSummary.period_type == "month",
            PerformanceSummary.year >= start.year,
            PerformanceSummary.year <= end.year,
        )
    )
    rows = [
        row
        for row in result.scalars().all()
        if start.replace(day=1) <= date(row.year, row.month or 1, 1) <= end
    ]
    p50_values = [float(r.norm_ratio_p50) for r in rows if r.norm_ratio_p50 is not None]
    expected_values = [float(r.expected_mwh) for r in rows if r.expected_mwh is not None]
    return {
        "p50_attainment_pct": (sum(p50_values) / len(p50_values)) * 100 if p50_values else None,
        "expected_mwh": sum(expected_values) if expected_values else None,
        "months_covered": len(rows),
    }


async def _capture_rate_pct(ctx: ReportContext, start: date, end: date) -> Optional[float]:
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
            "digest_capture_rate_failed",
            report_id=ctx.report_id,
            windfarm_id=ctx.windfarm_id,
            error=str(exc),
        )
        return None


async def _financials_for(ctx: ReportContext, as_of: date) -> Optional[FinancialData]:
    """The most recent reported fiscal period starting on or before ``as_of``.

    Financials are annual filings — for monthly/quarterly digests the same FY
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


async def _window_metrics(ctx: ReportContext, start: date, end: date) -> dict:
    gen = await _generation_totals(ctx, start, end)
    stats = await _summary_stats(ctx, start, end)
    capture = await _capture_rate_pct(ctx, start, end)
    fin = await _financials_for(ctx, end)

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
        "capture_rate_pct": capture,
        "ebitda_margin_pct": ebitda_margin,
        "opex_per_mwh": opex_per_mwh,
        "financials_label": fin_label,
    }


# ── scorecard assembly ──────────────────────────────────────────────────


def _direction(current: Optional[float], comparison: Optional[float]) -> Optional[str]:
    if current is None or comparison is None:
        return None
    if comparison == 0:
        return "flat" if current == 0 else ("up" if current > 0 else "down")
    rel = (current - comparison) / abs(comparison)
    if abs(rel) < _FLAT_TOLERANCE:
        return "flat"
    return "up" if rel > 0 else "down"


def _delta_pct(current: Optional[float], comparison: Optional[float]) -> Optional[float]:
    if current is None or comparison is None or comparison == 0:
        return None
    return round((current - comparison) / abs(comparison) * 100, 1)


def _fmt(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{decimals}f}"


def _scorecard_row(
    key: str,
    label: str,
    unit: Optional[str],
    metrics: dict[str, dict],
    metric_key: str,
    decimals: int = 1,
) -> dict:
    values = {col: m.get(metric_key) for col, m in metrics.items()}
    current = values.get("current")
    row: dict[str, Any] = {
        "key": key,
        "label": label,
        "unit": unit,
        "values": {col: _fmt(v, decimals) for col, v in values.items()},
        "raw": values,
        "direction": {},
        "delta_pct": {},
    }
    for col in ("previous", "yoy"):
        if col in values:
            direction = _direction(current, values[col])
            if direction is not None:
                row["direction"][col] = direction
            delta = _delta_pct(current, values[col])
            if delta is not None:
                row["delta_pct"][col] = delta
    return row


async def build_scorecard(ctx: ReportContext) -> dict:
    """The period scorecard: this period vs previous vs same period last year,
    value + directional arrow only (EPR-87: no severity colour-coding)."""
    start, end = ctx.period_start, ctx.period_end
    prev_start, prev_end = previous_window(start, end)
    yoy_start, yoy_end = yoy_window(start, end)

    metrics = {
        "current": await _window_metrics(ctx, start, end),
        "previous": await _window_metrics(ctx, prev_start, prev_end),
    }
    # For an annual digest "previous period" and "same period last year" are
    # the same window — don't emit a duplicate column.
    if (yoy_start, yoy_end) != (prev_start, prev_end):
        metrics["yoy"] = await _window_metrics(ctx, yoy_start, yoy_end)

    # Comparison columns with no generation data at all are dropped, not zeroed.
    for col in ("previous", "yoy"):
        if col in metrics and metrics[col]["hours_with_data"] == 0:
            del metrics[col]

    columns = [{"key": "current", "label": period_label(start, end)}]
    if "previous" in metrics:
        columns.append({"key": "previous", "label": period_label(prev_start, prev_end)})
    if "yoy" in metrics:
        columns.append({"key": "yoy", "label": period_label(yoy_start, yoy_end)})

    rows = [
        _scorecard_row("generation", "Generation", "GWh", metrics, "generation_gwh"),
        _scorecard_row("capacity_factor", "Capacity factor", "%", metrics, "capacity_factor_pct"),
        _scorecard_row("p50_attainment", "P50 attainment", "%", metrics, "p50_attainment_pct"),
        _scorecard_row("capture_rate", "Capture rate", "%", metrics, "capture_rate_pct"),
        _scorecard_row("curtailment", "Curtailment", "GWh", metrics, "curtailed_gwh", 2),
        _scorecard_row("ebitda_margin", "EBITDA margin", "%", metrics, "ebitda_margin_pct"),
        _scorecard_row("opex_per_mwh", "Opex / MWh", None, metrics, "opex_per_mwh", 0),
    ]

    # Rows with no data anywhere are noise, and curtailment that is zero
    # everywhere is not "a change" — drop them.
    def _keep(row: dict) -> bool:
        raw = [v for v in row["raw"].values() if v is not None]
        if not raw:
            return False
        if row["key"] == "curtailment" and all(v == 0 for v in raw):
            return False
        return True

    rows = [r for r in rows if _keep(r)]

    notes = []
    fin_labels = {m["financials_label"] for m in metrics.values() if m["financials_label"]}
    if fin_labels and any(r["key"] in ("ebitda_margin", "opex_per_mwh") for r in rows):
        notes.append(
            "Financial rows reflect the most recent reported fiscal year per period "
            f"({', '.join(sorted(fin_labels))}); annual filings can span several digest periods."
        )
    if len(columns) == 1:
        notes.append("No earlier generation data — comparisons unavailable for this period.")

    return {
        "columns": columns,
        "rows": rows,
        "notes": notes,
        "period_labels": {c["key"]: c["label"] for c in columns},
    }


# ── finding changes ─────────────────────────────────────────────────────


async def _severity_snapshot(ctx: ReportContext, as_of: date) -> Optional[dict]:
    """Severity counts as the detection engine last reported them at ``as_of``.

    The engine does not persist finding identity across runs (it supersedes and
    recreates), so this is deliberately count-level: the latest row per schema
    with ``detection_period_end`` within the recency window of ``as_of``.
    Returns None when no run had happened by then (no snapshot, not zeroes).
    """
    cutoff = as_of - timedelta(days=_SNAPSHOT_RECENCY_DAYS)
    result = await ctx.db.execute(
        select(Opportunity.schema_code, Opportunity.severity, Opportunity.detection_period_end)
        .where(
            Opportunity.windfarm_id == ctx.windfarm_id,
            cast(Opportunity.detection_period_end, Date) <= as_of,
            cast(Opportunity.detection_period_end, Date) > cutoff,
        )
        .order_by(Opportunity.schema_code, Opportunity.detection_period_end.desc())
    )
    latest_by_schema: dict[str, str] = {}
    for schema_code, severity, _ in result.all():
        if schema_code not in latest_by_schema:
            latest_by_schema[schema_code] = severity
    if not latest_by_schema:
        return None

    counts = {"confirmed": 0, "indicative": 0, "watch": 0, "suppressed": 0, "pass": 0}
    for severity in latest_by_schema.values():
        counts[severity.lower()] = counts.get(severity.lower(), 0) + 1
    # Pass is derived: an ACTIVE-status schema with no finding in the snapshot.
    active_schemas = [c for c in SchemaCode if SCHEMA_STATUS.get(c) == "ACTIVE"]
    counts["pass"] = sum(1 for c in active_schemas if c.value not in latest_by_schema)
    return counts


async def build_finding_changes(ctx: ReportContext) -> dict:
    """Severity count deltas vs the prior period ("Confirmed 3 → 5")."""
    start, end = ctx.period_start, ctx.period_end
    prev_start, prev_end = previous_window(start, end)

    current = await _severity_snapshot(ctx, end)
    previous = await _severity_snapshot(ctx, prev_end)

    columns = []
    if previous is not None:
        columns.append({"key": "previous", "label": f"As of {prev_end:%d %b %Y}"})
    if current is not None:
        columns.append({"key": "current", "label": f"As of {end:%d %b %Y}"})

    notes = []
    if current is None and previous is None:
        notes.append("No detection engine history for this asset in either period.")
    elif previous is None:
        notes.append(
            f"No detection history before {start:%d %b %Y} — this is the first "
            "assessed period, so deltas are unavailable."
        )
    notes.append(
        "Counts reflect the detection engine's latest run before each date; the "
        "engine tracks counts per severity, not individual finding identity."
    )

    rows = []
    if current is not None or previous is not None:
        for severity in ("confirmed", "indicative", "watch", "suppressed", "pass"):
            values = {}
            raw = {}
            if previous is not None:
                raw["previous"] = previous.get(severity, 0)
                values["previous"] = str(raw["previous"])
            if current is not None:
                raw["current"] = current.get(severity, 0)
                values["current"] = str(raw["current"])
            if severity == "suppressed" and all(v == 0 for v in raw.values()):
                continue
            row = {
                "key": severity,
                "label": severity.capitalize(),
                "unit": None,
                "values": values,
                "raw": raw,
                "direction": {},
                "delta_pct": {},
            }
            if current is not None and previous is not None:
                direction = _direction(float(raw["current"]), float(raw["previous"]))
                if direction is not None:
                    row["direction"]["previous"] = direction
            rows.append(row)

    return {"columns": columns, "rows": rows, "notes": notes}


# ── wind resource ───────────────────────────────────────────────────────


async def build_wind_resource(ctx: ReportContext) -> dict:
    """Wind resource vs the previous period, contextualising the generation
    delta: how much of the change was the wind, how much the asset."""
    start, end = ctx.period_start, ctx.period_end
    prev_start, prev_end = previous_window(start, end)

    current = await _summary_stats(ctx, start, end)
    previous = await _summary_stats(ctx, prev_start, prev_end)
    gen_current = await _generation_totals(ctx, start, end)
    gen_previous = await _generation_totals(ctx, prev_start, prev_end)

    expected = current["expected_mwh"]
    resource_delta = _delta_pct(expected, previous["expected_mwh"])
    generation_delta = _delta_pct(gen_current["generation_mwh"], gen_previous["generation_mwh"])

    cards = [
        {
            "label": "Expected generation",
            "value": _fmt(expected / 1000 if expected is not None else None),
            "unit": "GWh" if expected is not None else None,
            "raw": expected / 1000 if expected is not None else None,
        },
        {
            "label": "Resource vs prev period",
            "value": f"{resource_delta:+.1f}" if resource_delta is not None else "n/a",
            "unit": "%" if resource_delta is not None else None,
            "raw": resource_delta,
        },
        {
            "label": "Generation vs prev period",
            "value": f"{generation_delta:+.1f}" if generation_delta is not None else "n/a",
            "unit": "%" if generation_delta is not None else None,
            "raw": generation_delta,
        },
        {
            "label": "P50 attainment",
            "value": _fmt(current["p50_attainment_pct"]),
            "unit": "%" if current["p50_attainment_pct"] is not None else None,
            "raw": current["p50_attainment_pct"],
        },
    ]
    return {
        "cards": cards,
        "previous_p50_attainment_pct": previous["p50_attainment_pct"],
        "note": (
            "Expected generation is the P50 model's output for the period's actual "
            "wind conditions — the resource delta shows how much of the generation "
            "change the wind alone explains."
        ),
    }


# ── generation chart ────────────────────────────────────────────────────


async def _generation_series(
    ctx: ReportContext, start: date, end: date, granularity: str
) -> list[dict]:
    window_start, window_end = _utc_bounds(start, end)
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


async def build_generation_chart(ctx: ReportContext) -> dict:
    """chart_embed payload: the client re-renders the platform's own generation
    chart from these params; the compact series here is for the PDF renderer."""
    start, end = ctx.period_start, ctx.period_end
    prev_start, prev_end = previous_window(start, end)
    # Daily bars for a one-month window, monthly bars for anything longer.
    granularity = "day" if _months_spanned(start, end) <= 1 else "month"

    current_series = await _generation_series(ctx, start, end, granularity)
    previous_series = await _generation_series(ctx, prev_start, prev_end, granularity)

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
