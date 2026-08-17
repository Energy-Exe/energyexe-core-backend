"""Periodic Digest data builders (EPR-87).

Change-focused: every metric is computed for the report window, the previous
period, and the same period last year, with directional deltas. Comparison
windows are calendar-aware (a full-month window steps back whole months, not a
fixed day count). Missing comparison data degrades honestly — the column is
still emitted with "n/a" values, or dropped entirely when a whole snapshot has
no data (e.g. no detection history before the engine launched).

Window arithmetic and per-window metric helpers live in ``common.py`` (shared
with the Opportunity report); they are re-imported here under their original
names so existing tests and call sites keep working.
"""

from datetime import date, timedelta
from typing import Any, Optional

import structlog
from sqlalchemy import Date, cast, select

from app.models.opportunity import Opportunity, SchemaCode
from app.services.opportunity_schemas.registry import SCHEMA_STATUS
from app.services.reports.context import ReportContext
from app.services.reports.data_builders.common import (
    FLAT_TOLERANCE as _FLAT_TOLERANCE,  # noqa: F401  (re-exported for tests)
)
from app.services.reports.data_builders.common import (
    build_generation_chart_data,
    period_label,
    previous_window,
    yoy_window,
)
from app.services.reports.data_builders.common import delta_pct as _delta_pct
from app.services.reports.data_builders.common import direction as _direction
from app.services.reports.data_builders.common import fmt as _fmt
from app.services.reports.data_builders.common import generation_totals as _generation_totals
from app.services.reports.data_builders.common import summary_stats as _summary_stats
from app.services.reports.data_builders.common import utc_bounds as _utc_bounds  # noqa: F401
from app.services.reports.data_builders.common import window_metrics as _window_metrics

logger = structlog.get_logger()

# Findings older than this are treated as no longer reflecting the engine's
# state at a snapshot date (the engine re-runs and supersedes frequently).
_SNAPSHOT_RECENCY_DAYS = 60


# ── scorecard assembly ──────────────────────────────────────────────────


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


async def build_generation_chart(ctx: ReportContext) -> dict:
    """chart_embed payload (shared implementation in ``common.py``)."""
    return await build_generation_chart_data(ctx)
