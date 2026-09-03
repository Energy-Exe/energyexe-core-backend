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
from app.services.opportunity_schemas.evidence import format_evidence
from app.services.opportunity_schemas.registry import SCHEMA_STATUS
from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES
from app.services.reports.context import ReportContext
from app.services.reports.data_builders.common import (  # noqa: F401  (re-exported for tests)
    FLAT_TOLERANCE as _FLAT_TOLERANCE,
)
from app.services.reports.data_builders.common import build_generation_chart_data
from app.services.reports.data_builders.common import delta_pct as _delta_pct
from app.services.reports.data_builders.common import direction as _direction
from app.services.reports.data_builders.common import fmt as _fmt
from app.services.reports.data_builders.common import generation_totals as _generation_totals
from app.services.reports.data_builders.common import period_label, previous_window
from app.services.reports.data_builders.common import summary_stats as _summary_stats
from app.services.reports.data_builders.common import utc_bounds as _utc_bounds  # noqa: F401
from app.services.reports.data_builders.common import window_metrics as _window_metrics
from app.services.reports.data_builders.common import yoy_window

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


def _opex_unit(metrics: dict[str, dict]) -> Optional[str]:
    """Unit for the Opex / MWh row — the current column's filing currency (e.g. "NOK")."""
    current = (metrics.get("current") or {}).get("opex_unit")
    if current:
        return current
    for m in metrics.values():
        if m.get("opex_unit"):
            return m["opex_unit"]
    return None


def _is_full_calendar_year(start: date, end: date) -> bool:
    return start.month == 1 and start.day == 1 and end == date(start.year, 12, 31)


async def _sourced_p50_target(ctx: ReportContext, start: date, end: date) -> Optional[float]:
    """The sourced annual P50 target (GWh) valid for the window, or None.

    No latest-row fallback here: the scorecard is client-facing, so a target
    whose validity range does not cover the digest period is simply not shown.
    """
    from sqlalchemy import or_

    from app.models.p50_target import P50Target

    try:
        result = await ctx.db.execute(
            select(P50Target.p50_target_volume_gwh)
            .where(
                P50Target.windfarm_id == ctx.windfarm_id,
                P50Target.p50_target_start_date <= end,
                or_(
                    P50Target.p50_target_end_date.is_(None),
                    P50Target.p50_target_end_date >= start,
                ),
            )
            .order_by(P50Target.p50_target_start_date.desc())
        )
        value = result.scalars().first()
    except Exception:
        return None
    return float(value) if value else None


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

    # Generation target attainment (actual ÷ sourced annual P50 target) — only
    # meaningful when each column is a full calendar year, and deliberately
    # labelled apart from the weather-adjusted row: the two share the word
    # "attainment" but use different denominators, and rendering both as "P50
    # attainment" produced a contradictory client-facing Midtfjellet digest.
    # "Generation target" is the house term for the sourced P50 target
    # (EPR-117 comment 2) — never "bankable".
    generation_target = None
    if _is_full_calendar_year(start, end):
        generation_target = await _sourced_p50_target(ctx, start, end)
        if generation_target:
            for m in metrics.values():
                gen = m.get("generation_gwh")
                m["generation_target_attainment_pct"] = (
                    gen / generation_target * 100 if gen is not None else None
                )

    rows = [
        _scorecard_row("generation", "Generation", "GWh", metrics, "generation_gwh"),
        _scorecard_row("capacity_factor", "Capacity factor", "%", metrics, "capacity_factor_pct"),
        _scorecard_row(
            "p50_attainment", "Weather-adjusted attainment", "%", metrics, "p50_attainment_pct"
        ),
        _scorecard_row(
            "generation_target_attainment",
            "Generation target attainment",
            "%",
            metrics,
            "generation_target_attainment_pct",
        ),
        _scorecard_row("capture_rate", "Capture rate", "%", metrics, "capture_rate_pct"),
        _scorecard_row("curtailment", "Curtailment", "GWh", metrics, "curtailed_gwh", 2),
        _scorecard_row("ebitda_margin", "EBITDA margin", "%", metrics, "ebitda_margin_pct"),
        _scorecard_row(
            "opex_per_mwh", "Opex / MWh", _opex_unit(metrics), metrics, "opex_per_mwh", 0
        ),
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
    if any(r["key"] == "p50_attainment" for r in rows):
        notes.append(
            "Weather-adjusted attainment compares actual output to the farm's own "
            "power-curve expectation under the wind actually observed; Generation "
            "target attainment compares annual output to the sourced Generation "
            "target (P50). The two use different baselines and are not interchangeable."
        )
    if generation_target and any(r["key"] == "generation_target_attainment" for r in rows):
        notes.append(f"Generation target (P50): {_fmt(generation_target)} GWh/yr.")
    fin_labels = {m["financials_label"] for m in metrics.values() if m["financials_label"]}
    if fin_labels and any(r["key"] in ("ebitda_margin", "opex_per_mwh") for r in rows):
        notes.append(
            "Financial rows reflect the most recent reported fiscal year per period "
            f"({', '.join(sorted(fin_labels))}); annual filings can span several digest periods."
        )
    opex_units = {m.get("opex_unit") for m in metrics.values() if m.get("opex_unit")}
    if len(opex_units) > 1 and any(r["key"] == "opex_per_mwh" for r in rows):
        notes.append(
            "Opex / MWh columns are reported in different filing currencies "
            f"({', '.join(sorted(opex_units))}) and are not directly comparable."
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
    *created* within the recency window of ``as_of``. ``created_at`` is the run
    timestamp; ``detection_period_end`` is not — since EPR-126 it is clipped to
    the farm's last metered day, months behind the run for lagging feeds.
    Returns None when no run had happened by then (no snapshot, not zeroes).
    """
    cutoff = as_of - timedelta(days=_SNAPSHOT_RECENCY_DAYS)
    result = await ctx.db.execute(
        select(Opportunity.schema_code, Opportunity.severity, Opportunity.created_at)
        .where(
            Opportunity.windfarm_id == ctx.windfarm_id,
            cast(Opportunity.created_at, Date) <= as_of,
            cast(Opportunity.created_at, Date) > cutoff,
        )
        .order_by(Opportunity.schema_code, Opportunity.created_at.desc())
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


_SEVERITY_ORDER = {"confirmed": 0, "indicative": 1, "watch": 2}


async def _current_findings(ctx: ReportContext, as_of: date) -> list[dict]:
    """Latest non-pass finding per schema as of ``as_of``, with a headline value.

    The exec-summary LLM previously saw only severity COUNTS, so it could write
    "the wind resource accounts for essentially all of the shortfall" while a
    Confirmed Generation-target attainment finding sat in the same document. Naming
    the findings (code, name, severity, headline evidence) makes that
    contradiction visible to the model — and citable by the fact check.
    """
    cutoff = as_of - timedelta(days=_SNAPSHOT_RECENCY_DAYS)
    result = await ctx.db.execute(
        select(Opportunity)
        .where(
            Opportunity.windfarm_id == ctx.windfarm_id,
            # Run recency by created_at, not detection_period_end (EPR-126 clips
            # the latter to the farm's last metered day).
            cast(Opportunity.created_at, Date) <= as_of,
            cast(Opportunity.created_at, Date) > cutoff,
        )
        .order_by(Opportunity.schema_code, Opportunity.created_at.desc())
    )
    findings: list[dict] = []
    seen: set[str] = set()
    for opp in result.scalars().all():
        if opp.schema_code in seen:
            continue
        seen.add(opp.schema_code)
        severity = opp.severity.lower()  # Severity is a str-enum
        if severity not in _SEVERITY_ORDER:
            continue
        try:
            code = SchemaCode(opp.schema_code)
        except ValueError:
            continue
        formatted = format_evidence(code, opp.data_slots or {})
        headline = None
        if formatted["items"]:
            first = formatted["items"][0]
            headline = f"{first['label']}: {first['value']}"
        findings.append(
            {
                "code": opp.schema_code.replace("_", "-"),
                "name": SCHEMA_NAMES.get(code),
                "severity": severity,
                "headline": headline,
            }
        )
    findings.sort(key=lambda f: _SEVERITY_ORDER.get(f["severity"], 9))
    return findings


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

    return {
        "columns": columns,
        "rows": rows,
        "notes": notes,
        "current_findings": await _current_findings(ctx, end),
    }


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
            "label": "Weather-adjusted attainment",
            "value": _fmt(current["p50_attainment_pct"]),
            "unit": "%" if current["p50_attainment_pct"] is not None else None,
            "raw": current["p50_attainment_pct"],
        },
    ]
    return {
        "cards": cards,
        "previous_p50_attainment_pct": previous["p50_attainment_pct"],
        "note": (
            "Expected generation is the power-curve model's output for the period's "
            "actual wind conditions — the resource delta shows how much of the "
            "period-on-period generation change the wind alone explains. "
            "Weather-adjusted attainment compares actual output to that expectation; "
            "it is a different baseline from the Generation target (P50) used in "
            "attainment findings."
        ),
    }


# ── generation chart ────────────────────────────────────────────────────


async def build_generation_chart(ctx: ReportContext) -> dict:
    """chart_embed payload (shared implementation in ``common.py``)."""
    return await build_generation_chart_data(ctx)
