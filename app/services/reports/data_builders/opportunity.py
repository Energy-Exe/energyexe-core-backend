"""Opportunity Report data builders (EPR-88).

The findings table snapshots the *current* ACTIVE detection state (one row per
assessed schema, Pass rows derived for ACTIVE schemas with no finding), now
with per-schema formatted evidence from ``data_slots``; the metric strip is
computed over the report's date window with deltas vs the previous window; the
chart sections emit ``chart_embed`` payloads (client re-renders the platform's
own charts, the compact ``series`` block feeds the PDF).
"""

from typing import Any, Optional

import structlog
from sqlalchemy import select

from app.models.opportunity import Opportunity, OpportunityStatus, SchemaCode, Severity
from app.services.opportunity_schemas.evidence import format_evidence
from app.services.opportunity_schemas.registry import SCHEMA_STATUS
from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES, SCHEMA_ONE_LINERS
from app.services.reports.context import ReportContext
from app.services.reports.data_builders.common import (
    build_generation_chart_data,
    delta_pct,
    direction,
    monthly_summaries,
    period_label,
    previous_window,
    window_metrics,
)

logger = structlog.get_logger()

_DOMAINS = {"OPS": "Operational", "MKT": "Market", "FIN": "Financial", "DQ": "Data quality"}

# Severity sort order per EPR-88: Confirmed, Indicative, Watch, Pass, Suppressed last.
_SEVERITY_ORDER = ["CONFIRMED", "INDICATIVE", "WATCH", "PASS", "SUPPRESSED"]

# data_slots keys most worth surfacing as the row's single "key metric",
# best-first. Detector slot names vary per schema; this is a display heuristic.
# Kept alongside the richer per-schema evidence for old clients / frozen rows.
_KEY_METRIC_PREFERENCE = (
    "annual_loss_eur",
    "lost_eur",
    "lost_value_eur",
    "loss_eur",
    "capture_rate",
    "capture_rate_pct",
    "cannibalisation_pct",
    "attainment_pct",
    "p50_attainment",
    "opex_overrun_pct",
    "negative_price_hours",
    "curtailed_mwh",
    "gap_hours",
)


def _fmt_number(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:,.1f}M"
    if abs(value) >= 10_000:
        return f"{value:,.0f}"
    if abs(value) >= 100:
        return f"{value:,.0f}"
    return f"{value:,.2f}".rstrip("0").rstrip(".")


def _slot_label(key: str) -> str:
    label = key.replace("_", " ").strip()
    label = label.replace("eur", "EUR").replace("mwh", "MWh").replace("pct", "%")
    return label[:1].upper() + label[1:]


def _key_metric(data_slots: dict) -> Optional[str]:
    """Pick the single most display-worthy computed value for a finding row."""
    if not data_slots:
        return None
    for key in _KEY_METRIC_PREFERENCE:
        value = data_slots.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return f"{_slot_label(key)}: {_fmt_number(float(value))}"
    for key, value in data_slots.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return f"{_slot_label(key)}: {_fmt_number(float(value))}"
    return None


def _detection_period(finding: Opportunity) -> Optional[dict]:
    start = finding.detection_period_start
    end = finding.detection_period_end
    if start is None and end is None:
        return None
    return {
        "start": start.date().isoformat() if start is not None else None,
        "end": end.date().isoformat() if end is not None else None,
    }


async def build_findings(ctx: ReportContext) -> dict:
    """One row per assessed schema, Pass included (EPR-88: passes are not noise).

    Reads the current ACTIVE detection state (the engine keeps exactly one
    ACTIVE row per windfarm+schema). Pass is derived: an ACTIVE-status schema
    with no finding row produced by the engine's latest run. INACTIVE schemas
    (data-blocked, e.g. MKT_05/MKT_07) are omitted entirely. Flagged rows carry
    formatted per-schema ``evidence`` from ``data_slots`` (shared web/PDF).
    """
    result = await ctx.db.execute(
        select(Opportunity).where(
            Opportunity.windfarm_id == ctx.windfarm_id,
            Opportunity.status == OpportunityStatus.ACTIVE,
        )
    )
    by_schema = {row.schema_code: row for row in result.scalars().all()}

    rows: list[dict[str, Any]] = []
    for code in SchemaCode:
        if SCHEMA_STATUS.get(code) != "ACTIVE":
            continue
        finding = by_schema.get(code.value)
        severity = finding.severity if finding is not None else "PASS"
        evidence = None
        notes: list[str] = []
        if finding is not None:
            formatted = format_evidence(code, finding.data_slots)
            evidence = formatted["items"]
            notes = formatted["notes"]
        rows.append(
            {
                "schema_code": code.value.replace("_", "-"),  # display form, e.g. FIN-01
                "domain": _DOMAINS.get(code.value.split("_")[0], "Other"),
                "display_name": SCHEMA_NAMES[code],
                "one_liner": SCHEMA_ONE_LINERS.get(code),
                "severity": severity.lower(),
                "key_metric": _key_metric(finding.data_slots) if finding is not None else None,
                "evidence": evidence,
                "notes": notes,
                "detection_period": _detection_period(finding) if finding is not None else None,
                "suppression_reason": finding.suppression_reason if finding is not None else None,
                "detected_at": (
                    finding.detection_period_end.isoformat() if finding is not None else None
                ),
            }
        )

    rows.sort(
        key=lambda r: (
            _SEVERITY_ORDER.index(r["severity"].upper())
            if r["severity"].upper() in _SEVERITY_ORDER
            else len(_SEVERITY_ORDER)
        )
    )

    counts = {sev.lower(): 0 for sev in _SEVERITY_ORDER}
    for r in rows:
        counts[r["severity"]] = counts.get(r["severity"], 0) + 1

    return {"rows": rows, "severity_counts": counts, "assessed_schemas": len(rows)}


def _card(
    label: str,
    value: Optional[float],
    unit: Optional[str],
    fmt: str,
    previous: Optional[float] = None,
    good_direction: Optional[str] = None,
) -> dict:
    card: dict[str, Any] = {
        "label": label,
        "value": format(value, fmt) if value is not None else "n/a",
        "unit": unit if value is not None else None,
        "raw": value,
    }
    if good_direction is not None:
        card["delta_pct"] = delta_pct(value, previous)
        card["direction"] = direction(value, previous)
        card["good_direction"] = good_direction
    return card


async def build_key_metrics(ctx: ReportContext) -> dict:
    """The six-card strip with deltas vs the previous window (EPR-88 v2):
    P50 attainment, generation, capacity factor, lost value, capture rate vs
    zone, schemas flagged."""
    start, end = ctx.period_start, ctx.period_end
    prev_start, prev_end = previous_window(start, end)

    current = await window_metrics(ctx, start, end)
    previous = await window_metrics(ctx, prev_start, prev_end)
    # A previous window with no generation data yields meaningless deltas.
    if previous["hours_with_data"] == 0:
        previous = {key: None for key in previous}

    flagged_result = await ctx.db.execute(
        select(Opportunity.severity).where(
            Opportunity.windfarm_id == ctx.windfarm_id,
            Opportunity.status == OpportunityStatus.ACTIVE,
        )
    )
    flagged = sum(
        1
        for (sev,) in flagged_result.all()
        if sev in (Severity.CONFIRMED, Severity.INDICATIVE, Severity.WATCH)
    )

    cards = [
        _card(
            "P50 attainment",
            current["p50_attainment_pct"],
            "%",
            ".1f",
            previous["p50_attainment_pct"],
            "up",
        ),
        _card(
            "Generation",
            current["generation_gwh"],
            "GWh",
            ",.1f",
            previous["generation_gwh"],
            "up",
        ),
        _card(
            "Capacity factor",
            current["capacity_factor_pct"],
            "%",
            ".1f",
            previous["capacity_factor_pct"],
            "up",
        ),
        _card(
            "Lost value (period)",
            current["lost_eur"],
            "EUR",
            ",.0f",
            previous["lost_eur"],
            "down",
        ),
        _card(
            "Capture rate vs zone",
            current["capture_rate_pct"],
            "%",
            ".1f",
            previous["capture_rate_pct"],
            "up",
        ),
        {"label": "Schemas flagged", "value": str(flagged), "unit": None, "raw": flagged},
    ]
    return {
        "cards": cards,
        "months_covered": current["months_covered"],
        "previous_label": period_label(prev_start, prev_end),
    }


# ── chart sections ──────────────────────────────────────────────────────


async def build_generation_chart(ctx: ReportContext) -> dict:
    """chart_embed payload (shared implementation in ``common.py``)."""
    return await build_generation_chart_data(ctx)


async def build_wind_norm_chart(ctx: ReportContext) -> dict:
    """Monthly wind-normalised performance index (P50, baseline 100).

    The client renders the platform's own wind-normalisation chart from the
    window params; the compact ``series`` block feeds the PDF.
    """
    from datetime import date as _date

    start, end = ctx.period_start, ctx.period_end
    rows = sorted(await monthly_summaries(ctx, start, end), key=lambda r: (r.year, r.month or 1))
    points = [
        {
            "label": f"{_date(r.year, r.month or 1, 1):%b %Y}",
            "index": round(float(r.norm_index_p50), 2),
        }
        for r in rows
        if r.norm_index_p50 is not None
    ]
    return {
        "chart_key": "wind_norm_monthly",
        "windfarm_id": ctx.windfarm_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "years": list(range(start.year, end.year + 1)),
        "series": {"unit": "index", "baseline": 100, "points": points},
    }


async def build_capture_rate_chart(ctx: ReportContext) -> dict:
    """Monthly capture-rate trend vs the bidzone (%; 100% = market average)."""
    start, end = ctx.period_start, ctx.period_end
    payload: dict[str, Any] = {
        "chart_key": "capture_rate_monthly",
        "windfarm_id": ctx.windfarm_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "aggregation": "monthly",
        "series": {"unit": "%", "points": []},
    }
    try:
        from datetime import datetime, time

        from app.services.price_analytics_service import PriceAnalyticsService

        capture = await PriceAnalyticsService(ctx.db).calculate_capture_rate(
            windfarm_id=ctx.windfarm_id,
            start_date=datetime.combine(start, time.min),
            end_date=datetime.combine(end, time.max),
            aggregation="month",
        )
    except Exception as exc:  # price data is optional — the chart degrades
        logger.warning(
            "report_capture_chart_failed",
            report_id=ctx.report_id,
            windfarm_id=ctx.windfarm_id,
            error=str(exc),
        )
        payload["series"]["note"] = "Price data unavailable for this window."
        return payload

    points = []
    for period in capture.get("periods", []):
        rate = period.get("capture_rate")
        if rate is None:
            continue
        label = str(period.get("period", ""))
        try:
            from datetime import date as _date

            year, month = (int(part) for part in label.split("-")[:2])
            label = f"{_date(year, month, 1):%b %Y}"
        except (ValueError, IndexError):
            pass
        points.append(
            {
                "label": label,
                "capture_rate_pct": round(float(rate) * 100, 1),
                "achieved_price": period.get("achieved_price"),
                "market_avg_price": period.get("market_average_price"),
            }
        )
    overall = capture.get("overall", {}).get("capture_rate")
    payload["series"].update(
        {
            "points": points,
            "overall_capture_rate_pct": (
                round(float(overall) * 100, 1) if overall is not None else None
            ),
            "currency": capture.get("currency"),
        }
    )
    return payload
