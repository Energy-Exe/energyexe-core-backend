"""Opportunity Report data builders (EPR-88).

The findings table snapshots the *current* ACTIVE detection state (one row per
assessed schema, Pass rows derived for ACTIVE schemas with no finding); the
metric strip is computed over the report's date window from
``performance_summaries`` and the price analytics service.
"""

from datetime import date, datetime, time
from typing import Any, Optional

import structlog
from sqlalchemy import select

from app.models.opportunity import Opportunity, OpportunityStatus, SchemaCode, Severity
from app.models.performance_summary import PerformanceSummary
from app.services.opportunity_schemas.registry import SCHEMA_STATUS
from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES, SCHEMA_ONE_LINERS
from app.services.reports.context import ReportContext

logger = structlog.get_logger()

_DOMAINS = {"OPS": "Operational", "MKT": "Market", "FIN": "Financial", "DQ": "Data quality"}

# Severity sort order per EPR-88: Confirmed, Indicative, Watch, Pass, Suppressed last.
_SEVERITY_ORDER = ["CONFIRMED", "INDICATIVE", "WATCH", "PASS", "SUPPRESSED"]

# data_slots keys most worth surfacing as the row's single "key metric",
# best-first. Detector slot names vary per schema; this is a display heuristic.
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


async def build_findings(ctx: ReportContext) -> dict:
    """One row per assessed schema, Pass included (EPR-88: passes are not noise).

    Reads the current ACTIVE detection state (the engine keeps exactly one
    ACTIVE row per windfarm+schema). Pass is derived: an ACTIVE-status schema
    with no finding row produced by the engine's latest run. INACTIVE schemas
    (data-blocked, e.g. MKT_05/MKT_07) are omitted entirely.
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
        rows.append(
            {
                "schema_code": code.value.replace("_", "-"),  # display form, e.g. FIN-01
                "domain": _DOMAINS.get(code.value.split("_")[0], "Other"),
                "display_name": SCHEMA_NAMES[code],
                "one_liner": SCHEMA_ONE_LINERS.get(code),
                "severity": severity.lower(),
                "key_metric": _key_metric(finding.data_slots) if finding is not None else None,
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


async def _monthly_summaries(ctx: ReportContext) -> list[PerformanceSummary]:
    result = await ctx.db.execute(
        select(PerformanceSummary).where(
            PerformanceSummary.windfarm_id == ctx.windfarm_id,
            PerformanceSummary.period_type == "month",
            PerformanceSummary.year >= ctx.period_start.year,
            PerformanceSummary.year <= ctx.period_end.year,
        )
    )
    rows = []
    for row in result.scalars().all():
        month_start = date(row.year, row.month or 1, 1)
        if ctx.period_start.replace(day=1) <= month_start <= ctx.period_end:
            rows.append(row)
    return rows


async def build_key_metrics(ctx: ReportContext) -> dict:
    """The four-card strip: P50 attainment, lost value, capture rate vs zone,
    schemas flagged (EPR-88)."""
    summaries = await _monthly_summaries(ctx)

    p50_values = [float(s.norm_ratio_p50) for s in summaries if s.norm_ratio_p50 is not None]
    p50_attainment = (sum(p50_values) / len(p50_values)) * 100 if p50_values else None

    lost_eur_values = [float(s.lost_eur) for s in summaries if s.lost_eur is not None]
    lost_value = sum(lost_eur_values) if lost_eur_values else None

    capture_rate = None
    try:
        from app.services.price_analytics_service import PriceAnalyticsService

        capture = await PriceAnalyticsService(ctx.db).calculate_capture_rate(
            windfarm_id=ctx.windfarm_id,
            start_date=datetime.combine(ctx.period_start, time.min),
            end_date=datetime.combine(ctx.period_end, time.max),
            aggregation="month",
        )
        capture_rate = capture.get("overall", {}).get("capture_rate")
        if capture_rate is not None:
            # Service returns a ratio (achieved / market average) — display as %.
            capture_rate = float(capture_rate) * 100
    except Exception as exc:  # capture data is optional — the card degrades to n/a
        logger.warning(
            "report_key_metrics_capture_rate_failed",
            report_id=ctx.report_id,
            windfarm_id=ctx.windfarm_id,
            error=str(exc),
        )

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
        {
            "label": "P50 attainment",
            "value": f"{p50_attainment:.1f}" if p50_attainment is not None else "n/a",
            "unit": "%" if p50_attainment is not None else None,
            "raw": p50_attainment,
        },
        {
            "label": "Lost value (period)",
            "value": f"{lost_value:,.0f}" if lost_value is not None else "n/a",
            "unit": "EUR" if lost_value is not None else None,
            "raw": lost_value,
        },
        {
            "label": "Capture rate vs zone",
            "value": f"{capture_rate:.1f}" if capture_rate is not None else "n/a",
            "unit": "%" if capture_rate is not None else None,
            "raw": capture_rate,
        },
        {
            "label": "Schemas flagged",
            "value": str(flagged),
            "unit": None,
            "raw": flagged,
        },
    ]
    return {"cards": cards, "months_covered": len(summaries)}
