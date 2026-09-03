"""Opportunity Report data builders (EPR-88, EPR-117).

The findings table assesses every ACTIVE schema over the *report's own window*
(EPR-117): the detection registry is run on demand — write-free, via
``evaluate_for_windfarm`` — over the requested period clipped to the last day
with generation data, the same ``effective_window`` the metric strip uses, so
evidence, charts and metrics all describe one period. Nothing is persisted:
the nightly ACTIVE board stays the live state; the report is a period-scoped
assessment. Pass rows are derived for ACTIVE schemas with no finding; schemas
whose detector needs more in-window history than the window offers are marked
Suppressed / not assessable rather than Pass. The metric strip is computed
over the same window with deltas vs the previous window (its "Schemas flagged"
card reads the persisted findings counts); the chart sections emit
``chart_embed`` payloads (client re-renders the platform's own charts, the
compact ``series`` block feeds the PDF).
"""

from datetime import date, datetime, time, timezone
from types import SimpleNamespace
from typing import Any, Optional

import structlog
from sqlalchemy import select

from app.core.database import get_session_factory
from app.models.opportunity import SchemaCode
from app.models.report import ReportSection, SectionStatus
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.evidence import format_evidence
from app.services.opportunity_schemas.ops02_performance_seasonality import (
    MIN_MONTHS_REQUIRED as _OPS02_MIN_MONTHS,
)
from app.services.opportunity_schemas.registry import (
    SCHEMA_STATUS,
    _json_safe,
    evaluate_for_windfarm,
)
from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES, SCHEMA_ONE_LINERS
from app.services.reports.context import ReportContext
from app.services.reports.data_builders.common import (
    bidzone_names,
    build_generation_chart_data,
    coverage_note,
    delta_pct,
    direction,
    effective_window,
    monthly_summaries,
    months_spanned,
    period_label,
    previous_window,
    window_metrics,
    yoy_window,
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
    "pct_over_median",
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


# Module seams — tests swap these so ``build_findings`` runs DB-free.
_session_factory = get_session_factory
_evaluate = evaluate_for_windfarm


def _severity_str(severity: Any) -> str:
    """'confirmed' from ``Severity.CONFIRMED`` or a plain 'CONFIRMED' string."""
    return str(getattr(severity, "value", severity)).lower()


def _windfarm_snapshot(ctx: ReportContext) -> Any:
    """A detached snapshot of the windfarm attributes the detectors read.

    The ORM object's relationships are selectin-loaded by the orchestrator, but
    a lazy-load inside a detector would surface as ``MissingGreenlet`` and be
    swallowed by the registry's per-detector guard — silently dropping that
    schema. A plain namespace cannot lazy-load.
    """
    wf = ctx.windfarm
    if wf is None:
        return ctx.windfarm_id
    bidzone = getattr(wf, "bidzone", None)
    country = getattr(wf, "country", None)
    return SimpleNamespace(
        id=wf.id,
        bidzone_id=getattr(wf, "bidzone_id", None),
        commercial_operational_date=getattr(wf, "commercial_operational_date", None),
        foundation_type=getattr(wf, "foundation_type", None),
        location_type=getattr(wf, "location_type", None),
        bidzone=(
            SimpleNamespace(code=getattr(bidzone, "code", None)) if bidzone is not None else None
        ),
        country=(
            SimpleNamespace(code=getattr(country, "code", None)) if country is not None else None
        ),
    )


async def _evaluate_window(ctx: ReportContext, eff_start: date, eff_end: date) -> dict:
    """Run the detection registry over ``[eff_start, eff_end]`` without persisting.

    Own throwaway session: ``run_section`` writes the section row on ``ctx.db``,
    and the detection accessors swallow their query errors — which in Postgres
    abort the transaction, so one failed detector query would turn the
    section's COMMIT into a silent rollback. The session is closed (rolled
    back) on exit; nothing is ever committed.

    The end bound is the last *instant* of the final day, not the next
    midnight: detectors derive calendar units from ``period_end`` (OPS-07 fleet
    age, MKT-04 expiry horizon, the loaders' month/year clips), and a half-open
    end would assess a 2025 report as of 2026. SQL binds (``hour < :end``) still
    include the day's 23:00 row.
    """
    start = datetime.combine(eff_start, time.min, tzinfo=timezone.utc)
    end = datetime.combine(eff_end, time.max, tzinfo=timezone.utc)
    factory = _session_factory()
    async with factory() as eval_db:
        det_ctx = DetectionContext(
            db=eval_db,
            windfarm=_windfarm_snapshot(ctx),
            period_start=start,
            period_end=end,
        )
        results, _ordered = await _evaluate(det_ctx)
    return results


def _contains_complete_calendar_year(start: date, end: date) -> bool:
    return any(
        start <= date(year, 1, 1) and end >= date(year, 12, 31)
        for year in range(start.year, end.year + 1)
    )


def _not_assessable_reason(code: SchemaCode, eff_start: date, eff_end: date) -> Optional[str]:
    """Why a schema cannot be tested over this window (``None`` = it can).

    Only schemas whose detector needs a minimum span of *in-window* data and
    returns "no finding" when short of it — those render as Suppressed / not
    assessable instead of a misleading Pass. History-based schemas (OPS-06 reads
    months before the window; FIN-02/03 count fiscal years) are not listed.
    """
    months = months_spanned(eff_start, eff_end)
    if code is SchemaCode.OPS_02 and months < _OPS02_MIN_MONTHS:
        return (
            f"Not assessable over this window: needs {_OPS02_MIN_MONTHS} months of monthly "
            f"performance data (window covers {months})."
        )
    if code is SchemaCode.FIN_01 and not _contains_complete_calendar_year(eff_start, eff_end):
        return (
            "Not assessable over this window: needs a complete calendar year (Jan–Dec) "
            "inside the window."
        )
    return None


async def build_findings(ctx: ReportContext) -> dict:
    """One row per assessed schema, Pass included (EPR-88: passes are not noise).

    EPR-117: the schemas are evaluated over the report's own window — the
    requested period clipped to the last day with generation data (the same
    ``effective_window`` the key metrics use) — by running the detection
    registry write-free. Pass is derived: an ACTIVE schema with no result.
    Schemas that cannot be tested on a window this short are Suppressed with a
    "not assessable" reason. INACTIVE schemas (data-blocked, e.g. MKT_05/MKT_07)
    are omitted entirely. Flagged rows carry formatted per-schema ``evidence``
    from ``data_slots`` (shared web/PDF).
    """
    start, end = ctx.period_start, ctx.period_end
    eff_start, eff_end, data_through = await effective_window(ctx, start, end)
    zone_names = await bidzone_names(ctx)
    results = await _evaluate_window(ctx, eff_start, eff_end)

    window = {"start": eff_start.isoformat(), "end": eff_end.isoformat()}
    months = months_spanned(eff_start, eff_end)

    rows: list[dict[str, Any]] = []
    for code in SchemaCode:
        if SCHEMA_STATUS.get(code) != "ACTIVE":
            continue
        result = results.get(code)
        evidence = None
        notes: list[str] = []
        key_metric = None
        if result is not None:
            slots = _json_safe(result.data_slots or {})
            formatted = format_evidence(code, slots, zone_names=zone_names)
            evidence = formatted["items"]
            notes = formatted["notes"]
            key_metric = _key_metric(slots)
            severity = _severity_str(result.severity)
            suppression_reason = result.suppression_reason
        else:
            suppression_reason = _not_assessable_reason(code, eff_start, eff_end)
            severity = "suppressed" if suppression_reason else "pass"
        rows.append(
            {
                "schema_code": code.value.replace("_", "-"),  # display form, e.g. FIN-01
                "domain": _DOMAINS.get(code.value.split("_")[0], "Other"),
                "display_name": SCHEMA_NAMES[code],
                "one_liner": SCHEMA_ONE_LINERS.get(code),
                "severity": severity,
                "key_metric": key_metric,
                "evidence": evidence,
                "notes": notes,
                "detection_period": dict(window),
                "suppression_reason": suppression_reason,
                "detected_at": window["end"],
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

    payload: dict[str, Any] = {
        "rows": rows,
        "severity_counts": counts,
        "assessed_schemas": len(rows),
        "evaluation_window": window,
        "label": period_label(eff_start, eff_end),
        "months_covered": months,
    }
    if data_through is not None:
        payload["data_through"] = data_through.isoformat()
        note = coverage_note(end, data_through, eff_start, eff_end)
        if note:
            payload["note"] = note
    if months < 12:
        payload["window_note"] = (
            f"Window covers {months} month{'s' if months != 1 else ''} — schemas that need a "
            "full year or more of in-window history are marked not assessable."
        )
    return payload


async def _flagged_from_findings(ctx: ReportContext) -> Optional[int]:
    """ "Schemas flagged" from the persisted findings section (EPR-117).

    The findings section evaluates the schemas over the report window; this
    section runs after it (``SectionSpec.after``) and reads its counts, so the
    card and the table cannot disagree and the evaluation runs once. ``None``
    when findings has not generated (rendered as n/a).
    """
    result = await ctx.db.execute(
        select(ReportSection.data).where(
            ReportSection.report_id == ctx.report_id,
            ReportSection.section_key == "findings",
            ReportSection.status == SectionStatus.GENERATED,
        )
    )
    data = result.scalar_one_or_none()
    if not data:
        return None
    counts = data.get("severity_counts") or {}
    return sum(int(counts.get(key) or 0) for key in ("confirmed", "indicative", "watch"))


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
    weather-adjusted attainment, generation, capacity factor, lost value,
    capture rate vs zone, schemas flagged.

    Both windows are measured over the *covered* span (EPR-111): if the farm's
    data stops mid-window, comparing what exists against a full prior period
    reports a ~50% collapse that is pure coverage artefact.

    A clipped window compares against the same dates a year earlier rather than
    against the immediately preceding span. The two are equivalent for the
    usual full-year window, but once clipping shortens it they diverge, and the
    preceding span would pit (say) a Norwegian autumn against the spring that
    came before it — trading the coverage artefact for a seasonal one.
    """
    start, end = ctx.period_start, ctx.period_end
    eff_start, eff_end, data_through = await effective_window(ctx, start, end)
    if data_through is None:
        prev_start, prev_end = previous_window(eff_start, eff_end)
    else:
        prev_start, prev_end = yoy_window(eff_start, eff_end)

    current = await window_metrics(ctx, eff_start, eff_end)
    previous = await window_metrics(ctx, prev_start, prev_end)
    # A previous window with no generation data yields meaningless deltas.
    if previous["hours_with_data"] == 0:
        previous = {key: None for key in previous}

    flagged = await _flagged_from_findings(ctx)

    cards = [
        # "Weather-adjusted", not "P50": the metric is actual vs the farm's own
        # power-curve expectation — a different denominator from the Generation
        # target (P50) that FIN-01 evidence reports.
        _card(
            "Weather-adjusted attainment",
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
        {
            "label": "Schemas flagged",
            "value": str(flagged) if flagged is not None else "n/a",
            "unit": None,
            "raw": flagged,
        },
    ]
    payload = {
        "cards": cards,
        "months_covered": current["months_covered"],
        "previous_label": period_label(prev_start, prev_end),
    }
    if data_through is not None:
        payload["data_through"] = data_through.isoformat()
        note = coverage_note(end, data_through, eff_start, eff_end)
        if note:
            payload["note"] = note
    return payload


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
