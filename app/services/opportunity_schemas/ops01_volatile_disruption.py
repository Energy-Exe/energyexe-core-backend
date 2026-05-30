"""OPS-01 · Volatile disruption periods — verbatim migration (issue #92).

Reproduces the legacy ``OpportunityDetectionService._detect_ops01`` assembly
**byte-for-byte**, including the two known characteristics this migration must
preserve untouched (the fixes land in M2 / #95):

* the ``wind_resource_index`` slot is *always* added to ``missing_slots``; and
* a ``CONFIRMED`` severity is force-downgraded to ``INDICATIVE`` because that slot
  is missing (so OPS-01 can never reach CONFIRMED today).

Approach for pure helpers: **(A)** — the pure severity / branch / suppression
functions are *not* moved here. They stay on ``OpportunityDetectionService`` (so
the 57 tests in ``tests/test_opportunity_detection.py`` keep importing/calling
them unchanged) and are imported and reused below. M2 (#95) introduces corrected
logic in this module.

Data is obtained exclusively through ``DetectionContext`` accessors
(``load_monthly_performance`` / ``load_ppa_info``) which mirror the legacy
``_calc_monthly_availability`` / ``_load_ppa_info`` queries. The detector returns
a ``DetectorResult`` (or ``None``); persistence + ``triggered_by_id`` wiring is
the orchestrator's job (``run_for_windfarm``).
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_detection_service import (
    ODI_THRESHOLD_PCT,
    OpportunityDetectionService,
)
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# ─── Pure helpers (Approach A: re-used from the legacy service, verbatim) ─────
# Re-exported names so this module is a self-contained surface and so M2 can swap
# in corrected implementations without touching call sites.
classify_disruption_severity = OpportunityDetectionService.determine_ops01_severity
select_disruption_branch = OpportunityDetectionService.select_ops01_branch
check_disruption_suppression = OpportunityDetectionService.check_ops01_suppression


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-01: Volatile disruption periods.

    Verbatim reproduction of legacy ``_detect_ops01``. Returns ``None`` when the
    legacy method would not produce a row.
    """
    monthly = await ctx.load_monthly_performance()
    if not monthly:
        return None

    ppa_info = await ctx.load_ppa_info()

    low_months = [m for m in monthly if m["availability_pct"] < ODI_THRESHOLD_PCT]
    severity = classify_disruption_severity(len(low_months))
    if severity is None:
        return None

    # Gather data slots
    data_slots = {
        "odi_pct": round(sum(m["availability_pct"] for m in monthly) / len(monthly), 2)
        if monthly
        else None,
        "odi_months_below_threshold": len(low_months),
        "odi_threshold": ODI_THRESHOLD_PCT,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
        "disruption_month_list": [m["month"] for m in low_months],
        "ppa_status": ppa_info.get("ppa_status"),
    }
    missing = []
    if not ppa_info.get("ppa_status"):
        missing.append("ppa_status")
    missing.extend(["peer_odi_p50", "maintenance_schedule", "wind_resource_index"])

    # Branch selection
    years_affected = len(set(m["month"][:4] for m in low_months))
    has_spot = ppa_info.get("contract_type") in (None, "merchant", "indexed")
    branch = select_disruption_branch(low_months, years_affected, has_spot)

    # Suppression
    suppression = check_disruption_suppression(ppa_info, data_slots)
    if suppression:
        return None

    # Graceful degradation: without wind_resource_index, can't confirm operational cause
    if severity == Severity.CONFIRMED and "wind_resource_index" in missing:
        severity = Severity.INDICATIVE

    return DetectorResult(
        schema_code=SchemaCode.OPS_01,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
