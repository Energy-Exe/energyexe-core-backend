"""MKT-03 · High cannibalisation rates — verbatim migration (issue #93).

Reproduces the legacy ``OpportunityDetectionService._detect_mkt03`` assembly
**byte-for-byte**, including the graceful-degradation force-downgrade this
migration must preserve untouched (the recalibration lands in M2 / #98):

* a ``CONFIRMED`` severity is downgraded to ``INDICATIVE`` whenever the CI trend
  is unavailable (``ci_trend is None``) — single-year CI inputs can never reach
  CONFIRMED today.

MKT-03 is **independent of MKT-01** (it is registered between MKT-01 and MKT-02
in ``SCHEMA_REGISTRY`` and has no entry in ``SCHEMA_DEPENDENCIES``).

Approach for pure helpers: **(A)** — the pure severity / branch / suppression
functions stay on ``OpportunityDetectionService`` (so the existing tests keep
importing/calling them unchanged) and are imported and reused below. M2 (#98)
introduces corrected logic in this module.

Data is obtained through ``ctx.load_cannibalisation_index()`` (mirrors legacy
``_calc_cannibalisation_index``) and ``ctx.load_ppa_info()``. Returns a
``DetectorResult`` or ``None``; persistence is the orchestrator's job.
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_detection_service import OpportunityDetectionService
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# ─── Pure helpers (Approach A: re-used from the legacy service, verbatim) ─────
classify_cannibalisation_severity = OpportunityDetectionService.determine_mkt03_severity
select_cannibalisation_branch = OpportunityDetectionService.select_mkt03_branch
check_cannibalisation_suppression = OpportunityDetectionService.check_mkt03_suppression


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-03: High cannibalisation rates.

    Verbatim reproduction of legacy ``_detect_mkt03``. Returns ``None`` when the
    legacy method would not produce a row.
    """
    ci_data = await ctx.load_cannibalisation_index()
    if not ci_data or ci_data.get("ci_latest") is None:
        return None

    ci = ci_data["ci_latest"]
    years_sustained = ci_data.get("years_above_threshold", 0)
    severity = classify_cannibalisation_severity(ci, years_sustained)
    if severity is None:
        return None

    ppa_info = await ctx.load_ppa_info()

    # Suppression: long-dated fixed PPA
    if check_cannibalisation_suppression(ppa_info):
        return None

    data_slots = {
        "cannibalisation_index": round(ci, 4),
        "price_zone": ci_data.get("bidzone_code"),
        "ci_values_by_year": ci_data.get("ci_by_year"),
        "ci_trend_yoy": ci_data.get("ci_trend"),
        "ppa_status": ppa_info.get("ppa_status"),
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }
    missing = [
        "zone_renewable_penetration_pct",
        "peer_zone_ci",
        "portfolio_zone_correlation",
        "revenue_impact_eur",
        "alternative_zone_assets",
    ]

    # Graceful degradation: without CI trend, downgrade
    if ci_data.get("ci_trend") is None and severity == Severity.CONFIRMED:
        severity = Severity.INDICATIVE

    # Branch
    branch = select_cannibalisation_branch(ci_data)

    return DetectorResult(
        schema_code=SchemaCode.MKT_03,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
