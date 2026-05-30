"""MKT-01 · Low capture rates (contracting) — verbatim migration (issue #93).

Reproduces the legacy ``OpportunityDetectionService._detect_mkt01`` assembly
**byte-for-byte**, including the two known characteristics this migration must
preserve untouched (the fixes land in M2 / #94 and the proper reclassification
hook in #111):

* the MKT-03 reclassification short-circuit — when the cannibalisation index
  ``ci > MKT03_CI_CONFIRMED`` the detector returns ``None`` (so MKT-03 "handles
  it"); the proper reclassification hook is #111; and
* MKT-01 **never fires in production today** because the zone-average bug in
  ``compare_capture_rates_by_bidzone`` makes ``ctx.load_capture_rate()`` return
  ``None`` → this detector returns ``None`` (the data-layer fix is #94).

Approach for pure helpers: **(A)** — the pure severity / branch / suppression
functions are *not* moved here. They stay on ``OpportunityDetectionService`` (so
the existing tests in ``tests/test_opportunity_detection.py`` keep
importing/calling them unchanged) and are imported and reused below. M2 (#94)
introduces corrected logic in this module.

Data is obtained exclusively through ``DetectionContext`` accessors
(``load_capture_rate`` / ``load_cannibalisation_index`` / ``load_ppa_info``)
which mirror the legacy ``_calc_capture_rate_gap`` / ``_calc_cannibalisation_index``
/ ``_load_ppa_info`` queries. The detector returns a ``DetectorResult`` (or
``None``); persistence + ``triggered_by_id`` wiring is the orchestrator's job
(``run_for_windfarm``).
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode
from app.services.opportunity_detection_service import (
    MKT03_CI_CONFIRMED,
    OpportunityDetectionService,
)
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# ─── Pure helpers (Approach A: re-used from the legacy service, verbatim) ─────
# Re-exported names so this module is a self-contained surface and so M2 (#94)
# can swap in corrected implementations without touching call sites.
classify_capture_gap_severity = OpportunityDetectionService.determine_mkt01_severity
select_capture_branch = OpportunityDetectionService.select_mkt01_branch
check_capture_suppression = OpportunityDetectionService.check_mkt01_suppression


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-01: Low capture rates — contracting.

    Verbatim reproduction of legacy ``_detect_mkt01``. Returns ``None`` when the
    legacy method would not produce a row (incl. the never-fires zone-average bug
    when ``load_capture_rate`` is ``None`` and the MKT-03 reclassification
    short-circuit when ``ci > MKT03_CI_CONFIRMED``).
    """
    gap_data = await ctx.load_capture_rate()
    if gap_data is None:
        return None

    gap_pp = gap_data["gap_pp"]
    severity = classify_capture_gap_severity(gap_pp)
    if severity is None:
        return None

    ppa_info = await ctx.load_ppa_info()

    # Suppression
    suppression = check_capture_suppression(ppa_info, gap_data)
    if suppression:
        return None

    # CI for branch selection
    ci_data = await ctx.load_cannibalisation_index()
    ci = ci_data.get("ci_latest") if ci_data else None

    data_slots = {
        "capture_rate": gap_data.get("capture_rate"),
        "zone_avg_capture": gap_data.get("zone_avg"),
        "gap_pp": round(gap_pp, 2),
        "price_zone": gap_data.get("bidzone_code"),
        "ppa_status": ppa_info.get("ppa_status"),
        "cannibalisation_index": round(ci, 4) if ci else None,
        "ppa_expiry_date": str(ppa_info.get("ppa_end_date"))
        if ppa_info.get("ppa_end_date")
        else None,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }
    missing = []
    if ci is None:
        missing.append("cannibalisation_index")
    missing.extend(
        ["pcc_slope", "peer_capture_p50", "revenue_impact_eur", "high_wind_capture_delta"]
    )
    if not ppa_info.get("ppa_end_date"):
        missing.append("ppa_expiry_date")

    # Reclassification: if CI is dominant driver, reclassify to MKT-03
    if ci and ci > MKT03_CI_CONFIRMED:
        # MKT-03 will handle this — skip MKT-01 (proper hook is #111)
        return None

    # Branch selection
    branch = select_capture_branch(ci, ppa_info)

    return DetectorResult(
        schema_code=SchemaCode.MKT_01,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
