"""OPS-02 · Performance seasonality — verbatim migration (issue #92).

Reproduces the legacy ``OpportunityDetectionService._detect_ops02`` assembly
**byte-for-byte**, including the two known characteristics this migration must
preserve untouched (the rewrite lands in M2 / #96):

* OPS-02 only fires when the structurally-impossible inversion holds
  (``low_wind_cf > high_wind_cf`` → positive ``gap_pp``); and
* a CONFIRMED/INDICATIVE severity is force-capped to ``WATCH`` because
  ``wind_resource_index_monthly`` is *always* in ``missing_slots``.

Approach for pure helpers: **(A)** — the pure severity function stays on
``OpportunityDetectionService`` (so existing tests keep importing it) and is
imported / re-exported here. M2 (#96) replaces this with a HODI+SSR rewrite.

Data is obtained through ``ctx.load_seasonal_capture()`` (mirrors legacy
``_calc_seasonal_capture``) and ``ctx.load_ppa_info()``. Returns a
``DetectorResult`` or ``None``; persistence is the orchestrator's job.
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_detection_service import OpportunityDetectionService
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# ─── Pure helpers (Approach A: re-used from the legacy service, verbatim) ─────
classify_seasonality_severity = OpportunityDetectionService.determine_ops02_severity


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-02: Performance seasonality.

    Verbatim reproduction of legacy ``_detect_ops02``. Returns ``None`` when the
    legacy method would not produce a row.
    """
    seasonal = await ctx.load_seasonal_capture()
    if not seasonal or seasonal.get("high_wind_cf") is None or seasonal.get("low_wind_cf") is None:
        return None

    gap_pp = (seasonal["low_wind_cf"] - seasonal["high_wind_cf"]) * 100
    if gap_pp <= 0:
        return None  # No inversion — high-wind season performs better (expected)

    years_observed = seasonal.get("years_with_inversion", 0)
    severity = classify_seasonality_severity(gap_pp, years_observed)
    if severity is None:
        return None

    data_slots = {
        "high_wind_season_capture": round(seasonal["high_wind_cf"] * 100, 2),
        "low_wind_season_capture": round(seasonal["low_wind_cf"] * 100, 2),
        "seasonal_gap_pp": round(gap_pp, 2),
        "years_with_inversion": years_observed,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }
    missing = [
        "wind_resource_index_monthly",
        "turbine_scatter_spread",
        "cannibalisation_index_seasonal",
        "maintenance_calendar",
        "revenue_uplift_potential_eur",
    ]

    # Without wind resource index, can't confirm operational cause
    if "wind_resource_index_monthly" in missing and severity != Severity.WATCH:
        severity = Severity.WATCH

    branch = "C"  # Default to data-limited without turbine scatter or maintenance data
    missing_set = set(missing)
    if "turbine_scatter_spread" not in missing_set:
        branch = "A"
    elif "maintenance_calendar" not in missing_set:
        branch = "B"

    return DetectorResult(
        schema_code=SchemaCode.OPS_02,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
