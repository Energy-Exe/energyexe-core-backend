"""FIN-03 · Offshore OPEX overrun — M5 new detector (issue #108).

The offshore twin of FIN-02 (``fin02_onshore_opex.py``). It applies the IDENTICAL
OPEX-per-MWh formula and severity thresholds — both imported from the FIN-02
module so the logic lives in one place — but gates on ``location_type ==
"offshore"`` and benchmarks against the per-bidzone **offshore** peer median.

Onshore / offshore cohorts are kept strictly separate: an offshore farm is never
compared against the onshore median (and FIN-02 does not fire for it), so the two
detectors never cross-benchmark. See ``fin02_onshore_opex`` for the full formula,
threshold, two-full-years, and consolidated-exclusion documentation.

Snapshot safety
===============
Same as FIN-02: the M1 scenarios inject no OPEX financials, no zone median, and a
bare-int windfarm with no ``location_type``, so ``detect`` returns ``None`` for
every legacy scenario and the characterization snapshot stays byte-identical.
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Re-export the shared pure helpers so callers can import them from either module.
from app.services.opportunity_schemas.fin02_onshore_opex import (  # noqa: F401
    FIN_OPEX_CONFIRMED_MIN_FULL_YEARS,
    FIN_OPEX_CONFIRMED_PCT,
    FIN_OPEX_INDICATIVE_PCT,
    FIN_OPEX_WATCH_PCT,
    classify_opex_overrun_severity,
    compute_opex_per_mwh,
    compute_pct_over_median,
    run_opex_overrun_detector,
)

# This detector's location-type cohort.
LOCATION_TYPE = "offshore"


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """FIN-03: offshore OPEX overrun vs the per-bidzone offshore peer median.

    Fires only for offshore farms (gates on ``location_type == "offshore"``).
    Like FIN-02, CONFIRMED requires two full operating years — the first
    commissioning year is a ramp-up artefact excluded from the full-year count by
    the context accessor, so a +100% overrun on a single full year caps at
    INDICATIVE. Returns ``None`` when the farm is not offshore, has no
    ``primary_asset`` OPEX data, or its overrun is below the WATCH threshold.
    """
    return await run_opex_overrun_detector(
        ctx, schema_code=SchemaCode.FIN_03, location_type=LOCATION_TYPE
    )


__all__ = [
    "FIN_OPEX_CONFIRMED_PCT",
    "FIN_OPEX_INDICATIVE_PCT",
    "FIN_OPEX_WATCH_PCT",
    "FIN_OPEX_CONFIRMED_MIN_FULL_YEARS",
    "LOCATION_TYPE",
    "compute_opex_per_mwh",
    "classify_opex_overrun_severity",
    "compute_pct_over_median",
    "run_opex_overrun_detector",
    "detect",
]
