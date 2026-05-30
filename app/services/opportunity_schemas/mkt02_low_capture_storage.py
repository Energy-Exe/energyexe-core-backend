"""MKT-02 · Low capture rates (storage) — verbatim migration (issue #93).

Reproduces the legacy ``OpportunityDetectionService._detect_mkt02`` assembly
**byte-for-byte**. MKT-02 only fires when MKT-01 fired, and it reads two values
from the MKT-01 outcome: ``mkt01.severity`` (which caps MKT-02's severity) and
``mkt01.data_slots["price_zone"]``.

How the MKT-01 prerequisite is wired
====================================
The orchestrator (``run_for_windfarm``) gates MKT-02 on MKT-01 via
``SCHEMA_DEPENDENCIES[MKT_02] = [MKT_01]`` (MKT-02 is skipped unless MKT-01
produced a row) and wires ``triggered_by_id`` from the persisted MKT-01 row.
``detect(ctx)`` itself takes only ``ctx`` (the uniform detector signature), so to
obtain the MKT-01 *severity* / *price_zone* it re-runs MKT-01's ``detect(ctx)``.

That re-run is **free and identical**: MKT-01 reads only ``ctx``-memoized data
(``load_capture_rate`` / ``load_cannibalisation_index`` / ``load_ppa_info``), so
it returns the exact same ``DetectorResult`` the orchestrator already produced —
no extra DB hits, no risk of divergence from the legacy ``mkt01`` object the
legacy method received. The severity cap below reads ``mkt01.severity`` exactly
as the legacy method read it off the persisted ``mkt01`` row.

Approach for pure helpers: **(A)** — MKT-02's severity/branch logic is small and
inline in the legacy method (no named static helper to import); it is reproduced
inline here verbatim. (M2 may name it later.)
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas import mkt01_low_capture_contracting
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-02: Low capture rates — storage. Only fires if MKT-01 triggered.

    Verbatim reproduction of legacy ``_detect_mkt02``. Returns ``None`` when the
    legacy method would not produce a row (incl. when MKT-01 did not fire, which
    the orchestrator's dependency gate also enforces).
    """
    # MKT-02 is dependent on MKT-01: it consumes the MKT-01 outcome. Re-running
    # MKT-01's detect reads only ctx-memoized data, so it returns the identical
    # DetectorResult the orchestrator persisted (same severity, same price_zone).
    mkt01 = await mkt01_low_capture_contracting.detect(ctx)
    if mkt01 is None:
        return None

    ppa_info = await ctx.load_ppa_info()

    # We don't have BESS/MFRR data yet, so this fires at WATCH with graceful degradation
    data_slots = {
        "storage_present": False,  # Assumed — no BESS data
        "price_zone": mkt01.data_slots.get("price_zone"),
        "mkt01_severity": mkt01.severity,
        "ppa_status": ppa_info.get("ppa_status"),
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }
    missing = [
        "intraday_price_spread",
        "mfrr_eligible",
        "grid_headroom_mw",
        "bess_revenue_potential_eur",
        "optimal_bess_size_mwh",
    ]

    # Severity follows MKT-01 but capped due to missing data
    if mkt01.severity == Severity.CONFIRMED:
        severity = Severity.INDICATIVE  # Downgrade: no storage data
    else:
        severity = Severity.WATCH

    branch = "C"  # Feasibility-limited — no grid/BESS data

    return DetectorResult(
        schema_code=SchemaCode.MKT_02,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
