"""OPS-03 · Misaligned contracting strategies — verbatim migration (issue #92).

Reproduces the legacy ``OpportunityDetectionService._detect_ops03`` assembly
**byte-for-byte**. OPS-03 only fires when OPS-01 fired, and it reads two values
from the OPS-01 outcome: ``ops01.severity`` (the *force-downgraded* one) and
``ops01.data_slots["odi_pct"]``.

How the OPS-01 prerequisite is wired
====================================
The orchestrator (``run_for_windfarm``) already gates OPS-03 on OPS-01 via
``SCHEMA_DEPENDENCIES[OPS_03] = [OPS_01]`` (OPS-03 is skipped unless OPS-01
produced a row) and wires ``triggered_by_id`` from the persisted OPS-01 row.
``detect(ctx)`` itself takes only ``ctx`` (the uniform detector signature), so to
obtain the OPS-01 *severity* / *odi_pct* it re-runs OPS-01's ``detect(ctx)``.

That re-run is **free and identical**: OPS-01 reads only ``ctx``-memoized data
(``load_monthly_performance`` / ``load_ppa_info``), so it returns the exact same
``DetectorResult`` the orchestrator already produced — no extra DB hits, no risk
of divergence from the legacy ``ops01`` object the legacy method received.

Approach for pure helpers: **(A)** — OPS-03's severity/branch logic is small and
inline in the legacy method (no named static helper to import); it is reproduced
inline here verbatim. (#97 introduces a named ``classify_contracting_severity``.)
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas import ops01_volatile_disruption
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-03: Misaligned contracting strategies. Only fires if OPS-01 triggered.

    Verbatim reproduction of legacy ``_detect_ops03``. Returns ``None`` when the
    legacy method would not produce a row (incl. when OPS-01 did not fire, which
    the orchestrator's dependency gate also enforces).
    """
    # OPS-03 is dependent on OPS-01: it consumes the OPS-01 outcome. Re-running
    # OPS-01's detect reads only ctx-memoized data, so it returns the identical
    # DetectorResult the orchestrator persisted (same severity, same odi_pct).
    ops01 = await ops01_volatile_disruption.detect(ctx)
    if ops01 is None:
        return None

    ppa_info = await ctx.load_ppa_info()
    contract_type = ppa_info.get("contract_type")
    has_penalties = ppa_info.get("has_availability_penalties")

    data_slots = {
        "odi_pct": ops01.data_slots.get("odi_pct"),
        "contract_type": contract_type,
        "has_availability_penalties": has_penalties,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
        "ppa_status": ppa_info.get("ppa_status"),
    }
    missing = []
    if contract_type is None:
        missing.append("contract_type")
    if has_penalties is None:
        missing.append("contract_penalty_clauses")
    missing.extend(
        [
            "oem_response_time",
            "am_location",
            "peer_odi_p50",
            "insource_benchmark",
            "asset_age_years",
        ]
    )

    # Suppression: if contract has ODI-linked availability guarantees
    if has_penalties is True:
        return None

    # Severity
    if contract_type and has_penalties is False and ops01.severity == Severity.CONFIRMED:
        severity = Severity.CONFIRMED
    elif contract_type and ops01.severity in (Severity.CONFIRMED, Severity.INDICATIVE):
        severity = Severity.INDICATIVE
    else:
        severity = Severity.WATCH

    # Branch
    if contract_type and has_penalties is False:
        branch = "A"  # Incentive misalignment
    elif contract_type is None:
        branch = "C"  # Data-limited
    else:
        branch = "C"  # Default

    return DetectorResult(
        schema_code=SchemaCode.OPS_03,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
