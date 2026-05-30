"""OPS-03 · Misaligned contracting strategies — M2 alignment (issue #97).

OPS-03 inherits the OPS-01 severity tier: a misaligned contract is only an
actionable opportunity insofar as the asset is *actually* suffering volatile
disruption (OPS-01), and the OPS-01 tier sets the ceiling for how severe the
contracting finding can be. With #95 removing the OPS-01 force-downgrade, OPS-01
can now reach CONFIRMED, so **OPS-03 CONFIRMED is now reachable** (it never was
under the M1 verbatim behaviour, where OPS-01 was capped at INDICATIVE).

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
of divergence.

Approach for pure helpers: #97 replaces the previously-inline severity branch with
a named, importable, DB-free helper, ``classify_contracting_severity``, mirroring
the pattern established by OPS-01's ``classify_disruption_severity``. ``detect``
calls it. The retained legacy ``OpportunityDetectionService._detect_ops03`` keeps
its own inline logic (it backs the frozen ``M1_LEGACY_BASELINE``) and is NOT
touched here.
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas import ops01_volatile_disruption
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# ─── Pure, DB-free helper (#97) ──────────────────────────────────────────────


def classify_contracting_severity(
    ops01_severity: Optional[Severity],
    contract_type: Optional[str],
    has_penalties: Optional[bool],
) -> Optional[Severity]:
    """Classify OPS-03 severity by inheriting the OPS-01 tier.

    OPS-03 only fires when the asset is genuinely suffering volatile disruption
    (OPS-01 fired); the OPS-01 tier then sets the ceiling for the contracting
    finding. Per the 15-May-2026 spec:

        * **Suppressed** (returns ``None``) when the contract carries ODI-linked
          availability penalties (``has_penalties is True``): the counterparty
          already bears the disruption risk, so there is no misalignment. Also
          suppressed when OPS-01 did not fire (``ops01_severity is None``) — there
          is nothing to inherit.
        * **CONFIRMED** when OPS-01 is CONFIRMED AND the contract is a known,
          output-agnostic type with no penalties (``has_penalties is False``):
          the asset is definitively disrupted and the contract gives the operator
          no incentive / no protection. (Now reachable post-#95.)
        * **INDICATIVE** when OPS-01 is CONFIRMED or INDICATIVE and the contract
          type is known but lacks the strong "no-penalty, output-agnostic" signal
          above (e.g. ``has_penalties`` is unknown/``None``, or OPS-01 is only
          INDICATIVE).
        * **WATCH** otherwise — borderline, OPS-01 only at WATCH, or the contract
          type is unknown (``contract_type is None``): a data-limited flag.

    Args:
        ops01_severity: the OPS-01 ``Severity`` tier (or ``None`` if OPS-01 did
            not fire).
        contract_type: the PPA contract type (``None`` when unknown).
        has_penalties: whether the contract has ODI-linked availability penalties
            (``True`` / ``False`` / ``None`` for unknown).

    Returns:
        The inherited ``Severity`` tier, or ``None`` when OPS-03 is suppressed.
    """
    # Suppression: OPS-01 absent → nothing to inherit; or the contract already
    # carries ODI-linked availability penalties → no misalignment.
    if ops01_severity is None or has_penalties is True:
        return None

    # CONFIRMED: definitive disruption (OPS-01 CONFIRMED) under a known,
    # output-agnostic contract with no penalties.
    if contract_type and has_penalties is False and ops01_severity == Severity.CONFIRMED:
        return Severity.CONFIRMED

    # INDICATIVE: known contract inheriting an OPS-01 CONFIRMED/INDICATIVE tier
    # that did not meet the CONFIRMED bar above.
    if contract_type and ops01_severity in (Severity.CONFIRMED, Severity.INDICATIVE):
        return Severity.INDICATIVE

    # WATCH: borderline / contract type unknown / OPS-01 only at WATCH.
    return Severity.WATCH


def select_contracting_branch(contract_type: Optional[str], has_penalties: Optional[bool]) -> str:
    """Select the OPS-03 root-cause branch.

    Branch A (incentive misalignment) when the contract type is known and carries
    no availability penalties; branch C (data-limited / default) otherwise.
    """
    if contract_type and has_penalties is False:
        return "A"
    return "C"


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-03: Misaligned contracting strategies. Only fires if OPS-01 triggered.

    Returns ``None`` when OPS-01 did not fire (also enforced by the orchestrator's
    dependency gate) or when the contract carries ODI-linked availability
    penalties (suppression).
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

    severity = classify_contracting_severity(ops01.severity, contract_type, has_penalties)
    # Suppression: penalties present (or OPS-01 absent, already returned above).
    if severity is None:
        return None

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

    branch = select_contracting_branch(contract_type, has_penalties)

    return DetectorResult(
        schema_code=SchemaCode.OPS_03,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
