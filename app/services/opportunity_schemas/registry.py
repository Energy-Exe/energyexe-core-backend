"""Detector registry + orchestrator — the single ORM-build / persist point.

This module encodes three pieces of cross-detector knowledge plus the one
orchestrator that turns ``DetectorResult`` dataclasses into ``Opportunity`` ORM
rows:

* ``SCHEMA_REGISTRY`` — the *ordered* mapping ``SchemaCode -> detect callable``.
  Iteration order is the detection / dependency order. It starts **EMPTY**: the
  six legacy detectors (OPS-01/02/03, MKT-01/02/03) are migrated into it by
  #92/#93, and the twelve new detectors land across M3–M6. Until then the live
  detection path remains the legacy inline ``_detect_windfarm`` in
  ``opportunity_detection_service.py`` — see the module docstring there and the
  ``CRITICAL SEQUENCING CONSTRAINT`` in issue #90. ``run_for_windfarm`` is fully
  functional and fully tested against an empty / injected registry, but it is
  intentionally NOT yet wired as the live path; #93 performs that cutover once
  all six detectors are registered.

* ``SCHEMA_DEPENDENCIES`` — hard prerequisites: a detector only runs if every
  schema it lists produced a (non-``None``) result this run. The produced row's
  id is wired into the dependent row's ``triggered_by_id``. Seeded with the one
  legacy dependency (OPS_03 → OPS_01); #93 adds MKT_02 → MKT_01, and later
  issues add more (co-firing / reclassification post-passes are separate and
  arrive in #111/#112 as pure functions over ``dict[SchemaCode, DetectorResult]``).

* ``SCHEMA_STATUS`` — per-schema ACTIVE / INACTIVE. INACTIVE schemas are skipped
  entirely (no rows). MKT_05 (no PPA prices) and MKT_07 (no forecast data) are
  data-blocked and flipped to INACTIVE by #106; everything else is ACTIVE.

Detectors stay *pure*: each ``detect(ctx)`` returns ``Optional[DetectorResult]``
and never touches the DB. ``run_for_windfarm`` is the ONLY place that builds
``Opportunity`` rows, sets ``status=ACTIVE``, copies the detector's fields, wires
``triggered_by_id``, and flushes — parents before children so a dependent row can
reference its prerequisite's freshly-assigned id.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Dict, List, Optional

from app.models.opportunity import Opportunity, OpportunityStatus, SchemaCode, Severity
from app.services.opportunity_schemas import (
    dq01_data_gaps,
    fin01_p50_attainment,
    fin02_onshore_opex,
    fin03_offshore_opex,
    mkt01_low_capture_contracting,
    mkt02_low_capture_storage,
    mkt03_high_cannibalisation,
    mkt04_ppa_expiry,
    mkt05_ppa_underpricing,
    mkt06_negative_price_hours,
    mkt07_forecast_deviation,
    ops01_volatile_disruption,
    ops02_performance_seasonality,
    ops03_misaligned_contracting,
    ops04_turbine_degradation,
    ops05_grid_curtailment,
    ops06_persistent_underperformance,
    ops07_fleet_age_risk,
    ops08_structural_constraint,
)
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# A detector is an async callable: ``detect(ctx) -> Optional[DetectorResult]``.
Detector = Callable[[DetectionContext], Awaitable[Optional[DetectorResult]]]


# ─── Registry: ordered SchemaCode -> detector ───
#
# EMPTY by design. Detectors are registered by their migration / implementation
# issues, in dependency order:
#   #92 → OPS_01, OPS_02, OPS_03
#   #93 → MKT_01, MKT_03, MKT_02   (and the live-path cutover)
#   M3  → OPS_04..OPS_08
#   M4  → MKT_04, MKT_06   (MKT_05/MKT_07 registered but INACTIVE, see #106)
#   M5  → FIN_01, FIN_02, FIN_03
#   M6  → DQ_01
#
# Use an insertion-ordered dict (Python 3.7+ preserves order) so the iteration
# order *is* the detection order. New detectors must be appended in an order
# consistent with SCHEMA_DEPENDENCIES (a prerequisite must appear before its
# dependents).
#
# #92 registers OPS_01, OPS_02, OPS_03 (in that order — OPS_01 before its
# dependent OPS_03). #93 appends MKT_01, MKT_03, MKT_02 (MKT_03 is independent of
# MKT_01; MKT_02 depends on MKT_01, so MKT_01 must precede it) AND flips the live
# detection path over to ``run_for_windfarm`` (see ``_detect_windfarm`` in
# ``opportunity_detection_service.py``).
SCHEMA_REGISTRY: Dict[SchemaCode, Detector] = {
    SchemaCode.OPS_01: ops01_volatile_disruption.detect,
    SchemaCode.OPS_02: ops02_performance_seasonality.detect,
    SchemaCode.OPS_03: ops03_misaligned_contracting.detect,
    SchemaCode.MKT_01: mkt01_low_capture_contracting.detect,
    SchemaCode.MKT_03: mkt03_high_cannibalisation.detect,
    SchemaCode.MKT_02: mkt02_low_capture_storage.detect,
    # M3 — new operational detectors (no cross-schema dependency).
    SchemaCode.OPS_04: ops04_turbine_degradation.detect,  # #99
    SchemaCode.OPS_05: ops05_grid_curtailment.detect,  # #100
    SchemaCode.OPS_06: ops06_persistent_underperformance.detect,  # #101
    SchemaCode.OPS_07: ops07_fleet_age_risk.detect,  # #102 (no dependency)
    SchemaCode.OPS_08: ops08_structural_constraint.detect,  # #103 (no dependency)
    # M4 — new market detectors.
    SchemaCode.MKT_04: mkt04_ppa_expiry.detect,  # #104 (no dependency)
    SchemaCode.MKT_06: mkt06_negative_price_hours.detect,  # #105 (no dependency)
    # M4 — data-blocked schemas: REGISTERED so they are known to the engine, but
    # flipped to INACTIVE below so run_for_windfarm skips them (no rows). Their
    # detect() is a documented no-op returning None. #106 / activation tracked #116.
    SchemaCode.MKT_05: mkt05_ppa_underpricing.detect,  # #106 (INACTIVE — no PPA prices)
    SchemaCode.MKT_07: mkt07_forecast_deviation.detect,  # #106 (INACTIVE — no forecast data)
    # M5 — new financial detectors (no cross-schema dependency).
    SchemaCode.FIN_01: fin01_p50_attainment.detect,  # #107 (no dependency)
    SchemaCode.FIN_02: fin02_onshore_opex.detect,  # #108 (no dependency, onshore)
    SchemaCode.FIN_03: fin03_offshore_opex.detect,  # #108 (no dependency, offshore)
    # M6 — data-quality gate detector. Registered with NO dependency. The DQ-01
    # SUPPRESSION GATE (downstream gen-dependent schemas → SUPPRESSED when a gap
    # is present) is wired separately in #110 via apply_data_gap_gate — NOT here.
    SchemaCode.DQ_01: dq01_data_gaps.detect,  # #109 (no dependency)
}


# ─── Hard dependencies: schema -> list of prerequisite schemas ───
#
# A detector runs only if EVERY listed prerequisite produced a result this run.
# Seeded with the one legacy dependency. #93 adds ``MKT_02: [MKT_01]``; add more
# here as detectors land (keep this the single source of dependency truth).
SCHEMA_DEPENDENCIES: Dict[SchemaCode, List[SchemaCode]] = {
    SchemaCode.OPS_03: [SchemaCode.OPS_01],
    SchemaCode.MKT_02: [SchemaCode.MKT_01],
}


# ─── Per-schema status: ACTIVE / INACTIVE ───
#
# INACTIVE schemas are skipped wholesale (no per-windfarm rows). All schemas are
# ACTIVE today; MKT_05 (no PPA prices) and MKT_07 (no forecast data) are flipped
# to INACTIVE by #106. Defaulting to ACTIVE keeps any not-yet-listed code active.
SCHEMA_STATUS: Dict[SchemaCode, str] = {code: "ACTIVE" for code in SchemaCode}
# #106: MKT_05 (no PPA strike prices) and MKT_07 (no forecast data) are
# data-blocked. They are registered in SCHEMA_REGISTRY (so they are known) but
# overridden to INACTIVE here, so run_for_windfarm skips them and they emit no
# per-windfarm rows. Activation (flip back to "ACTIVE") tracked in #116.
SCHEMA_STATUS[SchemaCode.MKT_05] = "INACTIVE"
SCHEMA_STATUS[SchemaCode.MKT_07] = "INACTIVE"


# ─── DQ-01 suppression gate (#110, M6) ───
#
# When DQ-01 fires (a >= 72h generation gap is present in the detection window —
# DQ-01 only produces a finding at its CONFIRMED floor and above), every finding
# whose schema reads the (now-unreliable) generation series must be SUPPRESSED
# rather than published, so analysts are not chasing artefacts of missing data.
# Findings are still PERSISTED (severity=SUPPRESSED, status=ACTIVE — see the SUPPRESSED
# storage decision in the plan) so the gate is auditable; only their severity is
# rewritten. The gate is a PURE post-pass over the orchestrator's
# ``results_by_code: dict[SchemaCode, DetectorResult]`` — exactly like the
# co-firing / reclassification post-passes arriving in #111/#112 — so it is
# DB-free and unit-testable in isolation.
#
# GENERATION_DEPENDENT_SCHEMAS — membership rationale (per the spec; each entry
# reads the gapped ``generation_data`` series, directly or via a derived metric):
#   * OPS_01 — monthly ODI / availability is computed FROM the generation series.
#   * OPS_02 — HODI+SSR seasonality is computed FROM monthly ODI (generation).
#   * OPS_04 — turbine-degradation OLS regresses normalised OUTPUT over time.
#   * OPS_05 — curtailment % = curtailed / (curtailed + generation).
#   * OPS_06 — the wind-normalised performance index derives FROM generation.
#   * MKT_01 — capture-rate gap = generation-weighted price capture vs zone avg.
#   * MKT_02 — inherits MKT_01's capture signal (storage upside off the same gap).
#   * MKT_03 — cannibalisation index = 1 / (generation-weighted capture rate).
#   * MKT_06 — counts hours the farm GENERATES at a negative price.
#   * FIN_01 — P50 attainment = actual ANNUAL GENERATION vs the sourced P50 target.
#
# Deliberately EXCLUDED (their findings stay valid under a generation gap):
#   * DQ_01  — the gate's own trigger; suppressing it would erase the evidence.
#   * OPS_03 — date/contract-based (contract type + penalties); it only *inherits*
#              OPS_01's tier, and OPS_01 is itself suppressed, so OPS_03 is left
#              as-is rather than double-gated.
#   * OPS_07 — fleet-age risk is driven by turbine commissioning dates vs an
#              as-of date — independent of the generation series.
#   * OPS_08 — structural-constraint findings come from analyst-reviewed
#              Module-1b flags, not the raw generation series.
#   * MKT_04 — PPA expiry is purely a contract-date calculation.
#   * MKT_05 / MKT_07 — INACTIVE (no rows produced anyway).
#   * FIN_02 / FIN_03 — OPEX-per-MWh uses ``reported_generation_gwh`` from the
#              annual ``financial_data`` rows, NOT the hourly ``generation_data``
#              series DQ-01 monitors; a gap in the hourly feed does not invalidate
#              the reported annual financials, so these are NOT gen-dependent.
GENERATION_DEPENDENT_SCHEMAS: set[SchemaCode] = {
    SchemaCode.OPS_01,
    SchemaCode.OPS_02,
    SchemaCode.OPS_04,
    SchemaCode.OPS_05,
    SchemaCode.OPS_06,
    SchemaCode.MKT_01,
    SchemaCode.MKT_02,
    SchemaCode.MKT_03,
    SchemaCode.MKT_06,
    SchemaCode.FIN_01,
}

# The human-readable reason stamped onto every gen-dependent finding the gate
# suppresses. Surfaced on the persisted ``Opportunity.suppression_reason`` so the
# admin UI can explain why a finding is muted.
DATA_GAP_SUPPRESSION_REASON = "DQ-01: generation data gap detected in period"


def apply_data_gap_gate(
    results: Dict[SchemaCode, DetectorResult],
    gap_present: bool,
) -> Dict[SchemaCode, DetectorResult]:
    """Suppress generation-dependent findings when a data gap is present (#110).

    PURE post-pass over the orchestrator's ``results_by_code`` map. When
    ``gap_present`` is ``True``, every result whose ``schema_code`` is in
    :data:`GENERATION_DEPENDENT_SCHEMAS` has its ``severity`` rewritten to
    :attr:`Severity.SUPPRESSED` and its ``suppression_reason`` set to
    :data:`DATA_GAP_SUPPRESSION_REASON`. Results NOT in the set — and the DQ-01
    finding itself — are left untouched, so the data-gap evidence and any
    non-generation findings (e.g. PPA-expiry, fleet-age) still publish normally.

    When ``gap_present`` is ``False`` the ``results`` map is returned unchanged
    (the no-op the legacy / no-gap scenarios rely on for snapshot stability).

    Args:
        results: ``SchemaCode -> DetectorResult`` produced this run (the
            orchestrator's ``results_by_code``). Mutated in place AND returned for
            ergonomic chaining with the other post-passes.
        gap_present: ``True`` when DQ-01 produced a finding for this windfarm.

    Returns:
        The same ``results`` mapping (mutated in place when a gap is present).
    """
    if not gap_present:
        return results

    for schema_code, result in results.items():
        if schema_code in GENERATION_DEPENDENT_SCHEMAS:
            result.severity = Severity.SUPPRESSED
            result.suppression_reason = DATA_GAP_SUPPRESSION_REASON

    return results


async def run_for_windfarm(
    ctx: DetectionContext,
    *,
    registry: Dict[SchemaCode, Detector] = SCHEMA_REGISTRY,
    dependencies: Dict[SchemaCode, List[SchemaCode]] = SCHEMA_DEPENDENCIES,
    status: Dict[SchemaCode, str] = SCHEMA_STATUS,
    detection_run_id: Optional[int] = None,
) -> List[Opportunity]:
    """Run every registered detector for one windfarm and persist findings.

    This is the SOLE place that builds ``Opportunity`` ORM rows. Detectors are
    pure (``detect(ctx) -> Optional[DetectorResult]``); this orchestrator turns
    each non-``None`` result into one ``ACTIVE`` row, wires ``triggered_by_id``
    from its dependency's persisted row, and flushes parents before children.

    Behaviour (two phases — detect, then persist — with the DQ-01 gate between):
      * **Detection phase** — iterates ``registry`` in insertion order (=
        dependency order), collecting the non-``None`` ``DetectorResult``s into a
        ``results_by_code: dict[SchemaCode, DetectorResult]``:
          - **Status gating** — a schema whose ``status`` is ``"INACTIVE"`` is
            skipped entirely (no detector call).
          - **Dependency gating** — a schema is skipped unless EVERY prerequisite
            in ``dependencies`` produced a result earlier in this run. (Because
            the registry is dependency-ordered, prerequisites are evaluated
            first.)
          - Calls ``await detect(ctx)``; ``None`` means "no finding".
      * **DQ-01 suppression gate** (#110) — a PURE post-pass: ``gap_present`` is
        ``True`` iff DQ-01 produced a finding this run; :func:`apply_data_gap_gate`
        then rewrites every generation-dependent result's severity to
        ``SUPPRESSED`` (no-op when there is no gap). Runs AFTER detection collects
        all results and BEFORE any ``Opportunity`` is built, so the suppressed
        severities are what get persisted. (Co-firing / reclassification
        post-passes #111/#112 will hang off the same ``results_by_code`` here.)
      * **Persist phase** — builds one ``Opportunity`` per surviving
        ``DetectorResult`` (the only ORM-build point), copying ``severity`` /
        ``branch`` / ``data_slots`` / ``missing_slots`` / ``suppression_reason``,
        stamping ``status=ACTIVE`` and the detection period, then ``add`` +
        ``flush`` so the row gets an id before any dependent row references it via
        ``triggered_by_id``.

    Args:
        ctx: the per-windfarm ``DetectionContext`` (carries ``db`` + period).
        registry: ordered ``SchemaCode -> detector`` map (defaults to the module
            global; injectable so tests can pass fake detectors).
        dependencies: ``SchemaCode -> [prerequisite SchemaCode, ...]``.
        status: ``SchemaCode -> "ACTIVE" | "INACTIVE"``.
        detection_run_id: optional ``import_job_executions`` id stamped onto
            every created row.

    Returns:
        The list of created ``Opportunity`` rows (empty if nothing fired).
        Supersede of prior ACTIVE rows is handled once-per-run in
        ``OpportunityDetectionService.detect_all`` — NOT here — so this stays a
        pure additive persist over one windfarm.

    Note:
        Given the default empty ``SCHEMA_REGISTRY`` this returns ``[]`` and
        performs no DB writes — it is a safe no-op seam until #93 cuts the live
        path over to it.
    """
    # ── Phase 1: detection ──
    # Collect the pure DetectorResults (NOT yet ORM rows) in detection order so
    # the cross-schema post-passes (the DQ-01 gate now; co-firing #111/#112 later)
    # can operate over the full dict[SchemaCode, DetectorResult] before anything is
    # persisted. ``ordered_codes`` preserves the registry's iteration order for the
    # persist phase (a plain dict already preserves insertion order, but pinning
    # the order explicitly keeps parents-before-children unambiguous).
    results_by_code: Dict[SchemaCode, DetectorResult] = {}
    ordered_codes: List[SchemaCode] = []

    for schema_code, detect in registry.items():
        # Status gate: skip INACTIVE schemas wholesale.
        if status.get(schema_code, "ACTIVE") == "INACTIVE":
            continue

        # Dependency gate: every prerequisite must have produced a result.
        prereqs = dependencies.get(schema_code, [])
        if prereqs and any(p not in results_by_code for p in prereqs):
            continue

        result = await detect(ctx)
        if result is None:
            continue

        results_by_code[schema_code] = result
        ordered_codes.append(schema_code)

    # ── Phase 2: DQ-01 suppression gate (#110) ──
    # A pure post-pass over results_by_code: if DQ-01 produced a finding this run,
    # downgrade every generation-dependent result's severity to SUPPRESSED so the
    # suppressed severity is what gets persisted below. No-op when no gap fired
    # (the legacy / no-gap scenarios), keeping the M1 snapshot byte-identical.
    gap_present = SchemaCode.DQ_01 in results_by_code
    apply_data_gap_gate(results_by_code, gap_present)

    # ── Phase 3: persist ──
    # The SOLE ORM-build point. Iterate in detection order so a prerequisite's row
    # is flushed (and has an id) before its dependent row wires triggered_by_id.
    created: List[Opportunity] = []
    persisted_by_code: Dict[SchemaCode, Opportunity] = {}

    for schema_code in ordered_codes:
        result = results_by_code[schema_code]
        prereqs = dependencies.get(schema_code, [])
        triggered_by_id = _resolve_triggered_by_id(prereqs, persisted_by_code)

        opp = Opportunity(
            windfarm_id=ctx.windfarm_id,
            schema_code=result.schema_code,
            severity=result.severity,
            branch=result.branch,
            status=OpportunityStatus.ACTIVE,
            data_slots=result.data_slots,
            missing_slots=result.missing_slots,
            suppression_reason=result.suppression_reason,
            triggered_by_id=triggered_by_id,
            detection_period_start=ctx.period_start,
            detection_period_end=ctx.period_end,
            detection_run_id=detection_run_id,
        )
        ctx.db.add(opp)
        # Flush so the parent row gets an id before any dependent row below
        # references it via triggered_by_id.
        await ctx.db.flush()

        created.append(opp)
        persisted_by_code[schema_code] = opp

    return created


def _resolve_triggered_by_id(
    prereqs: List[SchemaCode], persisted_by_code: Dict[SchemaCode, Opportunity]
) -> Optional[int]:
    """Pick the ``triggered_by_id`` for a dependent row.

    A dependent row links to its (single) prerequisite's persisted id. When a
    schema declares multiple prerequisites we wire the first one present (the
    legacy engine only ever uses single-prerequisite chains: OPS_03→OPS_01,
    MKT_02→MKT_01). Returns ``None`` when there is no prerequisite.
    """
    for code in prereqs:
        parent = persisted_by_code.get(code)
        if parent is not None:
            return parent.id
    return None
