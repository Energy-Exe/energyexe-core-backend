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

from app.models.opportunity import Opportunity, OpportunityStatus, SchemaCode
from app.services.opportunity_schemas import (
    mkt01_low_capture_contracting,
    mkt02_low_capture_storage,
    mkt03_high_cannibalisation,
    mkt04_ppa_expiry,
    mkt06_negative_price_hours,
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
#   M4  → MKT_04, MKT_06   (MKT_05/MKT_07 stay out — INACTIVE, see #106)
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
# MKT_05 / MKT_07 become "INACTIVE" in #106 (data-blocked).


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

    Behaviour:
      * Iterates ``registry`` in insertion order (= dependency order).
      * **Status gating** — a schema whose ``status`` is ``"INACTIVE"`` is
        skipped entirely (no detector call, no row).
      * **Dependency gating** — a schema is skipped unless EVERY prerequisite in
        ``dependencies`` produced a result earlier in this run. (Because the
        registry is dependency-ordered, prerequisites are evaluated first.)
      * Calls ``await detect(ctx)``; ``None`` means "no finding" → no row.
      * Builds one ``Opportunity`` per ``DetectorResult`` (the only ORM-build
        point), copying ``severity`` / ``branch`` / ``data_slots`` /
        ``missing_slots`` / ``suppression_reason``, stamping ``status=ACTIVE``
        and the detection period, then ``add`` + ``flush`` so the row gets an id
        before any dependent detector references it via ``triggered_by_id``.

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
    created: List[Opportunity] = []
    # Map of schema -> the persisted row it produced this run, for dependency
    # gating and triggered_by_id wiring.
    results_by_code: Dict[SchemaCode, Opportunity] = {}

    for schema_code, detect in registry.items():
        # Status gate: skip INACTIVE schemas wholesale.
        if status.get(schema_code, "ACTIVE") == "INACTIVE":
            continue

        # Dependency gate: every prerequisite must have produced a row.
        prereqs = dependencies.get(schema_code, [])
        if prereqs and any(p not in results_by_code for p in prereqs):
            continue

        result = await detect(ctx)
        if result is None:
            continue

        triggered_by_id = _resolve_triggered_by_id(prereqs, results_by_code)

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
        # Flush so the parent row gets an id before any dependent detector below
        # references it via triggered_by_id.
        await ctx.db.flush()

        created.append(opp)
        results_by_code[schema_code] = opp

    return created


def _resolve_triggered_by_id(
    prereqs: List[SchemaCode], results_by_code: Dict[SchemaCode, Opportunity]
) -> Optional[int]:
    """Pick the ``triggered_by_id`` for a dependent row.

    A dependent row links to its (single) prerequisite's persisted id. When a
    schema declares multiple prerequisites we wire the first one present (the
    legacy engine only ever uses single-prerequisite chains: OPS_03→OPS_01,
    MKT_02→MKT_01). Returns ``None`` when there is no prerequisite.
    """
    for code in prereqs:
        parent = results_by_code.get(code)
        if parent is not None:
            return parent.id
    return None
