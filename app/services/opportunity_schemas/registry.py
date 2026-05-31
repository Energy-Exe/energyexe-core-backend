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


# ─── Cross-schema reclassification post-passes (#111, M7) ───
#
# Two PURE post-passes over the orchestrator's ``results_by_code`` map, hanging
# off the SAME dict the DQ-01 gate (#110) operates on. They encode the spec's
# cross-schema re-attribution: a *symptom* finding (low capture / seasonal skew)
# whose true *cause* is high cannibalisation should NOT surface as an independent
# actionable finding — the price-cannibalisation MKT-03 finding already explains
# (and owns) it.
#
# Replaces the buried ``ci > MKT03_CI_CONFIRMED → return None`` short-circuit that
# used to live INLINE in ``mkt01_low_capture_contracting.detect`` (#94 carried it
# forward verbatim; #111 lifts it out so ALL cross-schema logic lives here, in one
# auditable place over the full result set rather than inside one detector that
# cannot see the others' outcomes).
#
# Mechanism (chosen deliberately — documented for #112 and the admin UI):
#   * The reclassified finding's ``severity`` is rewritten to
#     :attr:`Severity.SUPPRESSED` and its ``suppression_reason`` set to the
#     human-readable redirect string. This MIRRORS the DQ-01 gate's
#     mute-but-persist contract (``severity=SUPPRESSED, status=ACTIVE`` — see the
#     SUPPRESSED-storage decision in the plan): the row is still PERSISTED so the
#     reclassification is fully auditable (an analyst sees the MKT-01 row, muted,
#     with "reclassified → MKT-03"), but it no longer publishes as an independent
#     actionable finding — the same NET outcome the old ``return None`` produced
#     for the live detector path (no actionable MKT-01), now expressed uniformly.
#   * The owning MKT-03 finding is annotated: its ``data_slots["reclassified_from"]``
#     accumulates the reclassified schema codes (e.g. ``["MKT_01", "OPS_02"]``) so
#     the cannibalisation finding records exactly which symptoms it absorbed.
#
# Why SUPPRESSED rather than dropping the row from the dict: dropping MKT-01 would
# orphan its dependent MKT-02 (which is gated on MKT-01 via SCHEMA_DEPENDENCIES and
# wires ``triggered_by_id`` off the persisted MKT-01 row). Keeping the MKT-01 row
# present-but-suppressed preserves that wiring and keeps the post-pass a pure,
# local severity/annotation rewrite with no dependency-graph surgery. MKT-02
# (storage upside) is intentionally left untouched: the storage opportunity is
# independent of *why* the capture gap exists.
#
# "CI-dominant" predicate — MKT-03 is the dominant driver when it fired AND is at
# its CONFIRMED tier, OR (equivalently, for a result built before the trend
# downgrade) its observed cannibalisation index exceeds the MKT-03 CONFIRMED CI
# floor (1.20). A SUPPRESSED MKT-03 (e.g. already muted by a data gap) is NOT
# dominant — there is nothing to reclassify into. This matches the legacy inline
# rule (``ci > MKT03_CI_CONFIRMED``) while also honouring a CONFIRMED MKT-03 whose
# CI sits exactly at the floor.

_RECLASSIFY_MKT01_REASON = (
    "MKT-01 reclassified to MKT-03: capture gap explained by high cannibalisation"
)
_RECLASSIFY_OPS02_REASON = (
    "OPS-02 reclassified to MKT-03: seasonal skew explained by high cannibalisation"
)


def _mkt03_is_ci_dominant(mkt03: DetectorResult) -> bool:
    """True when the MKT-03 result is the dominant (CI-driven) explanation.

    Dominant iff MKT-03 fired AND is either CONFIRMED, or its observed
    cannibalisation index exceeds the MKT-03 CONFIRMED CI floor (1.20). A
    SUPPRESSED MKT-03 (e.g. data-gap muted) is never dominant.
    """
    if mkt03.severity == Severity.SUPPRESSED:
        return False
    if mkt03.severity == Severity.CONFIRMED:
        return True
    ci = mkt03.data_slots.get("cannibalisation_index")
    return ci is not None and ci > mkt03_high_cannibalisation.MKT03_CI_CONFIRMED


def _annotate_reclassified_into(mkt03: DetectorResult, reclassified_code: SchemaCode) -> None:
    """Record on MKT-03 that it absorbed ``reclassified_code`` (idempotent)."""
    absorbed = mkt03.data_slots.setdefault("reclassified_from", [])
    code_value = reclassified_code.value
    if code_value not in absorbed:
        absorbed.append(code_value)


def reclassify_capture_to_cannibalisation(
    results: Dict[SchemaCode, DetectorResult],
) -> Dict[SchemaCode, DetectorResult]:
    """Reclassify MKT-01 (low capture) into MKT-03 when CI is the dominant driver.

    PURE post-pass over ``results_by_code``. When BOTH MKT-01 fired (and is not
    already suppressed) AND MKT-03 is CI-dominant (see :func:`_mkt03_is_ci_dominant`),
    the low-capture finding is a *symptom* of cannibalisation, not an independent
    contracting problem: MKT-01 is muted (``severity=SUPPRESSED`` + redirect
    reason) and MKT-03 records the absorbed code in
    ``data_slots["reclassified_from"]``.

    No-op (the ``results`` map is returned unchanged) when MKT-01 did not fire,
    MKT-03 did not fire, MKT-03 is not CI-dominant, or MKT-01 is already SUPPRESSED.
    This is exactly the no-op the legacy / normal-CI scenarios rely on for snapshot
    stability — it replaces the old inline ``ci > MKT03_CI_CONFIRMED → None``
    short-circuit in ``mkt01_low_capture_contracting.detect``.

    Args:
        results: ``SchemaCode -> DetectorResult`` (mutated in place AND returned
            for chaining with the other Phase-2 post-passes).

    Returns:
        The same ``results`` mapping.
    """
    mkt01 = results.get(SchemaCode.MKT_01)
    mkt03 = results.get(SchemaCode.MKT_03)
    if mkt01 is None or mkt03 is None:
        return results
    if mkt01.severity == Severity.SUPPRESSED:
        return results
    if not _mkt03_is_ci_dominant(mkt03):
        return results

    mkt01.severity = Severity.SUPPRESSED
    mkt01.suppression_reason = _RECLASSIFY_MKT01_REASON
    _annotate_reclassified_into(mkt03, SchemaCode.MKT_01)
    return results


def reclassify_seasonality_to_cannibalisation(
    results: Dict[SchemaCode, DetectorResult],
) -> Dict[SchemaCode, DetectorResult]:
    """Reclassify OPS-02 (seasonality) into MKT-03 when CI explains the seasonal skew.

    PURE post-pass over ``results_by_code``. When BOTH OPS-02 fired (and is not
    already suppressed) AND MKT-03 is CI-dominant (see :func:`_mkt03_is_ci_dominant`),
    the high-wind-season decline is explained by cannibalisation eating into the
    high-output season's price profile rather than by an operational seasonality
    problem: OPS-02 is muted (``severity=SUPPRESSED`` + redirect reason) and MKT-03
    records the absorbed code in ``data_slots["reclassified_from"]``.

    No-op (returned unchanged) when OPS-02 did not fire, MKT-03 did not fire,
    MKT-03 is not CI-dominant, or OPS-02 is already SUPPRESSED.

    Args:
        results: ``SchemaCode -> DetectorResult`` (mutated in place AND returned).

    Returns:
        The same ``results`` mapping.
    """
    ops02 = results.get(SchemaCode.OPS_02)
    mkt03 = results.get(SchemaCode.MKT_03)
    if ops02 is None or mkt03 is None:
        return results
    if ops02.severity == Severity.SUPPRESSED:
        return results
    if not _mkt03_is_ci_dominant(mkt03):
        return results

    ops02.severity = Severity.SUPPRESSED
    ops02.suppression_reason = _RECLASSIFY_OPS02_REASON
    _annotate_reclassified_into(mkt03, SchemaCode.OPS_02)
    return results


# ─── Cross-schema overlap-downgrade post-passes (#112, M7) ───
#
# Two more PURE post-passes over the same ``results_by_code`` map, encoding the
# spec's overlap relationships (#25). Unlike reclassification (#111) — which
# fully re-attributes a symptom finding to its cause (mute-but-persist) — these
# encode a SOFTER overlap: two findings share a root cause, so the *secondary*
# one is de-emphasised (one severity tier down) or merely FLAGGED PROVISIONAL,
# never suppressed.
#
#   1. MKT-06 vs MKT-03 (negative-price exposure vs cannibalisation). Both read
#      the price profile; when cannibalisation is CONFIRMED (MKT-03), the
#      negative-price-hours finding (MKT-06) is partly the same story, so MKT-06
#      is downgraded by ONE severity tier (CONFIRMED→INDICATIVE→WATCH; WATCH is
#      already the floor and stays WATCH). MKT-06's data_slots / branch are left
#      intact — only its tier moves — so the finding still publishes, just lower.
#      Left UNCHANGED whenever MKT-03 is not CONFIRMED (a non-CONFIRMED MKT-03 is
#      not a strong enough shared cause to dim MKT-06).
#
#   2. OPS-08 marks OPS-04 / OPS-06 provisional. A CONFIRMED structural
#      constraint (OPS-08, infrastructure root cause) can MASQUERADE as turbine
#      condition: a grid/connection constraint depresses output, which OPS-04
#      (turbine degradation) and OPS-06 (persistent underperformance) read off
#      the generation series. So when OPS-08 is CONFIRMED we do NOT suppress
#      OPS-04 / OPS-06 — the turbine problem may still be real — we only stamp
#      ``data_slots["provisional"] = True`` on them so an analyst knows the
#      structural constraint is a candidate alternative explanation. Their
#      severity is untouched.
#
# Both run AFTER reclassification (#111) and BEFORE the DQ-01 gap gate (#110):
# reclassification reasons on the detectors' real severities, then these overlap
# downgrades adjust the *survivors*, then the data-quality veto is applied last.
# Both are no-ops on the legacy / no-overlap scenarios, keeping the M1
# characterization snapshot byte-identical (no legacy scenario co-fires MKT-03 +
# MKT-06 or OPS-08 + OPS-04/06).

# Single-tier-down ladder for the MKT-06 overlap downgrade. WATCH is the floor:
# a WATCH finding has nowhere lower to go (the next step would be SUPPRESSED,
# which is a data-quality / reclassification verdict, NOT an overlap dimming), so
# it stays WATCH. SUPPRESSED is intentionally absent — a suppressed finding is
# already muted and must not be "downgraded" back into an active tier.
_ONE_TIER_DOWN: Dict[Severity, Severity] = {
    Severity.CONFIRMED: Severity.INDICATIVE,
    Severity.INDICATIVE: Severity.WATCH,
    Severity.WATCH: Severity.WATCH,
}

# Stamped on MKT-06 when it is dimmed because cannibalisation (MKT-03) is the
# shared, confirmed root cause — surfaced for the admin UI / analyst audit.
_MKT06_DOWNGRADE_REASON = (
    "MKT-06 downgraded one tier: negative-price exposure overlaps confirmed "
    "cannibalisation (MKT-03)"
)


def _downgrade_one_tier(severity: Severity) -> Severity:
    """Return the severity one tier below ``severity`` (WATCH is the floor).

    CONFIRMED→INDICATIVE, INDICATIVE→WATCH, WATCH→WATCH. Any other severity
    (e.g. SUPPRESSED) is returned unchanged — a muted finding is never moved.
    """
    return _ONE_TIER_DOWN.get(severity, severity)


def downgrade_negative_price_if_cannibalisation_confirmed(
    results: Dict[SchemaCode, DetectorResult],
) -> Dict[SchemaCode, DetectorResult]:
    """Downgrade MKT-06 one tier when MKT-03 is CONFIRMED (#112, overlap #25).

    PURE post-pass over ``results_by_code``. When BOTH MKT-06 fired AND MKT-03 is
    CONFIRMED, the negative-price-hours exposure (MKT-06) shares its root cause
    with the confirmed price cannibalisation (MKT-03), so MKT-06 is dimmed by
    exactly ONE severity tier via :func:`_downgrade_one_tier`
    (CONFIRMED→INDICATIVE, INDICATIVE→WATCH, WATCH stays WATCH). The finding is
    NOT suppressed — its ``data_slots`` / ``branch`` are untouched — so it still
    publishes, just at a lower tier, with a redirect note recorded on
    ``data_slots["overlap_downgraded_from"]`` for audit.

    No-op (the ``results`` map is returned unchanged) when MKT-06 did not fire,
    MKT-03 did not fire, or MKT-03 is not CONFIRMED (a non-CONFIRMED MKT-03 is too
    weak a shared cause to dim MKT-06). An already-SUPPRESSED MKT-06 is also left
    intact (``_downgrade_one_tier`` returns SUPPRESSED unchanged) — a muted
    finding is never resurrected.

    Args:
        results: ``SchemaCode -> DetectorResult`` (mutated in place AND returned
            for chaining with the other Phase-2 post-passes).

    Returns:
        The same ``results`` mapping.
    """
    mkt06 = results.get(SchemaCode.MKT_06)
    mkt03 = results.get(SchemaCode.MKT_03)
    if mkt06 is None or mkt03 is None:
        return results
    if mkt03.severity != Severity.CONFIRMED:
        return results

    downgraded = _downgrade_one_tier(mkt06.severity)
    if downgraded == mkt06.severity:
        # WATCH floor (or a non-movable severity): nothing to record.
        return results

    mkt06.data_slots["overlap_downgraded_from"] = mkt06.severity.value
    mkt06.severity = downgraded
    mkt06.suppression_reason = _MKT06_DOWNGRADE_REASON
    return results


def mark_provisional_if_structural_constraint(
    results: Dict[SchemaCode, DetectorResult],
) -> Dict[SchemaCode, DetectorResult]:
    """Flag OPS-04 / OPS-06 provisional when OPS-08 is CONFIRMED (#112, overlap #25).

    PURE post-pass over ``results_by_code``. When OPS-08 (structural constraint)
    is CONFIRMED, the infrastructure constraint can masquerade as turbine
    condition — it depresses the generation series that OPS-04 (turbine
    degradation) and OPS-06 (persistent underperformance) regress / index. So
    each of OPS-04 / OPS-06 that fired this run gets ``data_slots["provisional"]
    = True``, marking the structural constraint as a candidate alternative
    explanation. Their ``severity`` is DELIBERATELY left intact — they are NOT
    suppressed, only flagged — because the turbine problem may still be real.

    No-op (returned unchanged) when OPS-08 did not fire, OPS-08 is not CONFIRMED,
    or neither OPS-04 nor OPS-06 fired.

    Args:
        results: ``SchemaCode -> DetectorResult`` (mutated in place AND returned).

    Returns:
        The same ``results`` mapping.
    """
    ops08 = results.get(SchemaCode.OPS_08)
    if ops08 is None or ops08.severity != Severity.CONFIRMED:
        return results

    for code in (SchemaCode.OPS_04, SchemaCode.OPS_06):
        result = results.get(code)
        if result is not None:
            result.data_slots["provisional"] = True
    return results


async def run_for_windfarm(
    ctx: DetectionContext,
    *,
    registry: Dict[SchemaCode, Detector] = SCHEMA_REGISTRY,
    dependencies: Dict[SchemaCode, List[SchemaCode]] = SCHEMA_DEPENDENCIES,
    status: Dict[SchemaCode, str] = SCHEMA_STATUS,
    detection_run_id: Optional[int] = None,
    schema_codes: Optional[List[SchemaCode]] = None,
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
      * **Cross-schema post-passes** (PURE functions over ``results_by_code``,
        run AFTER detection collects all results and BEFORE any ``Opportunity`` is
        built, in this deliberate order):
          - **Reclassification** (#111) — :func:`reclassify_capture_to_cannibalisation`
            and :func:`reclassify_seasonality_to_cannibalisation` mute MKT-01 /
            OPS-02 to ``SUPPRESSED`` (with a redirect reason) and annotate MKT-03's
            ``reclassified_from`` when cannibalisation is the dominant driver. Run
            first, on the detectors' real severities.
          - **Overlap downgrades** (#112) —
            :func:`downgrade_negative_price_if_cannibalisation_confirmed` dims
            MKT-06 by one severity tier when MKT-03 is CONFIRMED, and
            :func:`mark_provisional_if_structural_constraint` stamps
            ``data_slots["provisional"]`` on OPS-04 / OPS-06 when OPS-08 is
            CONFIRMED. Run after reclassification, before the gap gate.
          - **DQ-01 suppression gate** (#110) — :func:`apply_data_gap_gate` rewrites
            every generation-dependent result's severity to ``SUPPRESSED`` when a
            gap is present (no-op otherwise). Run last (a data-quality veto).
        The rewritten severities / annotations are what get persisted.
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
        schema_codes: optional whitelist of ``SchemaCode``s to run. ``None``
            (the default) runs every registered schema — byte-identical to the
            pre-#114 behaviour. When a list is supplied, any schema NOT in it is
            skipped entirely (treated like an INACTIVE schema: no detector call,
            no result, no row). Dependency gating still applies to the survivors,
            so filtering to a dependent schema without its prerequisite simply
            yields no result for the dependent.

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

    # Schema-code whitelist (#114): when a filter is supplied, anything not in it
    # is skipped wholesale — same effect as an INACTIVE status. ``None`` = run all
    # (byte-identical to the unfiltered behaviour). Normalised to a set for O(1)
    # membership; accepts the SchemaCode enum members.
    schema_code_filter = set(schema_codes) if schema_codes is not None else None

    for schema_code, detect in registry.items():
        # Schema-code filter gate (#114): skip schemas not in the whitelist.
        if schema_code_filter is not None and schema_code not in schema_code_filter:
            continue

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

    # ── Phase 2: pure cross-schema post-passes over results_by_code ──
    # All of these are PURE functions over the collected dict[SchemaCode,
    # DetectorResult]; they run AFTER detection has gathered every result and
    # BEFORE anything is persisted, so the rewritten severities / annotations are
    # what Phase 3 writes. ORDER (deliberate, #111 + #110):
    #
    #   1. Reclassification (#111) — re-attribute symptom findings (MKT-01 low
    #      capture, OPS-02 seasonal skew) to MKT-03 when cannibalisation is the
    #      dominant driver. These run FIRST, on the detectors' REAL severities, so
    #      they reason about the true signal — never about a severity another pass
    #      has already mutated. (The #110 agent noted any pass after the gap gate
    #      sees already-SUPPRESSED severities; reclassification deliberately runs
    #      before it for exactly that reason — a data gap is a data-quality veto
    #      applied last, not an input to cross-schema re-attribution.)
    #   2. Overlap downgrades (#112) — softer cross-schema overlaps applied to the
    #      reclassification survivors: a CONFIRMED MKT-03 dims MKT-06 by one tier
    #      (shared price-cannibalisation root cause), and a CONFIRMED OPS-08
    #      (structural constraint) flags OPS-04 / OPS-06 provisional (the
    #      infrastructure constraint may explain the apparent turbine condition).
    #      Slotted AFTER reclassification (so they reason about the post-#111
    #      severities) and BEFORE the gap gate (so the data-quality veto still has
    #      the final word).
    #   3. DQ-01 suppression gate (#110) — applied LAST: if DQ-01 fired, every
    #      generation-dependent result (including a still-active MKT-03, and any
    #      already-reclassified MKT-01/OPS-02 — re-suppressing is idempotent) is
    #      muted to SUPPRESSED. No-op when no gap fired.
    #
    # FINAL Phase-2 order:
    #   reclassify_capture_to_cannibalisation
    #     → reclassify_seasonality_to_cannibalisation
    #       → downgrade_negative_price_if_cannibalisation_confirmed
    #         → mark_provisional_if_structural_constraint
    #           → apply_data_gap_gate
    #
    # All five are no-ops on the legacy / no-gap / normal-CI / no-overlap
    # scenarios, keeping the M1 characterization snapshot byte-identical.
    reclassify_capture_to_cannibalisation(results_by_code)
    reclassify_seasonality_to_cannibalisation(results_by_code)

    downgrade_negative_price_if_cannibalisation_confirmed(results_by_code)
    mark_provisional_if_structural_constraint(results_by_code)

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
