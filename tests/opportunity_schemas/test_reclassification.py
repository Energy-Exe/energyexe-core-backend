"""Cross-schema reclassification post-pass tests (issue #111, M7).

PURE tests of :func:`reclassify_capture_to_cannibalisation` and
:func:`reclassify_seasonality_to_cannibalisation` over a hand-built
``dict[SchemaCode, DetectorResult]`` — no DB, no orchestrator.

Mechanism under test (chosen in #111, mirrors the DQ-01 gate's mute-but-persist
contract):
    * the reclassified symptom finding (MKT-01 low capture / OPS-02 seasonal skew)
      has its ``severity`` rewritten to ``Severity.SUPPRESSED`` with a redirect
      ``suppression_reason``; and
    * the owning MKT-03 finding records the absorbed code(s) in
      ``data_slots["reclassified_from"]``.

"CI-dominant" = MKT-03 fired AND is CONFIRMED (or its observed CI > 1.20). When
MKT-03 is absent / low / not CI-dominant, MKT-01 and OPS-02 are left untouched.
"""

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectorResult
from app.services.opportunity_schemas.mkt03_high_cannibalisation import MKT03_CI_CONFIRMED
from app.services.opportunity_schemas.registry import (
    reclassify_capture_to_cannibalisation,
    reclassify_seasonality_to_cannibalisation,
)


def _mkt01(severity: Severity = Severity.CONFIRMED) -> DetectorResult:
    return DetectorResult(
        schema_code=SchemaCode.MKT_01,
        severity=severity,
        branch="A",
        data_slots={"gap_pp": 20.0, "cannibalisation_index": 1.25},
    )


def _ops02(severity: Severity = Severity.CONFIRMED) -> DetectorResult:
    return DetectorResult(
        schema_code=SchemaCode.OPS_02,
        severity=severity,
        branch="C",
        data_slots={"hodi_pct": 9.0, "ssr": 2.0},
    )


def _mkt03_confirmed() -> DetectorResult:
    return DetectorResult(
        schema_code=SchemaCode.MKT_03,
        severity=Severity.CONFIRMED,
        branch="A",
        data_slots={"cannibalisation_index": 1.25},
    )


def _mkt03_ci_above_floor_indicative() -> DetectorResult:
    """MKT-03 not CONFIRMED but with an observed CI above the 1.20 floor.

    Mirrors a MKT-03 result whose CONFIRMED tier was trend-downgraded to
    INDICATIVE but whose raw CI still flags cannibalisation as dominant.
    """
    return DetectorResult(
        schema_code=SchemaCode.MKT_03,
        severity=Severity.INDICATIVE,
        branch="A",
        data_slots={"cannibalisation_index": 1.30},
    )


# ─────────────────── MKT-01 → MKT-03 reclassification ────────────────────────


def test_mkt01_reclassifies_to_mkt03_when_ci_dominant():
    """MKT-01 fired + MKT-03 CONFIRMED → MKT-01 SUPPRESSED (redirect reason),
    MKT-03 annotated with the absorbed code."""
    mkt01 = _mkt01(Severity.CONFIRMED)
    mkt03 = _mkt03_confirmed()
    results = {SchemaCode.MKT_01: mkt01, SchemaCode.MKT_03: mkt03}

    returned = reclassify_capture_to_cannibalisation(results)

    # Same mapping returned (mutated in place) for post-pass chaining.
    assert returned is results
    # MKT-01 reclassified: muted + reason set.
    assert mkt01.severity == Severity.SUPPRESSED
    assert mkt01.suppression_reason is not None
    assert "MKT-03" in mkt01.suppression_reason
    # MKT-03 records that it absorbed MKT-01.
    assert mkt03.data_slots["reclassified_from"] == ["MKT_01"]
    # MKT-03's own severity is untouched.
    assert mkt03.severity == Severity.CONFIRMED


def test_mkt01_reclassifies_when_mkt03_ci_above_floor_but_not_confirmed():
    """CI-dominant also holds when MKT-03 is sub-CONFIRMED but its observed CI is
    above the 1.20 floor (e.g. a trend-downgraded CONFIRMED)."""
    mkt01 = _mkt01(Severity.CONFIRMED)
    mkt03 = _mkt03_ci_above_floor_indicative()
    results = {SchemaCode.MKT_01: mkt01, SchemaCode.MKT_03: mkt03}

    reclassify_capture_to_cannibalisation(results)

    assert mkt01.severity == Severity.SUPPRESSED
    assert mkt03.data_slots["reclassified_from"] == ["MKT_01"]


def test_mkt01_not_reclassified_when_mkt03_indicative_ci_at_floor():
    """MKT-03 INDICATIVE with CI == 1.20 (not strictly above the floor) is NOT
    dominant → MKT-01 untouched."""
    mkt01 = _mkt01(Severity.CONFIRMED)
    mkt03 = DetectorResult(
        schema_code=SchemaCode.MKT_03,
        severity=Severity.INDICATIVE,
        branch="C",
        data_slots={"cannibalisation_index": MKT03_CI_CONFIRMED},  # == 1.20, not >
    )
    results = {SchemaCode.MKT_01: mkt01, SchemaCode.MKT_03: mkt03}

    reclassify_capture_to_cannibalisation(results)

    assert mkt01.severity == Severity.CONFIRMED
    assert mkt01.suppression_reason is None
    assert "reclassified_from" not in mkt03.data_slots


def test_mkt01_not_reclassified_when_already_suppressed():
    """An already-SUPPRESSED MKT-01 (e.g. data-gap gated) is left as-is and MKT-03
    is not annotated (idempotency / no double-handling)."""
    mkt01 = _mkt01(Severity.SUPPRESSED)
    mkt01.suppression_reason = "DQ-01: generation data gap detected in period"
    mkt03 = _mkt03_confirmed()
    results = {SchemaCode.MKT_01: mkt01, SchemaCode.MKT_03: mkt03}

    reclassify_capture_to_cannibalisation(results)

    # MKT-01 keeps its original suppression reason (not overwritten with redirect).
    assert mkt01.severity == Severity.SUPPRESSED
    assert mkt01.suppression_reason == "DQ-01: generation data gap detected in period"
    assert "reclassified_from" not in mkt03.data_slots


# ─────────────────── OPS-02 → MKT-03 reclassification ────────────────────────


def test_ops02_reclassifies_to_mkt03_when_ci_explains_seasonality():
    """OPS-02 fired + MKT-03 CI-dominant → OPS-02 SUPPRESSED (redirect reason),
    MKT-03 annotated."""
    ops02 = _ops02(Severity.CONFIRMED)
    mkt03 = _mkt03_confirmed()
    results = {SchemaCode.OPS_02: ops02, SchemaCode.MKT_03: mkt03}

    returned = reclassify_seasonality_to_cannibalisation(results)

    assert returned is results
    assert ops02.severity == Severity.SUPPRESSED
    assert ops02.suppression_reason is not None
    assert "MKT-03" in ops02.suppression_reason
    assert mkt03.data_slots["reclassified_from"] == ["OPS_02"]
    assert mkt03.severity == Severity.CONFIRMED


def test_both_passes_accumulate_reclassified_from_on_mkt03():
    """Running BOTH passes records both absorbed codes on MKT-03 (order-preserving,
    no duplicates)."""
    mkt01 = _mkt01(Severity.CONFIRMED)
    ops02 = _ops02(Severity.CONFIRMED)
    mkt03 = _mkt03_confirmed()
    results = {
        SchemaCode.OPS_02: ops02,
        SchemaCode.MKT_01: mkt01,
        SchemaCode.MKT_03: mkt03,
    }

    reclassify_capture_to_cannibalisation(results)
    reclassify_seasonality_to_cannibalisation(results)

    assert mkt01.severity == Severity.SUPPRESSED
    assert ops02.severity == Severity.SUPPRESSED
    assert mkt03.data_slots["reclassified_from"] == ["MKT_01", "OPS_02"]


# ─────────────────────── No reclassification cases ───────────────────────────


def test_no_reclassification_when_ci_normal():
    """MKT-03 absent → MKT-01 and OPS-02 are completely untouched."""
    mkt01 = _mkt01(Severity.CONFIRMED)
    ops02 = _ops02(Severity.CONFIRMED)
    results = {SchemaCode.MKT_01: mkt01, SchemaCode.OPS_02: ops02}

    reclassify_capture_to_cannibalisation(results)
    reclassify_seasonality_to_cannibalisation(results)

    assert mkt01.severity == Severity.CONFIRMED
    assert mkt01.suppression_reason is None
    assert ops02.severity == Severity.CONFIRMED
    assert ops02.suppression_reason is None


def test_no_reclassification_when_mkt03_low_severity():
    """MKT-03 present but WATCH with a low CI (not dominant) → no reclassification."""
    mkt01 = _mkt01(Severity.CONFIRMED)
    ops02 = _ops02(Severity.CONFIRMED)
    mkt03 = DetectorResult(
        schema_code=SchemaCode.MKT_03,
        severity=Severity.WATCH,
        branch="C",
        data_slots={"cannibalisation_index": 1.08},  # below the 1.20 floor
    )
    results = {
        SchemaCode.MKT_01: mkt01,
        SchemaCode.OPS_02: ops02,
        SchemaCode.MKT_03: mkt03,
    }

    reclassify_capture_to_cannibalisation(results)
    reclassify_seasonality_to_cannibalisation(results)

    assert mkt01.severity == Severity.CONFIRMED
    assert ops02.severity == Severity.CONFIRMED
    assert "reclassified_from" not in mkt03.data_slots


def test_no_reclassification_when_symptom_absent():
    """MKT-03 CI-dominant but neither MKT-01 nor OPS-02 fired → no-op, MKT-03 not
    annotated."""
    mkt03 = _mkt03_confirmed()
    results = {SchemaCode.MKT_03: mkt03}

    reclassify_capture_to_cannibalisation(results)
    reclassify_seasonality_to_cannibalisation(results)

    assert mkt03.severity == Severity.CONFIRMED
    assert "reclassified_from" not in mkt03.data_slots


def test_no_reclassification_when_mkt03_suppressed():
    """A SUPPRESSED MKT-03 (e.g. data-gap muted) is never the dominant explanation
    → MKT-01 / OPS-02 are left intact."""
    mkt01 = _mkt01(Severity.CONFIRMED)
    ops02 = _ops02(Severity.CONFIRMED)
    mkt03 = DetectorResult(
        schema_code=SchemaCode.MKT_03,
        severity=Severity.SUPPRESSED,
        branch="A",
        data_slots={"cannibalisation_index": 1.25},
    )
    results = {
        SchemaCode.MKT_01: mkt01,
        SchemaCode.OPS_02: ops02,
        SchemaCode.MKT_03: mkt03,
    }

    reclassify_capture_to_cannibalisation(results)
    reclassify_seasonality_to_cannibalisation(results)

    assert mkt01.severity == Severity.CONFIRMED
    assert ops02.severity == Severity.CONFIRMED
    assert "reclassified_from" not in mkt03.data_slots
