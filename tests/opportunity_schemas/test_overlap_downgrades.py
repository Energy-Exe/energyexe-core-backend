"""Cross-schema overlap-downgrade post-pass tests (issue #112, M7).

PURE tests of :func:`downgrade_negative_price_if_cannibalisation_confirmed` and
:func:`mark_provisional_if_structural_constraint` over a hand-built
``dict[SchemaCode, DetectorResult]`` — no DB, no orchestrator.

Mechanism under test (chosen in #112, the spec's overlap relationships #25):
    * MKT-06 (negative-price exposure) is DOWNGRADED by one severity tier when
      MKT-03 (cannibalisation) is CONFIRMED — shared price root cause. WATCH is
      the floor (stays WATCH). MKT-06 is NOT suppressed.
    * OPS-04 (turbine degradation) and OPS-06 (persistent underperformance) get
      ``data_slots["provisional"] = True`` when OPS-08 (structural constraint) is
      CONFIRMED — the infrastructure constraint may explain the apparent turbine
      condition. They are NOT suppressed and keep their severity.
"""

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectorResult
from app.services.opportunity_schemas.registry import (
    downgrade_negative_price_if_cannibalisation_confirmed,
    mark_provisional_if_structural_constraint,
)


def _mkt06(severity: Severity = Severity.CONFIRMED) -> DetectorResult:
    return DetectorResult(
        schema_code=SchemaCode.MKT_06,
        severity=severity,
        branch="A",
        data_slots={"negative_price_hours_per_year": 420},
    )


def _mkt03(severity: Severity = Severity.CONFIRMED) -> DetectorResult:
    return DetectorResult(
        schema_code=SchemaCode.MKT_03,
        severity=severity,
        branch="A",
        data_slots={"cannibalisation_index": 1.25},
    )


def _ops04(severity: Severity = Severity.INDICATIVE) -> DetectorResult:
    return DetectorResult(
        schema_code=SchemaCode.OPS_04,
        severity=severity,
        branch="A",
        data_slots={"slope_pct_per_year": -1.8},
    )


def _ops06(severity: Severity = Severity.CONFIRMED) -> DetectorResult:
    return DetectorResult(
        schema_code=SchemaCode.OPS_06,
        severity=severity,
        branch="A",
        data_slots={"consecutive_months": 9},
    )


def _ops08(severity: Severity = Severity.CONFIRMED) -> DetectorResult:
    return DetectorResult(
        schema_code=SchemaCode.OPS_08,
        severity=severity,
        branch="A",
        data_slots={"duration_hours": 800},
    )


# ────────────────── MKT-06 vs MKT-03 overlap downgrade ───────────────────────


def test_mkt06_downgraded_one_tier_when_mkt03_confirmed():
    """MKT-06 CONFIRMED + MKT-03 CONFIRMED → MKT-06 becomes INDICATIVE."""
    mkt06 = _mkt06(Severity.CONFIRMED)
    mkt03 = _mkt03(Severity.CONFIRMED)
    results = {SchemaCode.MKT_06: mkt06, SchemaCode.MKT_03: mkt03}

    returned = downgrade_negative_price_if_cannibalisation_confirmed(results)

    # Same mapping returned (mutated in place) for post-pass chaining.
    assert returned is results
    # MKT-06 dropped exactly one tier.
    assert mkt06.severity == Severity.INDICATIVE
    # Original tier recorded for audit; redirect reason set.
    assert mkt06.data_slots["overlap_downgraded_from"] == "CONFIRMED"
    assert mkt06.suppression_reason is not None
    assert "MKT-03" in mkt06.suppression_reason
    # MKT-03's own severity untouched.
    assert mkt03.severity == Severity.CONFIRMED


def test_mkt06_indicative_downgraded_to_watch():
    """INDICATIVE MKT-06 drops to WATCH (one tier)."""
    mkt06 = _mkt06(Severity.INDICATIVE)
    results = {SchemaCode.MKT_06: mkt06, SchemaCode.MKT_03: _mkt03(Severity.CONFIRMED)}

    downgrade_negative_price_if_cannibalisation_confirmed(results)

    assert mkt06.severity == Severity.WATCH
    assert mkt06.data_slots["overlap_downgraded_from"] == "INDICATIVE"


def test_mkt06_watch_stays_watch_floor():
    """WATCH is the floor: MKT-06 stays WATCH, nothing recorded."""
    mkt06 = _mkt06(Severity.WATCH)
    results = {SchemaCode.MKT_06: mkt06, SchemaCode.MKT_03: _mkt03(Severity.CONFIRMED)}

    downgrade_negative_price_if_cannibalisation_confirmed(results)

    assert mkt06.severity == Severity.WATCH
    assert "overlap_downgraded_from" not in mkt06.data_slots
    assert mkt06.suppression_reason is None


def test_mkt06_unchanged_when_mkt03_not_confirmed():
    """MKT-03 absent or not CONFIRMED → MKT-06 unchanged."""
    # MKT-03 only INDICATIVE.
    mkt06 = _mkt06(Severity.CONFIRMED)
    results = {SchemaCode.MKT_06: mkt06, SchemaCode.MKT_03: _mkt03(Severity.INDICATIVE)}
    downgrade_negative_price_if_cannibalisation_confirmed(results)
    assert mkt06.severity == Severity.CONFIRMED
    assert "overlap_downgraded_from" not in mkt06.data_slots
    assert mkt06.suppression_reason is None

    # MKT-03 entirely absent.
    mkt06_b = _mkt06(Severity.CONFIRMED)
    results_b = {SchemaCode.MKT_06: mkt06_b}
    downgrade_negative_price_if_cannibalisation_confirmed(results_b)
    assert mkt06_b.severity == Severity.CONFIRMED
    assert "overlap_downgraded_from" not in mkt06_b.data_slots


def test_mkt06_unchanged_when_absent():
    """MKT-03 CONFIRMED but MKT-06 did not fire → no-op."""
    mkt03 = _mkt03(Severity.CONFIRMED)
    results = {SchemaCode.MKT_03: mkt03}

    returned = downgrade_negative_price_if_cannibalisation_confirmed(results)

    assert returned is results
    assert mkt03.severity == Severity.CONFIRMED


# ───────────── OPS-08 marks OPS-04 / OPS-06 provisional ──────────────────────


def test_ops04_ops06_marked_provisional_when_ops08_confirmed():
    """OPS-08 CONFIRMED → OPS-04 & OPS-06 get data_slots['provisional'] == True,
    severity untouched, OPS-08 itself unchanged."""
    ops04 = _ops04(Severity.INDICATIVE)
    ops06 = _ops06(Severity.CONFIRMED)
    ops08 = _ops08(Severity.CONFIRMED)
    results = {
        SchemaCode.OPS_04: ops04,
        SchemaCode.OPS_06: ops06,
        SchemaCode.OPS_08: ops08,
    }

    returned = mark_provisional_if_structural_constraint(results)

    assert returned is results
    assert ops04.data_slots["provisional"] is True
    assert ops06.data_slots["provisional"] is True
    # Severities NOT touched — they are flagged, not suppressed.
    assert ops04.severity == Severity.INDICATIVE
    assert ops06.severity == Severity.CONFIRMED
    assert ops08.severity == Severity.CONFIRMED


def test_only_present_ops_results_marked_provisional():
    """Only the OPS-04 / OPS-06 results that actually fired get flagged."""
    ops04 = _ops04(Severity.INDICATIVE)
    results = {SchemaCode.OPS_04: ops04, SchemaCode.OPS_08: _ops08(Severity.CONFIRMED)}

    mark_provisional_if_structural_constraint(results)

    assert ops04.data_slots["provisional"] is True
    assert SchemaCode.OPS_06 not in results


def test_no_provisional_when_ops08_not_confirmed():
    """OPS-08 absent or not CONFIRMED → OPS-04 / OPS-06 left untouched."""
    # OPS-08 only INDICATIVE.
    ops04 = _ops04(Severity.INDICATIVE)
    ops06 = _ops06(Severity.CONFIRMED)
    results = {
        SchemaCode.OPS_04: ops04,
        SchemaCode.OPS_06: ops06,
        SchemaCode.OPS_08: _ops08(Severity.INDICATIVE),
    }
    mark_provisional_if_structural_constraint(results)
    assert "provisional" not in ops04.data_slots
    assert "provisional" not in ops06.data_slots

    # OPS-08 entirely absent.
    ops04_b = _ops04(Severity.INDICATIVE)
    ops06_b = _ops06(Severity.CONFIRMED)
    results_b = {SchemaCode.OPS_04: ops04_b, SchemaCode.OPS_06: ops06_b}
    mark_provisional_if_structural_constraint(results_b)
    assert "provisional" not in ops04_b.data_slots
    assert "provisional" not in ops06_b.data_slots
