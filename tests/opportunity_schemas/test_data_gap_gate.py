"""Tests for the DQ-01 suppression gate (issue #110, M6).

Two layers:

* **Pure** tests of :func:`apply_data_gap_gate` over a hand-built
  ``dict[SchemaCode, DetectorResult]`` — exhaustively checking that exactly the
  generation-dependent schemas flip to ``SUPPRESSED`` under a gap (and nothing
  else), and that no-gap is a strict no-op.
* **Integration** test driving the LIVE registry (``run_for_windfarm``) against a
  DB-free ``DetectionContext(prefetched={...})`` seeded with a >= 72h generation
  gap plus inputs that fire two generation-dependent schemas (FIN-01, OPS-06) and
  one non-generation schema (MKT-04). Asserts DQ-01 is persisted CONFIRMED, the
  gen-dependent rows are persisted SUPPRESSED (with the reason), and the
  non-generation row is untouched.

All DB-free: ``_FakeSession`` records ``add()``ed rows and assigns ids on
``flush()`` (same contract as ``tests/opportunity_schemas/test_registry.py`` and
``tests/test_opportunity_detection_integration.py``); every accessor a detector
needs is injected via ``prefetched`` so Postgres is never touched.
"""

from datetime import datetime, timedelta

import pytest

from app.models.opportunity import OpportunityStatus, SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult
from app.services.opportunity_schemas.registry import (
    DATA_GAP_SUPPRESSION_REASON,
    GENERATION_DEPENDENT_SCHEMAS,
    apply_data_gap_gate,
    run_for_windfarm,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WINDFARM_ID = 101


# ───────────────────────── DB-free harness plumbing ─────────────────────────


class _EmptyResult:
    """A SQLAlchemy-Result-shaped stub reporting 'no rows' for every pattern.

    Lets any detector whose accessor key is NOT in ``prefetched`` fall through to
    ``execute`` and resolve to ``None`` (so it simply does not fire) instead of
    raising — exactly the integration-harness pattern from
    ``tests/test_opportunity_detection_integration.py``.
    """

    def scalars(self) -> "_EmptyResult":
        return self

    def mappings(self) -> "_EmptyResult":
        return self

    def all(self) -> list:
        return []

    def first(self):
        return None

    def one_or_none(self):
        return None

    def fetchone(self):
        return None

    def fetchall(self) -> list:
        return []

    def scalar(self):
        return None

    def scalar_one_or_none(self):
        return None


class _FakeSession:
    """Minimal AsyncSession stand-in: records adds, assigns ids on flush."""

    def __init__(self) -> None:
        self.added: list = []
        self._next_id = 1

    def add(self, obj) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        for obj in self.added:
            if getattr(obj, "id", None) is None:
                obj.id = self._next_id
                self._next_id += 1

    async def execute(self, *args, **kwargs) -> "_EmptyResult":
        return _EmptyResult()


def _result(code: SchemaCode, severity: Severity = Severity.CONFIRMED) -> DetectorResult:
    """A minimal DetectorResult for the pure-gate tests."""
    return DetectorResult(schema_code=code, severity=severity, branch="A")


# ─────────────────────────────── Pure gate tests ────────────────────────────


@pytest.mark.asyncio
async def test_gate_sets_downstream_suppressed():
    """gap_present=True → every gen-dependent result is SUPPRESSED with the reason;
    non-gen-dependent results and the DQ-01 finding itself are untouched."""
    # A spread of schemas: several gen-dependent, two NOT gen-dependent, plus DQ-01.
    gen_dependent = [
        SchemaCode.OPS_01,
        SchemaCode.OPS_05,
        SchemaCode.OPS_06,
        SchemaCode.MKT_01,
        SchemaCode.MKT_03,
        SchemaCode.MKT_06,
        SchemaCode.FIN_01,
    ]
    not_gen_dependent = [SchemaCode.OPS_07, SchemaCode.MKT_04]

    results = {code: _result(code) for code in gen_dependent + not_gen_dependent}
    # The DQ-01 finding that arms the gate (must never be suppressed by it).
    results[SchemaCode.DQ_01] = _result(SchemaCode.DQ_01)

    returned = apply_data_gap_gate(results, gap_present=True)

    # Returns the same mapping (mutated in place) for post-pass chaining.
    assert returned is results

    # Every gen-dependent schema → SUPPRESSED + reason.
    for code in gen_dependent:
        assert code in GENERATION_DEPENDENT_SCHEMAS  # sanity: these ARE gen-dependent
        assert results[code].severity == Severity.SUPPRESSED
        assert results[code].suppression_reason == DATA_GAP_SUPPRESSION_REASON

    # Non-gen-dependent schemas are untouched (severity + reason unchanged).
    for code in not_gen_dependent:
        assert code not in GENERATION_DEPENDENT_SCHEMAS
        assert results[code].severity == Severity.CONFIRMED
        assert results[code].suppression_reason is None

    # DQ-01 itself is NOT suppressed (it is the evidence).
    assert SchemaCode.DQ_01 not in GENERATION_DEPENDENT_SCHEMAS
    assert results[SchemaCode.DQ_01].severity == Severity.CONFIRMED
    assert results[SchemaCode.DQ_01].suppression_reason is None


@pytest.mark.asyncio
async def test_gate_noop_when_no_gap():
    """gap_present=False → results are returned completely unchanged."""
    results = {
        SchemaCode.OPS_01: _result(SchemaCode.OPS_01, Severity.CONFIRMED),
        SchemaCode.MKT_03: _result(SchemaCode.MKT_03, Severity.INDICATIVE),
        SchemaCode.MKT_04: _result(SchemaCode.MKT_04, Severity.WATCH),
    }
    # Snapshot severities/reasons before.
    before = {c: (r.severity, r.suppression_reason) for c, r in results.items()}

    returned = apply_data_gap_gate(results, gap_present=False)

    assert returned is results
    after = {c: (r.severity, r.suppression_reason) for c, r in results.items()}
    assert after == before
    # Specifically: no SUPPRESSED anywhere, no reason set.
    assert all(r.severity != Severity.SUPPRESSED for r in results.values())
    assert all(r.suppression_reason is None for r in results.values())


def test_generation_dependent_membership_is_per_spec():
    """The membership set matches the spec list exactly (lock the rationale).

    Gen-dependent: OPS-01/02/04/05/06, MKT-01/02/03/06, FIN-01. Explicitly NOT
    gen-dependent: DQ-01 (the trigger), the date/contract schemas (OPS-03, OPS-07,
    OPS-08, MKT-04, MKT-05, MKT-07), and FIN-02/03 (reported-financial generation,
    not the hourly series).
    """
    assert GENERATION_DEPENDENT_SCHEMAS == {
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
    # DQ-01 and the date/contract/reported-financial schemas are excluded.
    for excluded in (
        SchemaCode.DQ_01,
        SchemaCode.OPS_03,
        SchemaCode.OPS_07,
        SchemaCode.OPS_08,
        SchemaCode.MKT_04,
        SchemaCode.FIN_02,
        SchemaCode.FIN_03,
    ):
        assert excluded not in GENERATION_DEPENDENT_SCHEMAS


# ─────────────────────────── Integration (live path) ────────────────────────


def _ctx_with_gap_and_findings() -> DetectionContext:
    """Build a DB-free context: a >= 72h gen gap + inputs firing FIN-01, OPS-06,
    and the non-generation MKT-04.

    Every accessor a registered detector might call is injected via ``prefetched``
    so the (empty) ``_FakeSession`` is never relied on for a firing schema. Keys
    whose value is ``None`` short-circuit the DB query and produce "no finding".
    """
    # A single 100h gap, bracketed by present data → DQ-01 CONFIRMED (>= 72h).
    g0 = datetime(2024, 6, 1, 0)
    gap = (g0 + timedelta(hours=1), g0 + timedelta(hours=101), 100)

    # OPS-06: 24 monthly points, all below 80 → a 24-month run < 80 → CONFIRMED.
    norm_index_series = [{"month": f"2024-{i:02d}", "norm_index_p50": 70.0} for i in range(1, 13)]
    norm_index_series += [{"month": f"2025-{i:02d}", "norm_index_p50": 70.0} for i in range(1, 13)]

    prefetched = {
        # DQ-01 trigger.
        "generation_gaps": [gap],
        # FIN-01 (gen-dependent): two consecutive years < 85% attainment → CONFIRMED.
        "annual_generation_gwh": {2024: 80.0, 2025: 80.0},
        "p50_target": 100.0,
        # OPS-06 (gen-dependent).
        "norm_index_series": norm_index_series,
        # MKT-04 (NOT gen-dependent): active PPA expiring in ~3 months → CONFIRMED.
        "ppa_info": {
            "ppa_buyer": "ACME Energy",
            "ppa_status": "active",
            "ppa_end_date": END.date() + timedelta(days=90),
            "ppa_price_eur_mwh": 45.0,
            "contract_type": "fixed",
        },
        # Everything else short-circuits to "no finding" so only the four above fire.
        "monthly_performance": None,
        "seasonal_capture": None,
        "capture_rate": None,
        "cannibalisation_index": None,
        "curtailment_pct": None,
        "negative_price_hours": None,
        "degradation_result": None,
        "turbine_start_dates": None,
        "structural_constraint_flags": None,
        "own_opex_financials": None,
        "zone_opex_median:onshore": None,
        "zone_opex_median:offshore": None,
    }
    return DetectionContext(
        db=_FakeSession(),
        windfarm=WINDFARM_ID,
        period_start=START,
        period_end=END,
        prefetched=prefetched,
    )


@pytest.mark.asyncio
async def test_windfarm_with_100h_gap_suppresses_downstream():
    """Live registry run with a 100h gap: DQ-01 fires CONFIRMED, generation-dependent
    findings persist as SUPPRESSED, and the non-generation finding is untouched."""
    ctx = _ctx_with_gap_and_findings()

    created = await run_for_windfarm(ctx, detection_run_id=777)

    by_code = {o.schema_code: o for o in created}

    # DQ-01 produced a finding and is persisted at its CONFIRMED tier (not suppressed).
    assert SchemaCode.DQ_01 in by_code
    dq = by_code[SchemaCode.DQ_01]
    assert dq.severity == Severity.CONFIRMED
    assert dq.status == OpportunityStatus.ACTIVE
    assert dq.suppression_reason is None
    assert dq.data_slots["max_gap_hours"] == 100

    # The generation-dependent findings (FIN-01, OPS-06) are persisted SUPPRESSED.
    for code in (SchemaCode.FIN_01, SchemaCode.OPS_06):
        assert code in by_code, f"{code} should have fired"
        row = by_code[code]
        assert row.severity == Severity.SUPPRESSED
        assert row.suppression_reason == DATA_GAP_SUPPRESSION_REASON
        # Suppressed findings are still PERSISTED (status ACTIVE, single row).
        assert row.status == OpportunityStatus.ACTIVE

    # The non-generation finding (MKT-04, PPA expiry) is UNTOUCHED by the gate.
    assert SchemaCode.MKT_04 in by_code
    mkt04 = by_code[SchemaCode.MKT_04]
    assert mkt04.severity == Severity.CONFIRMED
    assert mkt04.suppression_reason is None
    assert mkt04.status == OpportunityStatus.ACTIVE

    # Every created row is ACTIVE (suppression rewrites severity, not status).
    assert all(o.status == OpportunityStatus.ACTIVE for o in created)


@pytest.mark.asyncio
async def test_no_gap_no_suppression():
    """Complete data (no gap) → the same gen-dependent schemas fire at their real
    severity; nothing is SUPPRESSED and DQ-01 does not fire."""
    ctx = _ctx_with_gap_and_findings()
    # Remove the gap so DQ-01 does not fire → the gate is a no-op.
    ctx._cache["generation_gaps"] = []

    created = await run_for_windfarm(ctx, detection_run_id=778)
    by_code = {o.schema_code: o for o in created}

    # DQ-01 did not fire.
    assert SchemaCode.DQ_01 not in by_code

    # Gen-dependent findings keep their REAL (un-suppressed) severities.
    assert by_code[SchemaCode.FIN_01].severity == Severity.CONFIRMED
    assert by_code[SchemaCode.OPS_06].severity == Severity.CONFIRMED
    assert by_code[SchemaCode.MKT_04].severity == Severity.CONFIRMED

    # No SUPPRESSED rows anywhere; no suppression reasons set.
    assert all(o.severity != Severity.SUPPRESSED for o in created)
    assert all(o.suppression_reason is None for o in created)
