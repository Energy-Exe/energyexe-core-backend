"""Tests for the detector registry + ``run_for_windfarm`` orchestrator.

All tests are DB-free. Detectors are tiny ``async def`` callables returning a
``DetectorResult`` (or ``None``). Persistence is exercised against a
``FakeSession`` that records ``add()``ed objects and, on ``flush()``, assigns
each still-unsaved object a sequential ``id`` — exactly the behaviour
``run_for_windfarm`` relies on to wire ``triggered_by_id`` from a parent's
freshly-assigned id. No live Postgres / JSONB is needed.
"""

from datetime import datetime

import pytest

from app.models.opportunity import Opportunity, OpportunityStatus, SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult
from app.services.opportunity_schemas.registry import run_for_windfarm

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)


class FakeSession:
    """Minimal stand-in for an AsyncSession used by ``run_for_windfarm``.

    Records every ``add()``ed object in ``self.added`` (insertion order) and, on
    each ``await flush()``, assigns a sequential ``id`` to any added object that
    does not already have one. This mirrors Postgres autoincrement closely enough
    that ``triggered_by_id`` wiring (which reads ``parent.id`` after a flush) works
    without a real DB. Reusable for any later orchestrator/persistence test.
    """

    def __init__(self) -> None:
        self.added: list = []
        self._next_id = 1
        self.flush_count = 0

    def add(self, obj) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        self.flush_count += 1
        for obj in self.added:
            if getattr(obj, "id", None) is None:
                obj.id = self._next_id
                self._next_id += 1


def _make_ctx() -> DetectionContext:
    """A DetectionContext bound to a FakeSession for windfarm 1."""
    return DetectionContext(db=FakeSession(), windfarm=1, period_start=START, period_end=END)


def _detector(result):
    """Build a fake async detector that always returns ``result``."""

    async def detect(ctx: DetectionContext):
        return result

    return detect


@pytest.mark.asyncio
async def test_empty_registry_returns_no_rows():
    """An empty registry produces no findings and writes nothing."""
    ctx = _make_ctx()
    created = await run_for_windfarm(ctx, registry={})
    assert created == []
    assert ctx.db.added == []
    assert ctx.db.flush_count == 0


@pytest.mark.asyncio
async def test_results_persisted_as_active_opportunity_rows():
    """Two detectors that fire → two ACTIVE rows with correct code/severity."""
    ctx = _make_ctx()
    registry = {
        SchemaCode.OPS_01: _detector(
            DetectorResult(
                schema_code=SchemaCode.OPS_01,
                severity=Severity.CONFIRMED,
                branch="A",
                data_slots={"odi_pct": 90.0},
                missing_slots=["peer_odi_p50"],
            )
        ),
        SchemaCode.MKT_01: _detector(
            DetectorResult(
                schema_code=SchemaCode.MKT_01,
                severity=Severity.WATCH,
                data_slots={"gap_pp": 3.0},
            )
        ),
    }

    created = await run_for_windfarm(ctx, registry=registry, dependencies={})

    assert len(created) == 2
    assert ctx.db.added == created  # the orchestrator is the sole row builder
    for opp in created:
        assert isinstance(opp, Opportunity)
        assert opp.status == OpportunityStatus.ACTIVE
        assert opp.windfarm_id == 1
        assert opp.detection_period_start == START
        assert opp.detection_period_end == END

    by_code = {o.schema_code: o for o in created}
    assert by_code[SchemaCode.OPS_01].severity == Severity.CONFIRMED
    assert by_code[SchemaCode.OPS_01].branch == "A"
    assert by_code[SchemaCode.OPS_01].data_slots == {"odi_pct": 90.0}
    assert by_code[SchemaCode.OPS_01].missing_slots == ["peer_odi_p50"]
    assert by_code[SchemaCode.MKT_01].severity == Severity.WATCH


@pytest.mark.asyncio
async def test_one_detector_exception_does_not_abort_others():
    """A detector that raises is skipped + logged; the others still persist.

    Reliability guard (B1): before the per-detector try/except, a single detector
    raising propagated out of ``run_for_windfarm`` and aborted ALL schemas for the
    windfarm (zero rows). Now the bad schema is dropped and the good ones survive.
    """
    ctx = _make_ctx()

    async def boom(c):
        raise RuntimeError("detector blew up")

    registry = {
        SchemaCode.OPS_01: _detector(
            DetectorResult(schema_code=SchemaCode.OPS_01, severity=Severity.CONFIRMED)
        ),
        SchemaCode.OPS_04: boom,  # raises mid-run
        SchemaCode.MKT_01: _detector(
            DetectorResult(schema_code=SchemaCode.MKT_01, severity=Severity.WATCH)
        ),
    }

    created = await run_for_windfarm(ctx, registry=registry, dependencies={})

    codes = {o.schema_code for o in created}
    assert codes == {SchemaCode.OPS_01, SchemaCode.MKT_01}
    assert SchemaCode.OPS_04 not in codes
    assert len(created) == 2


@pytest.mark.asyncio
async def test_dependent_detector_skipped_when_prerequisite_absent():
    """OPS_03 (dep OPS_01) is not run when OPS_01 returns None."""
    ctx = _make_ctx()
    ops03_calls = {"n": 0}

    async def ops03_detect(c):
        ops03_calls["n"] += 1
        return DetectorResult(schema_code=SchemaCode.OPS_03, severity=Severity.INDICATIVE)

    registry = {
        SchemaCode.OPS_01: _detector(None),  # prerequisite produces no finding
        SchemaCode.OPS_03: ops03_detect,
    }
    dependencies = {SchemaCode.OPS_03: [SchemaCode.OPS_01]}

    created = await run_for_windfarm(ctx, registry=registry, dependencies=dependencies)

    assert created == []
    assert ops03_calls["n"] == 0  # dependency gate prevents the call entirely
    assert ctx.db.added == []


@pytest.mark.asyncio
async def test_triggered_by_id_wired_from_dependency():
    """OPS_03's row links to the OPS_01 row's flushed id via triggered_by_id."""
    ctx = _make_ctx()
    registry = {
        SchemaCode.OPS_01: _detector(
            DetectorResult(schema_code=SchemaCode.OPS_01, severity=Severity.CONFIRMED)
        ),
        SchemaCode.OPS_03: _detector(
            DetectorResult(schema_code=SchemaCode.OPS_03, severity=Severity.INDICATIVE)
        ),
    }
    dependencies = {SchemaCode.OPS_03: [SchemaCode.OPS_01]}

    created = await run_for_windfarm(ctx, registry=registry, dependencies=dependencies)

    assert len(created) == 2
    parent = next(o for o in created if o.schema_code == SchemaCode.OPS_01)
    child = next(o for o in created if o.schema_code == SchemaCode.OPS_03)
    assert parent.id is not None
    assert child.triggered_by_id == parent.id
    assert parent.triggered_by_id is None  # parent has no prerequisite


@pytest.mark.asyncio
async def test_inactive_status_schema_is_skipped():
    """A schema marked INACTIVE produces no row, even if its detector would fire."""
    ctx = _make_ctx()
    mkt05_calls = {"n": 0}

    async def mkt05_detect(c):
        mkt05_calls["n"] += 1
        return DetectorResult(schema_code=SchemaCode.MKT_05, severity=Severity.WATCH)

    registry = {SchemaCode.MKT_05: mkt05_detect}
    status = {SchemaCode.MKT_05: "INACTIVE"}

    created = await run_for_windfarm(ctx, registry=registry, status=status)

    assert created == []
    assert mkt05_calls["n"] == 0  # INACTIVE schema is never invoked
    assert ctx.db.added == []


@pytest.mark.asyncio
async def test_schema_codes_filter_runs_only_listed(monkeypatch):
    """schema_codes whitelist → only listed detectors run; others skipped (#114)."""
    ctx = _make_ctx()
    calls = {"ops01": 0, "mkt01": 0}

    async def ops01_detect(c):
        calls["ops01"] += 1
        return DetectorResult(schema_code=SchemaCode.OPS_01, severity=Severity.WATCH)

    async def mkt01_detect(c):
        calls["mkt01"] += 1
        return DetectorResult(schema_code=SchemaCode.MKT_01, severity=Severity.WATCH)

    registry = {SchemaCode.OPS_01: ops01_detect, SchemaCode.MKT_01: mkt01_detect}

    created = await run_for_windfarm(
        ctx,
        registry=registry,
        dependencies={},
        schema_codes=[SchemaCode.OPS_01],
    )

    assert calls["ops01"] == 1
    assert calls["mkt01"] == 0  # not in whitelist → never invoked
    assert [o.schema_code for o in created] == [SchemaCode.OPS_01]


@pytest.mark.asyncio
async def test_schema_codes_none_runs_all():
    """schema_codes=None (default) is byte-identical to the unfiltered run (#114)."""
    ctx = _make_ctx()
    registry = {
        SchemaCode.OPS_01: _detector(
            DetectorResult(schema_code=SchemaCode.OPS_01, severity=Severity.WATCH)
        ),
        SchemaCode.MKT_01: _detector(
            DetectorResult(schema_code=SchemaCode.MKT_01, severity=Severity.WATCH)
        ),
    }

    created = await run_for_windfarm(ctx, registry=registry, dependencies={}, schema_codes=None)

    assert {o.schema_code for o in created} == {SchemaCode.OPS_01, SchemaCode.MKT_01}
