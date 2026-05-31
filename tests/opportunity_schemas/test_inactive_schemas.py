"""INACTIVE data-blocked schema tests (issue #106) — MKT-05 & MKT-07.

MKT-05 (PPA underpricing — no PPA strike prices in the data model) and MKT-07
(intraday forecast deviation — no forecast data ingested) are genuinely
data-blocked. They are *registered* in ``SCHEMA_REGISTRY`` so they are known to
the engine, but marked ``"INACTIVE"`` in ``SCHEMA_STATUS`` so the orchestrator
(``run_for_windfarm``) skips them wholesale and they emit **no per-windfarm
rows**. Activation is tracked in #116.

All tests are DB-free: the orchestrator persists via a ``FakeSession`` that
records ``add()``ed objects and assigns sequential ids on ``flush()`` — no live
Postgres / JSONB needed. The two detectors' ``detect`` are documented no-ops
that return ``None`` even if invoked directly.
"""

from datetime import datetime

import pytest

from app.models.opportunity import OpportunityStatus, SchemaCode, Severity
from app.services.opportunity_schemas import mkt05_ppa_underpricing, mkt07_forecast_deviation
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult
from app.services.opportunity_schemas.registry import (
    SCHEMA_REGISTRY,
    SCHEMA_STATUS,
    run_for_windfarm,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)


class FakeSession:
    """Minimal AsyncSession stand-in: records ``add()``s, assigns ids on flush."""

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


def test_registry_marks_mkt05_mkt07_inactive():
    """Registry reports both MKT-05 and MKT-07 as INACTIVE (overriding ACTIVE)."""
    assert SCHEMA_STATUS[SchemaCode.MKT_05] == "INACTIVE"
    assert SCHEMA_STATUS[SchemaCode.MKT_07] == "INACTIVE"
    # str-Enum equality: the AC's enum form must hold too.
    assert SCHEMA_STATUS[SchemaCode.MKT_05] == OpportunityStatus.INACTIVE
    assert SCHEMA_STATUS[SchemaCode.MKT_07] == OpportunityStatus.INACTIVE


def test_inactive_schemas_are_registered_but_not_active():
    """Both schemas are *registered* (known to the engine) yet not ACTIVE."""
    assert SchemaCode.MKT_05 in SCHEMA_REGISTRY
    assert SchemaCode.MKT_07 in SCHEMA_REGISTRY
    assert SCHEMA_STATUS[SchemaCode.MKT_05] != "ACTIVE"
    assert SCHEMA_STATUS[SchemaCode.MKT_07] != "ACTIVE"


@pytest.mark.asyncio
async def test_inactive_detectors_are_noops():
    """The data-blocked detectors return ``None`` even if invoked directly."""
    ctx = _make_ctx()
    assert await mkt05_ppa_underpricing.detect(ctx) is None
    assert await mkt07_forecast_deviation.detect(ctx) is None


@pytest.mark.asyncio
async def test_inactive_schemas_produce_no_rows():
    """``run_for_windfarm`` yields no MKT-05/MKT-07 rows when they are registered.

    The registry contains MKT-05 and MKT-07 (registered), but
    ``SCHEMA_STATUS`` marks them INACTIVE, so the orchestrator skips them: no
    detector call, no ``Opportunity`` row, nothing persisted for those codes.
    A spy guards that ``detect`` is never even invoked for an INACTIVE schema.
    """
    ctx = _make_ctx()
    calls = {"mkt05": 0, "mkt07": 0}

    async def mkt05_spy(c):
        calls["mkt05"] += 1
        return DetectorResult(schema_code=SchemaCode.MKT_05, severity=Severity.WATCH)

    async def mkt07_spy(c):
        calls["mkt07"] += 1
        return DetectorResult(schema_code=SchemaCode.MKT_07, severity=Severity.WATCH)

    registry = {SchemaCode.MKT_05: mkt05_spy, SchemaCode.MKT_07: mkt07_spy}
    status = {
        SchemaCode.MKT_05: SCHEMA_STATUS[SchemaCode.MKT_05],
        SchemaCode.MKT_07: SCHEMA_STATUS[SchemaCode.MKT_07],
    }

    created = await run_for_windfarm(ctx, registry=registry, status=status)

    assert created == []
    # No MKT-05/MKT-07 rows produced.
    produced_codes = {o.schema_code for o in created}
    assert SchemaCode.MKT_05 not in produced_codes
    assert SchemaCode.MKT_07 not in produced_codes
    # Nothing persisted at all, and the INACTIVE detectors were never invoked.
    assert ctx.db.added == []
    assert calls == {"mkt05": 0, "mkt07": 0}


@pytest.mark.asyncio
async def test_live_registry_skips_inactive_genuine_noop_detectors():
    """Live ``SCHEMA_REGISTRY`` no-op detectors + live ``SCHEMA_STATUS`` → no rows.

    Drive the orchestrator with the genuine module-level no-op detectors for
    MKT-05/MKT-07 (pulled from the real ``SCHEMA_REGISTRY``) and the genuine live
    ``SCHEMA_STATUS``. Because both are INACTIVE there, ``run_for_windfarm`` skips
    them — proving the live wiring (registered + INACTIVE) produces no
    per-windfarm rows. (The DB-bound ACTIVE detectors aren't exercised here;
    those have their own per-detector tests.)
    """
    ctx = _make_ctx()
    live_registry = {
        SchemaCode.MKT_05: SCHEMA_REGISTRY[SchemaCode.MKT_05],
        SchemaCode.MKT_07: SCHEMA_REGISTRY[SchemaCode.MKT_07],
    }

    created = await run_for_windfarm(ctx, registry=live_registry, status=SCHEMA_STATUS)

    assert created == []
    assert ctx.db.added == []
    # The detectors must not have been reached (INACTIVE skip happens first), so
    # the FakeSession was never flushed.
    assert ctx.db.flush_count == 0
