"""OPS-05 detector tests (issue #100) — grid curtailment, UK only, no proxy.

OPS-05 fires only for UK (GB) windfarms with metered curtailment data. The
percentage comes from ``ctx.load_curtailment_pct()`` (curtailed /
(curtailed + generation) * 100); tiers are >=10 / 5 / 3 →
CONFIRMED / INDICATIVE / WATCH. Non-UK farms never fire (no proxy fallback).

All tests are DB-free: the curtailment percentage is injected via
``DetectionContext(prefetched={"curtailment_pct": ...})`` and the windfarm is a
``SimpleNamespace`` exposing only the ``bidzone`` / ``country`` codes the UK gate
reads.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops05_grid_curtailment import (
    classify_curtailment_severity,
    compute_curtailment_pct,
    detect,
    is_uk_bidzone,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 100


def _wf(*, bidzone_code=None, country_code=None):
    """Fake windfarm exposing just the bidzone / country codes the UK gate reads."""
    bidzone = SimpleNamespace(code=bidzone_code) if bidzone_code is not None else None
    country = SimpleNamespace(code=country_code) if country_code is not None else None
    return SimpleNamespace(id=WF_ID, bidzone=bidzone, country=country)


def _uk_wf():
    return _wf(bidzone_code="10YGB----------A")


def _non_uk_wf():
    return _wf(bidzone_code="10YNO-2--------T", country_code="NOR")


def _ctx(curtailment_pct=None, windfarm=None):
    return DetectionContext(
        db=None,
        windfarm=windfarm if windfarm is not None else _uk_wf(),
        period_start=START,
        period_end=END,
        prefetched={"curtailment_pct": curtailment_pct},
    )


# ─── compute_curtailment_pct (pure) ───────────────────────────────────────────


def test_curtailment_pct_formula():
    """curtailed=100, gen=900 → 100/(100+900)*100 = 10.0%."""
    assert compute_curtailment_pct(100, 900) == 10.0


def test_curtailment_pct_none_when_denominator_zero():
    """Zero curtailed + zero generation → None (no meaningful percentage)."""
    assert compute_curtailment_pct(0, 0) is None


def test_curtailment_pct_none_when_input_missing():
    """A missing input → None."""
    assert compute_curtailment_pct(None, 900) is None
    assert compute_curtailment_pct(100, None) is None


# ─── classify_curtailment_severity (pure) ─────────────────────────────────────


def test_severity_boundaries():
    """10.0→CONFIRMED, 9.99→INDICATIVE, 5.0→INDICATIVE, 4.99→WATCH, 3.0→WATCH, 2.99→None."""
    assert classify_curtailment_severity(10.0) == Severity.CONFIRMED
    assert classify_curtailment_severity(9.99) == Severity.INDICATIVE
    assert classify_curtailment_severity(5.0) == Severity.INDICATIVE
    assert classify_curtailment_severity(4.99) == Severity.WATCH
    assert classify_curtailment_severity(3.0) == Severity.WATCH
    assert classify_curtailment_severity(2.99) is None


def test_severity_none_when_pct_missing():
    assert classify_curtailment_severity(None) is None


# ─── is_uk_bidzone (pure) ─────────────────────────────────────────────────────


def test_is_uk_bidzone():
    assert is_uk_bidzone(_wf(bidzone_code="10YGB----------A")) is True
    assert is_uk_bidzone(_wf(country_code="GBR")) is True
    assert is_uk_bidzone(_wf(country_code="GB")) is True
    assert is_uk_bidzone(_non_uk_wf()) is False
    assert is_uk_bidzone(SimpleNamespace(id=1)) is False  # no bidzone/country attrs
    assert is_uk_bidzone(WF_ID) is False  # bare int


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_non_uk_farm_does_not_fire():
    """A non-UK (ENTSOE) bidzone → None even with 10% curtailment injected (no proxy)."""
    ctx = _ctx(curtailment_pct=10.0, windfarm=_non_uk_wf())
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_uk_farm_fires():
    """A UK farm with 10% curtailment → CONFIRMED DetectorResult."""
    ctx = _ctx(curtailment_pct=10.0, windfarm=_uk_wf())
    result = await detect(ctx)
    assert result is not None
    assert result.schema_code is SchemaCode.OPS_05
    assert result.severity is Severity.CONFIRMED
    assert result.data_slots["curtailment_pct"] == 10.0


@pytest.mark.asyncio
async def test_uk_farm_via_country_code_fires_watch():
    """A UK farm tagged only by country (GBR) with 3% curtailment → WATCH."""
    ctx = _ctx(curtailment_pct=3.0, windfarm=_wf(country_code="GBR"))
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.WATCH


@pytest.mark.asyncio
async def test_uk_farm_no_curtailment_data_does_not_fire():
    """A UK farm with no reachable curtailment data (None) → None."""
    ctx = _ctx(curtailment_pct=None, windfarm=_uk_wf())
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_uk_farm_sub_threshold_does_not_fire():
    """A UK farm below the 3% WATCH floor → None."""
    ctx = _ctx(curtailment_pct=2.99, windfarm=_uk_wf())
    assert await detect(ctx) is None
