"""FIN-03 detector tests (issue #108) — offshore OPEX overrun.

FIN-03 is the offshore twin of FIN-02: identical OPEX-per-MWh formula and
severity thresholds (imported from the FIN-02 module), but it fires only for
**offshore** farms and benchmarks against the per-bidzone offshore peer median.
CONFIRMED requires two full operating years — the first commissioning year is a
ramp-up artefact and is excluded from the full-year count by the accessor.

All tests are DB-free via ``DetectionContext(prefetched={...})``.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.fin03_offshore_opex import (
    classify_opex_overrun_severity,
    compute_opex_per_mwh,
    detect,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 300


def _wf(*, location_type="offshore"):
    return SimpleNamespace(id=WF_ID, location_type=location_type)


def _ctx(*, location_type="offshore", financials=None, offshore_median=None, onshore_median=None):
    prefetched = {"own_opex_financials": financials}
    if offshore_median is not None:
        prefetched["zone_opex_median:offshore"] = offshore_median
    if onshore_median is not None:
        prefetched["zone_opex_median:onshore"] = onshore_median
    return DetectionContext(
        db=None,
        windfarm=_wf(location_type=location_type),
        period_start=START,
        period_end=END,
        prefetched=prefetched,
    )


def _financials(*, opex_eur, gen_gwh, full_years):
    return {
        "total_opex_eur": opex_eur,
        "generation_gwh": gen_gwh,
        "full_years": full_years,
        "relationship_type": "primary_asset",
    }


# ─── Shared pure helpers re-exported through the FIN-03 module ─────────────────


def test_opex_per_mwh_formula():
    """Shared helper: 3.18M€ / 100 GWh → 31.8 €/MWh (generation in MWh)."""
    assert compute_opex_per_mwh(3.18e6, 100_000) == pytest.approx(31.8)


def test_pct_over_median_severity():
    """+100→CONFIRMED(2yr), +70→INDICATIVE, +30→WATCH, +29→None."""
    assert classify_opex_overrun_severity(100.0, 2) == Severity.CONFIRMED
    assert classify_opex_overrun_severity(70.0, 2) == Severity.INDICATIVE
    assert classify_opex_overrun_severity(30.0, 2) == Severity.WATCH
    assert classify_opex_overrun_severity(29.0, 2) is None


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_confirmed_offshore_two_years():
    """Offshore farm +100% over the offshore median with 2 full years → CONFIRMED."""
    ctx = _ctx(
        financials=_financials(opex_eur=120e6, gen_gwh=1000, full_years=2),
        offshore_median=60.0,
    )
    result = await detect(ctx)
    assert result is not None
    assert result.schema_code is SchemaCode.FIN_03
    assert result.severity is Severity.CONFIRMED
    assert result.data_slots["location_type"] == "offshore"


@pytest.mark.asyncio
async def test_fin03_requires_two_full_years():
    """First-year-only data → +100% overrun caps at INDICATIVE, not CONFIRMED.

    The commissioning ramp-up year is excluded by the accessor, so ``full_years``
    is 1 → no CONFIRMED.
    """
    ctx = _ctx(
        financials=_financials(opex_eur=120e6, gen_gwh=1000, full_years=1),
        offshore_median=60.0,
    )
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.INDICATIVE  # NOT CONFIRMED
    assert result.severity is not Severity.CONFIRMED


@pytest.mark.asyncio
async def test_onshore_offshore_cohorts_separated():
    """An onshore farm is NOT benchmarked by FIN-03 (offshore-only gate)."""
    ctx = _ctx(
        location_type="onshore",
        financials=_financials(opex_eur=120e6, gen_gwh=1000, full_years=2),
        offshore_median=60.0,
        onshore_median=60.0,
    )
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_consolidated_entity_excluded_v1():
    """A consolidated link → no own financials → FIN-03 does not fire."""
    ctx = _ctx(financials=None, offshore_median=60.0)
    assert await detect(ctx) is None
