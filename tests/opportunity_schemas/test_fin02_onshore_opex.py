"""FIN-02 detector tests (issue #108) — onshore OPEX overrun.

FIN-02 compares an **onshore** windfarm's OPEX-per-MWh against a dynamically
computed per-bidzone, per-location-type peer median. It fires only for onshore
farms (gates on ``location_type``), uses ``primary_asset`` 1:1 links only (v1
skips consolidated entities), and CONFIRMED requires two full operating years.

All tests are DB-free: the subject's own OPEX financials and the cohort median
are injected via ``DetectionContext(prefetched={...})`` using the accessor cache
keys ``"own_opex_financials"`` and ``"zone_opex_median:<location_type>"``. The
windfarm is a ``SimpleNamespace`` exposing only ``location_type`` / ``id``.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.fin02_onshore_opex import (
    classify_opex_overrun_severity,
    compute_opex_per_mwh,
    detect,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 200


def _wf(*, location_type="onshore"):
    """Fake windfarm exposing just the location_type the cohort gate reads."""
    return SimpleNamespace(id=WF_ID, location_type=location_type)


def _ctx(*, location_type="onshore", financials=None, onshore_median=None, offshore_median=None):
    """DB-free context. Inject own financials + per-cohort medians by cache key."""
    prefetched = {"own_opex_financials": financials}
    if onshore_median is not None:
        prefetched["zone_opex_median:onshore"] = onshore_median
    if offshore_median is not None:
        prefetched["zone_opex_median:offshore"] = offshore_median
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


# ─── compute_opex_per_mwh (pure) ──────────────────────────────────────────────


def test_opex_per_mwh_formula():
    """3.18M€ / 100 GWh → 31.8 €/MWh (generation supplied in MWh)."""
    # 100 GWh == 100_000 MWh.
    assert compute_opex_per_mwh(3.18e6, 100_000) == pytest.approx(31.8)


def test_opex_per_mwh_none_when_inputs_missing():
    assert compute_opex_per_mwh(None, 100_000) is None
    assert compute_opex_per_mwh(3.18e6, None) is None
    assert compute_opex_per_mwh(3.18e6, 0) is None


# ─── classify_opex_overrun_severity (pure) ────────────────────────────────────


def test_pct_over_median_severity():
    """+100→CONFIRMED(2yr), +70→INDICATIVE, +30→WATCH, +29→None."""
    assert classify_opex_overrun_severity(100.0, 2) == Severity.CONFIRMED
    assert classify_opex_overrun_severity(70.0, 2) == Severity.INDICATIVE
    assert classify_opex_overrun_severity(30.0, 2) == Severity.WATCH
    assert classify_opex_overrun_severity(29.0, 2) is None


def test_confirmed_requires_two_full_years_else_indicative():
    """A +100% overrun with only one full year caps at INDICATIVE."""
    assert classify_opex_overrun_severity(120.0, 1) == Severity.INDICATIVE
    assert classify_opex_overrun_severity(120.0, 2) == Severity.CONFIRMED


def test_severity_none_when_pct_missing():
    assert classify_opex_overrun_severity(None, 2) is None


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_none_when_no_financials():
    """No primary_asset OPEX data → None (snapshot-safety path)."""
    ctx = _ctx(financials=None, onshore_median=30.0)
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_detect_watch_overrun():
    """Own 39 €/MWh vs median 30 → +30% → WATCH."""
    # opex 39M€ / 1000 GWh = 39 €/MWh; +30% over a 30 median.
    ctx = _ctx(
        financials=_financials(opex_eur=39e6, gen_gwh=1000, full_years=2),
        onshore_median=30.0,
    )
    result = await detect(ctx)
    assert result is not None
    assert result.schema_code is SchemaCode.FIN_02
    assert result.severity is Severity.WATCH
    assert result.data_slots["opex_per_mwh"] == pytest.approx(39.0)
    assert result.data_slots["pct_over_median"] == pytest.approx(30.0)


@pytest.mark.asyncio
async def test_detect_confirmed_overrun_two_years():
    """Own 60 €/MWh vs median 30 → +100% with 2 full years → CONFIRMED."""
    ctx = _ctx(
        financials=_financials(opex_eur=60e6, gen_gwh=1000, full_years=2),
        onshore_median=30.0,
    )
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.CONFIRMED


@pytest.mark.asyncio
async def test_onshore_offshore_cohorts_separated():
    """An offshore farm is NOT benchmarked by FIN-02 (onshore-only).

    Even though a huge onshore median would make the offshore farm look fine /
    fire differently, FIN-02 gates on location_type and does not fire for an
    offshore farm at all.
    """
    ctx = _ctx(
        location_type="offshore",
        financials=_financials(opex_eur=60e6, gen_gwh=1000, full_years=2),
        onshore_median=30.0,
        offshore_median=30.0,
    )
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_consolidated_entity_excluded_v1():
    """A consolidated (non-primary_asset) link surfaces no own financials → None.

    The context accessor filters to relationship_type='primary_asset', so a
    consolidated-only entity yields ``None`` financials and FIN-02 does not fire.
    """
    ctx = _ctx(financials=None, onshore_median=30.0)
    assert await detect(ctx) is None


# ─── 2026-08 fix: metered denominator, EUR cohort, nightly bare-int gate ──────


@pytest.mark.asyncio
async def test_data_slots_carry_provenance():
    """Currency, filing-currency ratio, years pooled, basis and cohort size ride along."""
    financials = _financials(opex_eur=39e6, gen_gwh=1000, full_years=3)
    financials.update(
        {
            "currency": "EUR",
            "native_currency": "NOK",
            "native_opex_per_mwh": 440.0,
            "years_used": [2023, 2024, 2025],
            "generation_source": "metered",
        }
    )
    ctx = _ctx(financials=financials, onshore_median=30.0)
    ctx._cache["zone_opex_peer_count:onshore"] = 7
    result = await detect(ctx)
    assert result is not None
    slots = result.data_slots
    assert slots["currency"] == "EUR"
    assert slots["native_currency"] == "NOK"
    assert slots["native_opex_per_mwh"] == 440.0
    assert slots["years_used"] == [2023, 2024, 2025]
    assert slots["generation_source"] == "metered"
    assert slots["peer_count"] == 7


@pytest.mark.asyncio
async def test_legacy_financials_dict_still_works_without_provenance():
    ctx = _ctx(
        financials=_financials(opex_eur=39e6, gen_gwh=1000, full_years=2), onshore_median=30.0
    )
    result = await detect(ctx)
    assert result is not None
    assert result.data_slots["currency"] is None
    assert result.data_slots["peer_count"] is None


@pytest.mark.asyncio
async def test_lutelandet_corrected_ratio_does_not_fire():
    """Prod report 41: 3 pooled NOK filings ≈ 6.1M€ over 380.8 GWh = 16 €/MWh vs a
    13.4 €/MWh cohort median → +19% → below the 30% WATCH floor → no finding."""
    ctx = _ctx(
        financials=_financials(opex_eur=6.11e6, gen_gwh=380.76, full_years=3),
        onshore_median=13.4,
    )
    assert await detect(ctx) is None


def _db_with_meta(row):
    from unittest.mock import AsyncMock, MagicMock

    db = MagicMock()
    res = MagicMock()
    res.first.return_value = row
    db.execute = AsyncMock(return_value=res)
    return db


@pytest.mark.asyncio
async def test_bare_int_windfarm_fires_when_location_resolves_from_db():
    """The nightly passes ``windfarm=<int>``; the gate now resolves location_type via the DB."""
    ctx = DetectionContext(
        db=_db_with_meta(("onshore", None, 69)),
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={
            "own_opex_financials": _financials(opex_eur=39e6, gen_gwh=1000, full_years=2),
            "zone_opex_median:onshore": 30.0,
        },
    )
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.WATCH
    assert result.data_slots["location_type"] == "onshore"


@pytest.mark.asyncio
async def test_bare_int_windfarm_none_when_meta_empty():
    """Characterization-harness contract: an empty lookup keeps the gate closed."""
    ctx = DetectionContext(
        db=_db_with_meta(None),
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={
            "own_opex_financials": _financials(opex_eur=39e6, gen_gwh=1000, full_years=2),
            "zone_opex_median:onshore": 30.0,
        },
    )
    assert await detect(ctx) is None
