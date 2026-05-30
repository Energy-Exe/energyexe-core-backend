"""MKT-06 detector tests (issue #105) — negative-price-hours exposure.

MKT-06 counts hours the farm GENERATES at a negative day-ahead price and
classifies on a per-year rate:

    >= 400 → CONFIRMED   >= 250 → INDICATIVE   >= 150 → WATCH   else → None

Boundaries (locked): 400 → CONFIRMED, 399 → INDICATIVE, 250 → INDICATIVE,
150 → WATCH, 149 → None.

All tests are DB-free: the negative-price-hours count is injected via
``DetectionContext(prefetched={"negative_price_hours": N})``, which
short-circuits the price-analytics query entirely.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.mkt06_negative_price_hours import (
    annualise_hours,
    classify_negative_price_severity,
    detect,
)

WF_ID = 106


def _ctx(negative_hours=None, *, start=datetime(2025, 1, 1), end=datetime(2026, 1, 1)):
    """A DB-free DetectionContext with ``negative_price_hours`` injected.

    Window defaults to exactly one calendar year (365 days), so the injected
    count is annualised by ≈1.0007 — i.e. the boundary integers below classify
    on essentially their own value.
    """
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=start,
        period_end=end,
        prefetched={"negative_price_hours": negative_hours},
    )


# ─── classify_negative_price_severity — the required boundary table ───────────


def test_severity_boundaries():
    """400 → CONFIRMED, 399 → INDICATIVE, 250 → INDICATIVE, 150 → WATCH, 149 → None."""
    assert classify_negative_price_severity(400) == Severity.CONFIRMED
    assert classify_negative_price_severity(399) == Severity.INDICATIVE
    assert classify_negative_price_severity(250) == Severity.INDICATIVE
    assert classify_negative_price_severity(150) == Severity.WATCH
    assert classify_negative_price_severity(149) is None


def test_severity_none_input_is_none():
    """A ``None`` hours-per-year (no data) → no finding."""
    assert classify_negative_price_severity(None) is None


# ─── annualise_hours — the per-year normalization rule ────────────────────────


def test_annualise_two_year_window_halves():
    """A 2-year (730.5-day) window halves the raw count to an annual average."""
    assert annualise_hours(800, 730.5) == pytest.approx(400.0)


def test_annualise_half_year_window_doubles():
    """A half-year window doubles the raw count to a per-year rate."""
    assert annualise_hours(100, 365.25 / 2) == pytest.approx(200.0)


def test_annualise_missing_window_uses_raw_count():
    """A missing / non-positive window length falls back to the raw count."""
    assert annualise_hours(300, None) == 300.0
    assert annualise_hours(300, 0) == 300.0


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_fires_on_negative_hours():
    """400 negative-price hours over a ~1-year window → CONFIRMED."""
    result = await detect(_ctx(400))
    assert result is not None
    assert result.schema_code == SchemaCode.MKT_06
    assert result.severity == Severity.CONFIRMED
    assert result.data_slots["negative_price_hours"] == 400


@pytest.mark.asyncio
async def test_detect_none_when_no_negative_hours():
    """Zero negative-price hours → no finding."""
    assert await detect(_ctx(0)) is None


@pytest.mark.asyncio
async def test_detect_none_when_accessor_returns_none():
    """A ``None`` count (no data reachable) → no finding (None/0-safe)."""
    assert await detect(_ctx(None)) is None


@pytest.mark.asyncio
async def test_detect_none_below_watch_floor():
    """149 negative-price hours/year is below the WATCH floor → no finding."""
    assert await detect(_ctx(149)) is None


@pytest.mark.asyncio
async def test_detect_annualises_multi_year_window():
    """1600 raw hours over a ~2-year window annualises to ~800/yr → CONFIRMED.

    Proves the per-year normalization is applied inside ``detect``: the raw count
    slot keeps 1600, but the per-year slot is roughly halved (≈800), and severity
    classifies on the annualised rate. (A 2024→2026 window is 731 days — 2024 is
    a leap year — so 1600 * 365.25/731 ≈ 799.5.)
    """
    result = await detect(_ctx(1600, start=datetime(2024, 1, 1), end=datetime(2026, 1, 1)))
    assert result is not None
    assert result.severity == Severity.CONFIRMED
    assert result.data_slots["negative_price_hours"] == 1600
    assert result.data_slots["negative_price_hours_per_year"] == pytest.approx(800.0, abs=1.0)


@pytest.mark.asyncio
async def test_detect_annualisation_can_lower_tier():
    """800 raw hours over a ~2-year window annualises to ~400/yr → just under
    CONFIRMED (INDICATIVE), proving the tier follows the ANNUALISED rate, not the
    raw count (raw 800 alone would be CONFIRMED)."""
    result = await detect(_ctx(800, start=datetime(2024, 1, 1), end=datetime(2026, 1, 1)))
    assert result is not None
    # 800 * 365.25/731 ≈ 399.7 → below the 400 CONFIRMED floor.
    assert result.severity == Severity.INDICATIVE
    assert result.data_slots["negative_price_hours"] == 800


@pytest.mark.asyncio
async def test_detect_short_window_promotes_via_annualisation():
    """100 hours over a half-year window annualises to ~200/yr → INDICATIVE.

    A raw 100 would be below the WATCH floor; annualisation lifts it past the
    INDICATIVE (>=250? no — 200) ... it lands in WATCH (>=150). This locks that
    the detector classifies on the ANNUALISED rate, not the raw count.
    """
    result = await detect(_ctx(100, start=datetime(2025, 1, 1), end=datetime(2025, 7, 2)))
    assert result is not None
    # ~182.6-day window → 100 * 365.25/182.6 ≈ 200/yr → WATCH band.
    assert result.severity == Severity.WATCH
