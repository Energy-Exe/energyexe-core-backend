"""OPS-07 detector tests (issue #102) — fleet-age / end-of-life risk.

OPS-07 compares each turbine's age against the 25-year design life. Age is
computed against a DYNAMIC ``as_of_date`` (``as_of_year - start_year``); the
"final 5 years" window is age >= 20 and a turbine past design life is age >= 25.

Severity tiers (inclusive ``>=`` on the fraction of turbines in their final 5 yrs;
``any_past_design_life`` always wins → CONFIRMED):
    any past 25-yr life → CONFIRMED   0.75 → CONFIRMED   0.50 → INDICATIVE
    0.30 → WATCH        0.29 → None

The 20-year boundary derives entirely from ``as_of_date.year`` — there is NO
hardcoded calendar year (``test_dynamic_20yr_boundary_uses_as_of_year``).

All tests are DB-free: turbine start dates are injected via
``DetectionContext(prefetched={"turbine_start_dates": [...]})``.
"""

from datetime import date, datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops07_fleet_age_risk import (
    classify_fleet_age_severity,
    compute_fleet_age_metrics,
    detect,
)

WF_ID = 102


def _ctx(start_dates=None, *, period_end=datetime(2026, 1, 1)):
    """A DB-free DetectionContext with turbine start dates injected.

    ``period_end`` drives the dynamic ``as_of_date`` (the detector reads
    ``ctx.period_end``); the default 2026-01-01 means ``as_of_year == 2026``.
    """
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=datetime(2024, 1, 1),
        period_end=period_end,
        prefetched={"turbine_start_dates": start_dates},
    )


def _starts(*years):
    """Turbine start dates from a list of commissioning years (Jan 1 each)."""
    return [date(y, 1, 1) for y in years]


# ─── compute_fleet_age_metrics (pure) ─────────────────────────────────────────


def test_metrics_empty_fleet_is_zero():
    """An empty fleet → (0.0 pct, no turbine past design life)."""
    pct, past = compute_fleet_age_metrics([], date(2026, 1, 1))
    assert pct == 0.0
    assert past is False


def test_metrics_age_uses_year_difference():
    """Age = as_of_year - start_year; >= 20 counts in-window, >= 25 is past life."""
    # ages: 2026-2000=26 (past+in window), 2026-2010=16 (neither), 2026-2006=20 (in window)
    pct, past = compute_fleet_age_metrics(_starts(2000, 2010, 2006), date(2026, 1, 1))
    assert past is True  # the 2000 turbine is age 26
    assert pct == pytest.approx(2 / 3)  # 2000 (26) and 2006 (20) are in the final-5yr window


# ─── classify_fleet_age_severity (pure) ───────────────────────────────────────


def test_turbine_past_25yr_is_confirmed():
    """A turbine past the 25-yr design life (start 2000, as_of 2026 → age 26)
    → CONFIRMED, regardless of the in-final-5yr fraction."""
    pct, past = compute_fleet_age_metrics(_starts(2000), date(2026, 1, 1))
    assert past is True
    assert classify_fleet_age_severity(pct, past) == Severity.CONFIRMED
    # any_past_design_life wins even when the pct fraction alone would be sub-threshold.
    assert classify_fleet_age_severity(0.0, True) == Severity.CONFIRMED


def test_pct_thresholds():
    """75%/50%/30% of turbines in final-5yr → CONFIRMED/INDICATIVE/WATCH; 29% → None
    (no turbine past design life in any of these)."""
    assert classify_fleet_age_severity(0.75, False) == Severity.CONFIRMED
    assert classify_fleet_age_severity(0.50, False) == Severity.INDICATIVE
    assert classify_fleet_age_severity(0.30, False) == Severity.WATCH
    assert classify_fleet_age_severity(0.29, False) is None


def test_dynamic_20yr_boundary_uses_as_of_year():
    """The 20-yr boundary derives from as_of_year (no hardcoded year).

    With as_of 2026, a turbine commissioned in 2006 (== as_of_year - 20) is age 20
    → INCLUDED in the final-5yr window; one commissioned in 2007 (== as_of_year -
    19) is age 19 → EXCLUDED. Proven again at a DIFFERENT as_of year (2030) so the
    boundary is genuinely dynamic.
    """
    # as_of 2026: 2006 included (age 20), 2007 excluded (age 19).
    pct_2026, _ = compute_fleet_age_metrics(_starts(2006), date(2026, 1, 1))
    assert pct_2026 == 1.0  # the single turbine IS in the final-5yr window
    pct_2026_excl, _ = compute_fleet_age_metrics(_starts(2007), date(2026, 1, 1))
    assert pct_2026_excl == 0.0  # age 19 → excluded

    # Shift the as_of year: now 2010 is the boundary, 2011 is excluded.
    pct_2030, _ = compute_fleet_age_metrics(_starts(2010), date(2030, 1, 1))
    assert pct_2030 == 1.0  # 2030 - 2010 = 20 → included
    pct_2030_excl, _ = compute_fleet_age_metrics(_starts(2011), date(2030, 1, 1))
    assert pct_2030_excl == 0.0  # 2030 - 2011 = 19 → excluded


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_confirmed_when_turbine_past_design_life():
    """A fleet with a turbine past 25 yrs (as_of 2026, start 2000) → CONFIRMED."""
    result = await detect(_ctx(_starts(2000, 2010)))
    assert result is not None
    assert result.schema_code is SchemaCode.OPS_07
    assert result.severity is Severity.CONFIRMED
    assert result.data_slots["any_past_design_life"] is True
    assert result.data_slots["as_of_year"] == 2026
    assert result.data_slots["turbine_count"] == 2


@pytest.mark.asyncio
async def test_detect_watch_at_30pct_window():
    """30% of turbines in the final-5yr window (none past design life) → WATCH."""
    # 3 of 10 turbines aged 20 (start 2006, as_of 2026); the rest young.
    start_dates = _starts(2006, 2006, 2006) + _starts(*([2018] * 7))
    result = await detect(_ctx(start_dates))
    assert result is not None
    assert result.severity is Severity.WATCH
    assert result.data_slots["any_past_design_life"] is False
    assert result.data_slots["pct_in_final_5yr"] == pytest.approx(0.30)


@pytest.mark.asyncio
async def test_detect_none_when_below_threshold():
    """A young fleet (no turbine in the final-5yr window) → no finding."""
    assert await detect(_ctx(_starts(2018, 2019, 2020))) is None


@pytest.mark.asyncio
async def test_detect_none_when_no_turbine_data():
    """No turbine start-date data injected → detect returns None."""
    assert await detect(_ctx(None)) is None
    assert await detect(_ctx([])) is None


@pytest.mark.asyncio
async def test_detect_uses_dynamic_as_of_from_period_end():
    """A later detection period end re-classifies the same fleet (no hardcoded year).

    A fleet of turbines commissioned in 2005 is age 19 at as_of 2024 (None) but
    age 21 at as_of 2026 (in the final-5yr window → WATCH/up).
    """
    young = await detect(_ctx(_starts(2005), period_end=datetime(2024, 1, 1)))
    assert young is None  # 2024 - 2005 = 19 → not yet in the window
    older = await detect(_ctx(_starts(2005), period_end=datetime(2026, 1, 1)))
    assert older is not None  # 2026 - 2005 = 21 → in the window
    assert older.data_slots["as_of_year"] == 2026
