"""FIN-01 detector tests (issue #107) — P50 generation attainment.

FIN-01 compares actual annual generation against a *sourced* P50 target
(``p50_targets.p50_target_volume_gwh``). It NEVER substitutes an internal
estimate; a farm with actual generation but no sourced target surfaces a *blank
finding* flagging the gap. A single below-target year caps at WATCH; escalation
to INDICATIVE / CONFIRMED needs two consecutive years.

All tests are DB-free: the actual annual generation (``{year: gwh}``) and the
sourced target are injected via
``DetectionContext(prefetched={"annual_generation_gwh": ..., "p50_target": ...})``
and the windfarm is a ``SimpleNamespace`` exposing only
``commercial_operational_date`` (the COD-exclusion input).
"""

from datetime import date, datetime
from types import SimpleNamespace

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.fin01_p50_attainment import (
    classify_attainment_severity,
    compute_attainment_pct,
    detect,
    is_cod_year_excluded,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 100


def _wf(*, cod=None):
    """Fake windfarm exposing just the COD the exclusion rule reads."""
    return SimpleNamespace(id=WF_ID, commercial_operational_date=cod)


def _ctx(*, annual=None, target=None, cod=None):
    return DetectionContext(
        db=None,
        windfarm=_wf(cod=cod),
        period_start=START,
        period_end=END,
        prefetched={"annual_generation_gwh": annual, "p50_target": target},
    )


# ─── compute_attainment_pct (pure) ────────────────────────────────────────────


def test_attainment_formula():
    """95 GWh / 100 GWh → 95.0%."""
    assert compute_attainment_pct(95, 100) == 95.0


def test_attainment_none_when_target_missing_or_zero():
    """No sourced target (None / 0) → None (no meaningful ratio)."""
    assert compute_attainment_pct(95, None) is None
    assert compute_attainment_pct(95, 0) is None


def test_attainment_none_when_actual_missing():
    assert compute_attainment_pct(None, 100) is None


# ─── classify_attainment_severity (pure) ──────────────────────────────────────


def test_single_year_caps_at_watch():
    """80% one year, no prior → WATCH (a single below-target year always caps)."""
    assert classify_attainment_severity(80.0, None) == Severity.WATCH


def test_single_year_at_or_above_target_is_none():
    """At / above P50 with no prior → None (healthy)."""
    assert classify_attainment_severity(95.0, None) is None
    assert classify_attainment_severity(102.0, None) is None


def test_confirmed_requires_two_consecutive_below_85():
    """(82, prior 84) → CONFIRMED; (82, prior 91) → INDICATIVE."""
    assert classify_attainment_severity(82.0, 84.0) == Severity.CONFIRMED
    assert classify_attainment_severity(82.0, 91.0) == Severity.INDICATIVE


def test_two_consecutive_below_90_is_indicative():
    """Both years below 90 (but not both below 85) → INDICATIVE."""
    assert classify_attainment_severity(88.0, 89.0) == Severity.INDICATIVE


def test_below_target_two_years_not_escalated_is_watch():
    """Below 95 this year, prior healthy (>=90) → WATCH (not escalated)."""
    assert classify_attainment_severity(92.0, 96.0) == Severity.WATCH


def test_two_years_at_or_above_target_is_none():
    assert classify_attainment_severity(96.0, 97.0) is None


def test_severity_none_when_attainment_missing():
    assert classify_attainment_severity(None, 90.0) is None


# ─── is_cod_year_excluded (pure) ──────────────────────────────────────────────


def test_cod_after_may_excludes_first_year():
    """COD month=7 → the commissioning calendar year is the excluded partial year."""
    assert is_cod_year_excluded(date(2023, 7, 15), 2023) is True


def test_cod_before_june_keeps_first_year():
    """COD month=3 → operational most of the year → not excluded."""
    assert is_cod_year_excluded(date(2023, 3, 1), 2023) is False


def test_cod_only_excludes_commissioning_year():
    """A later, full year is never excluded even with a late COD."""
    assert is_cod_year_excluded(date(2023, 7, 15), 2024) is False


def test_cod_unknown_excludes_nothing():
    assert is_cod_year_excluded(None, 2023) is False


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_none_when_no_generation():
    """No actual generation → None (snapshot-safety path: legacy scenarios)."""
    ctx = _ctx(annual=None, target=100.0)
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_no_sourced_target_returns_none():
    """Generation present but no sourced P50 target → None (no finding).

    Previously this emitted a blank WATCH placeholder, which flooded the board
    on low-P50-coverage fleets; attainment can't be assessed without a target,
    so it is no longer surfaced as an opportunity.
    """
    ctx = _ctx(annual={2025: 95.0}, target=None)
    assert await detect(ctx) is None
    # target == 0 is treated the same (no usable target).
    ctx0 = _ctx(annual={2025: 95.0}, target=0)
    assert await detect(ctx0) is None


@pytest.mark.asyncio
async def test_detect_single_year_watch():
    """One full year at 80% attainment → WATCH."""
    ctx = _ctx(annual={2025: 80.0}, target=100.0)
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.WATCH
    assert result.data_slots["attainment_pct"] == 80.0


@pytest.mark.asyncio
async def test_detect_two_consecutive_below_85_confirmed():
    """Two consecutive years below 85% → CONFIRMED."""
    ctx = _ctx(annual={2024: 84.0, 2025: 82.0}, target=100.0)
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.CONFIRMED
    assert result.data_slots["attainment_pct"] == 82.0
    assert result.data_slots["prior_attainment_pct"] == 84.0


@pytest.mark.asyncio
async def test_detect_at_target_no_finding():
    """A single year exactly at P50 → no finding (None)."""
    ctx = _ctx(annual={2025: 100.0}, target=100.0)
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_detect_cod_partial_year_excluded():
    """The late-COD commissioning year is dropped, leaving a single full year →
    classified as a single-year WATCH (not a two-consecutive CONFIRMED)."""
    # 2024 is the COD partial year (COD 2024-07) and is excluded; only 2025 (82%)
    # remains → single year → WATCH (would be CONFIRMED if 2024's 80% counted).
    ctx = _ctx(
        annual={2024: 80.0, 2025: 82.0},
        target=100.0,
        cod=date(2024, 7, 1),
    )
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.WATCH
    assert result.data_slots["attainment_year"] == 2025
    assert result.data_slots["prior_attainment_pct"] is None
