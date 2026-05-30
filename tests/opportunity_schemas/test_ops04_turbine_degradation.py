"""OPS-04 detector tests (issue #99) — turbine degradation, capped at INDICATIVE.

OPS-04 reads the Module 5 degradation OLS result (``degradation_results``) via
``ctx.load_degradation_result()`` and flags a significantly negative
``slope_pct_per_year``. Because ``baseline_cap_pu`` is a hardcoded placeholder,
severity is **capped at INDICATIVE**: a CONFIRMED-eligible slope is emitted as
INDICATIVE with ``data_slots["baseline_caveat"] = True``.

Tiers (strict ``<`` on slope and p-value):
    (-3.5, 0.05) CONFIRMED-eligible → INDICATIVE (cap)
    (-2.0, 0.05) → INDICATIVE
    (-1.0, 0.10) → WATCH
    else         → None

Guards (→ detect None): floating foundation, ``abs(slope) > 20`` artefact,
< 3 years of regression history.

All tests are DB-free: the degradation row is injected via
``DetectionContext(prefetched={"degradation_result": {...}})`` and the windfarm is
a ``SimpleNamespace`` for the foundation / years-of-data checks.
"""

from datetime import date, datetime
from types import SimpleNamespace

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops04_turbine_degradation import (
    classify_degradation_severity,
    detect,
    is_degradation_modelling_artifact,
    is_floating_foundation,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 101

# A 5-year regression window (>= the 3-year minimum) used by default.
DEFAULT_ANALYSIS_START = date(2019, 1, 1)
DEFAULT_ANALYSIS_END = date(2024, 1, 1)


def _wf(foundation_type="fixed"):
    """Fake windfarm ORM object exposing just the fields OPS-04 reads."""
    return SimpleNamespace(id=WF_ID, foundation_type=foundation_type)


def _degradation(
    slope_pct,
    p_value,
    *,
    analysis_start=DEFAULT_ANALYSIS_START,
    analysis_end=DEFAULT_ANALYSIS_END,
):
    return {
        "slope_pct_per_year": slope_pct,
        "p_value": p_value,
        "r_squared": 0.8,
        "ci_lower_95_pct": None,
        "ci_upper_95_pct": None,
        "n_constraint_hours_excluded": 12,
        "baseline_cap_pu": 0.35,
        "reference_curve": "q50",
        "analysis_start": analysis_start,
        "analysis_end": analysis_end,
        "data_points": 60,
    }


def _ctx(degradation=None, windfarm=None):
    return DetectionContext(
        db=None,
        windfarm=windfarm if windfarm is not None else _wf(),
        period_start=START,
        period_end=END,
        prefetched={"degradation_result": degradation},
    )


# ─── classify_degradation_severity (pure) ─────────────────────────────────────


def test_severity_confirmed_capped_to_indicative():
    """A CONFIRMED-eligible slope (-4.0, p=0.04) is capped to INDICATIVE."""
    assert classify_degradation_severity(-4.0, 0.04) == Severity.INDICATIVE


def test_indicative_boundary():
    """(-2.01, 0.04)→INDICATIVE; (-1.01, 0.06)→WATCH; (-1.01, 0.11)→None."""
    assert classify_degradation_severity(-2.01, 0.04) == Severity.INDICATIVE
    assert classify_degradation_severity(-1.01, 0.06) == Severity.WATCH
    assert classify_degradation_severity(-1.01, 0.11) is None


def test_watch_requires_p_below_010():
    """WATCH tier requires p < 0.10 (strict)."""
    assert classify_degradation_severity(-1.01, 0.06) == Severity.WATCH
    assert classify_degradation_severity(-1.01, 0.10) is None  # p not < 0.10
    assert classify_degradation_severity(-1.01, 0.11) is None


def test_severity_none_when_inputs_missing():
    """Missing slope or p-value → None."""
    assert classify_degradation_severity(None, 0.01) is None
    assert classify_degradation_severity(-4.0, None) is None


def test_indicative_requires_significance():
    """A -2.5 slope at p=0.06 fails the 0.05 INDICATIVE gate; falls to WATCH."""
    assert classify_degradation_severity(-2.5, 0.04) == Severity.INDICATIVE
    # p=0.06 misses the 0.05 significance gate but -2.5 < -1.0 and 0.06 < 0.10:
    assert classify_degradation_severity(-2.5, 0.06) == Severity.WATCH


# ─── is_degradation_modelling_artifact (pure) ─────────────────────────────────


def test_artifact_guard_excludes_slope_over_20pct():
    """|slope| > 20 %/yr is a modelling artefact (e.g. Kincardine -172 %/yr)."""
    assert is_degradation_modelling_artifact(-172.0, 5.0) is True
    assert is_degradation_modelling_artifact(25.0, 5.0) is True
    assert is_degradation_modelling_artifact(-4.0, 5.0) is False
    assert is_degradation_modelling_artifact(None, 5.0) is False


# ─── is_floating_foundation (pure) ────────────────────────────────────────────


def test_is_floating_foundation():
    assert is_floating_foundation(_wf("floating")) is True
    assert is_floating_foundation(_wf("Floating")) is True
    assert is_floating_foundation(_wf("fixed")) is False
    assert is_floating_foundation(SimpleNamespace(id=1)) is False  # no attr
    assert is_floating_foundation(WF_ID) is False  # bare int


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_fires_indicative_with_baseline_caveat():
    """A degrading fixed-bottom farm fires INDICATIVE with baseline_caveat True."""
    ctx = _ctx(_degradation(-4.0, 0.04))
    result = await detect(ctx)
    assert result is not None
    assert result.schema_code is SchemaCode.OPS_04
    assert result.severity is Severity.INDICATIVE
    assert result.data_slots["baseline_caveat"] is True
    assert result.data_slots["confirmed_eligible"] is True
    assert result.data_slots["slope_pct_per_year"] == -4.0


@pytest.mark.asyncio
async def test_detect_watch_tier():
    """A modest slope fires WATCH and is NOT flagged confirmed_eligible."""
    ctx = _ctx(_degradation(-1.5, 0.08))
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.WATCH
    assert result.data_slots["baseline_caveat"] is True
    assert result.data_slots["confirmed_eligible"] is False


@pytest.mark.asyncio
async def test_floating_foundation_excluded():
    """Floating foundation → detect returns None even with a degrading slope."""
    ctx = _ctx(_degradation(-4.0, 0.04), windfarm=_wf("floating"))
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_artifact_guard_excludes_slope_over_20pct_in_detect():
    """A -172 %/yr artefact is excluded by detect (returns None)."""
    ctx = _ctx(_degradation(-172.0, 0.01))
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_fewer_than_3_years_data_excluded():
    """A regression window spanning < 3 years → detect returns None."""
    ctx = _ctx(
        _degradation(
            -4.0,
            0.04,
            analysis_start=date(2023, 1, 1),
            analysis_end=date(2024, 6, 1),  # ~1.4 years
        )
    )
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_detect_none_when_no_degradation_row():
    """No degradation result for the windfarm → detect returns None."""
    ctx = _ctx(None)
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_detect_none_when_sub_threshold():
    """A shallow / insignificant slope → no finding."""
    ctx = _ctx(_degradation(-0.5, 0.5))
    assert await detect(ctx) is None
