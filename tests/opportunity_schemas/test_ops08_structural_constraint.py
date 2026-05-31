"""OPS-08 detector tests (issue #103) — structural export constraint.

OPS-08 reads a Module 1b ``structural_constraint_flags`` row and classifies
severity from the analyst ``review_status`` plus the constraint's
``duration_hours`` / ``mean_q90_ratio`` — NOT from raw magnitude.

Severity tiers (see ``ops08_structural_constraint`` docstring):
    dismissed                                   → None (suppressed)
    mean_q90_ratio >= 0.85                      → None (too shallow)
    confirmed AND dur >= 672 AND q90 < 0.65     → CONFIRMED
    confirmed AND dur >= 336                    → INDICATIVE
    confirmed (dur < 336)                       → WATCH
    pending_review                              → WATCH

The 672 h bar = 4 weeks (CONFIRMED); 336 h = 2 weeks = Module 1b's minimum
detectable run = the INDICATIVE floor. A confirmed flag with ``dur == 335`` is
below that 2-week floor (a degenerate / sub-detector input) → WATCH.

All tests are DB-free: the flag dict is injected via
``DetectionContext(prefetched={"structural_constraint_flags": {...}})``.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops08_structural_constraint import (
    classify_constraint_severity,
    detect,
)

WF_ID = 103


def _ctx(flag=None):
    """A DB-free DetectionContext with a structural-constraint flag injected.

    Passing ``flag=None`` (the default) injects "no flag row" so ``detect``
    returns ``None``; pass a dict to inject a flag.
    """
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=datetime(2024, 1, 1),
        period_end=datetime(2026, 1, 1),
        prefetched={"structural_constraint_flags": flag},
    )


def _flag(review_status, duration_hours, mean_q90_ratio):
    """A minimal flag dict in the shape ``load_structural_constraint_flags`` returns."""
    return {
        "review_status": review_status,
        "duration_hours": duration_hours,
        "mean_q90_ratio": mean_q90_ratio,
        "mean_q50_ratio": None,
        "flag_trigger": "q90_ratio",
        "period_start": datetime(2024, 6, 1),
        "period_end": datetime(2024, 9, 1),
    }


# ─── classify_constraint_severity (pure) ──────────────────────────────────────


def test_confirmed_requires_confirmed_and_672h_and_q90_below_065():
    """CONFIRMED needs ALL of: confirmed + duration >= 672 + q90 < 0.65.

    Drop any one and it falls to a lower tier:
        ("confirmed", 672, 0.64) → CONFIRMED   (meets the full dual bar)
        ("confirmed", 672, 0.66) → INDICATIVE  (q90 >= 0.65 → not deep enough)
        ("confirmed", 335, 0.5)  → WATCH        (below the 336 h INDICATIVE floor)
    """
    assert classify_constraint_severity("confirmed", 672, 0.64) is Severity.CONFIRMED
    assert classify_constraint_severity("confirmed", 672, 0.66) is Severity.INDICATIVE
    # 335 h is below the 2-week (336 h) INDICATIVE floor → WATCH (real but
    # sub-threshold). q90 0.5 is deep and well below the 0.85 suppression bar.
    assert classify_constraint_severity("confirmed", 335, 0.5) is Severity.WATCH


def test_q90_065_boundary_is_strict():
    """The q90 depth bar is strict ``<``: q90 == 0.65 misses CONFIRMED → INDICATIVE."""
    assert classify_constraint_severity("confirmed", 672, 0.65) is Severity.INDICATIVE
    assert classify_constraint_severity("confirmed", 672, 0.649) is Severity.CONFIRMED


def test_duration_672_boundary_is_inclusive():
    """The 672 h CONFIRMED bar is inclusive ``>=``; 671 h (deep) → INDICATIVE."""
    assert classify_constraint_severity("confirmed", 672, 0.5) is Severity.CONFIRMED
    assert classify_constraint_severity("confirmed", 671, 0.5) is Severity.INDICATIVE


def test_indicative_when_confirmed_below_dual_threshold():
    """Confirmed but below the dual CONFIRMED bar (duration >= 336) → INDICATIVE."""
    assert classify_constraint_severity("confirmed", 336, 0.7) is Severity.INDICATIVE


def test_pending_review_is_watch():
    """Auto-detected, not-yet-reviewed flags → WATCH regardless of depth/duration."""
    assert classify_constraint_severity("pending_review", 400, 0.5) is Severity.WATCH
    assert classify_constraint_severity("pending_review", 1000, 0.3) is Severity.WATCH


def test_dismissed_does_not_fire():
    """A dismissed flag → None even when deep and long-lived."""
    assert classify_constraint_severity("dismissed", 700, 0.4) is None
    assert classify_constraint_severity("dismissed", 336, 0.6) is None


def test_shallow_constraint_is_suppressed():
    """mean_q90_ratio >= 0.85 = too shallow to matter → None, even if confirmed."""
    assert classify_constraint_severity("confirmed", 1000, 0.85) is None
    assert classify_constraint_severity("pending_review", 1000, 0.9) is None
    # Just below the suppression bar still fires.
    assert classify_constraint_severity("confirmed", 1000, 0.84) is Severity.INDICATIVE


def test_none_q90_does_not_suppress_and_caps_at_indicative():
    """A missing q90 ratio does NOT suppress, but cannot clear the CONFIRMED depth bar."""
    assert classify_constraint_severity("confirmed", 1000, None) is Severity.INDICATIVE
    assert classify_constraint_severity("pending_review", 1000, None) is Severity.WATCH


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_none_when_no_flags():
    """No structural-constraint flag for the windfarm → detect returns None."""
    assert await detect(_ctx(None)) is None


@pytest.mark.asyncio
async def test_detect_confirmed_emits_result():
    """A confirmed, deep, long-lived flag → CONFIRMED DetectorResult on OPS_08."""
    result = await detect(_ctx(_flag("confirmed", 800, 0.5)))
    assert result is not None
    assert result.schema_code is SchemaCode.OPS_08
    assert result.severity is Severity.CONFIRMED
    assert result.data_slots["review_status"] == "confirmed"
    assert result.data_slots["duration_hours"] == 800
    assert result.data_slots["mean_q90_ratio"] == 0.5


@pytest.mark.asyncio
async def test_detect_pending_review_is_watch():
    """A pending-review flag surfaces as a WATCH finding."""
    result = await detect(_ctx(_flag("pending_review", 400, 0.5)))
    assert result is not None
    assert result.severity is Severity.WATCH


@pytest.mark.asyncio
async def test_detect_dismissed_does_not_fire():
    """A dismissed flag → no finding (detect returns None)."""
    assert await detect(_ctx(_flag("dismissed", 700, 0.4))) is None


@pytest.mark.asyncio
async def test_detect_shallow_constraint_suppressed():
    """A too-shallow flag (q90 >= 0.85) → no finding even when confirmed."""
    assert await detect(_ctx(_flag("confirmed", 1000, 0.9))) is None
