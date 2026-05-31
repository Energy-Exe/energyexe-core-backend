"""OPS-01 detector tests — corrected M2 behaviour (issue #95).

#92 migrated OPS-01 verbatim (preserving the force-downgrade + threshold bugs).
#95 fixes them:

* spec thresholds 8 / 4 / 2 months → CONFIRMED / INDICATIVE / WATCH (1 → None);
* two consecutive low months escalate a WATCH to INDICATIVE;
* an average ODI > 97% soft-caps the result to WATCH;
* the ``wind_resource_index`` force-downgrade is removed (CONFIRMED stays
  CONFIRMED) but the slot is STILL flagged in ``missing_slots``;
* a fixed-price long-dated active PPA suppresses the finding.

The pure helper ``classify_disruption_severity`` is exercised directly for the
tier boundaries; ``detect`` is exercised end-to-end (DB-free) for assembly,
missing-slot flagging, and suppression.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops01_volatile_disruption import (
    check_disruption_suppression,
    classify_disruption_severity,
    detect,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 101


def _months(*specs):
    return [{"month": m, "availability_pct": pct} for m, pct in specs]


def _ctx(monthly=None, ppa=None):
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={
            "monthly_performance": monthly if monthly is not None else [],
            "ppa_info": ppa if ppa is not None else {},
        },
    )


def _shape(result):
    return (
        result.schema_code,
        result.severity,
        result.branch,
        tuple(sorted(result.missing_slots)),
        tuple(sorted(result.data_slots.keys())),
    )


# ───────────────────────── Pure severity-tier boundaries ────────────────────


def test_severity_8_months_is_confirmed():
    """8 months below threshold → CONFIRMED (previously impossible — the bug)."""
    assert classify_disruption_severity(8) == Severity.CONFIRMED


def test_severity_4_months_is_indicative():
    """4 months below threshold → INDICATIVE."""
    assert classify_disruption_severity(4) == Severity.INDICATIVE


def test_severity_2_months_is_watch():
    """2 months below threshold (non-consecutive) → WATCH."""
    assert classify_disruption_severity(2, max_consecutive=1) == Severity.WATCH


def test_severity_1_month_is_none():
    """1 month below threshold → no finding."""
    assert classify_disruption_severity(1) is None


def test_two_consecutive_months_escalates_to_indicative():
    """months_below=2 with a 2-month consecutive run → escalated to INDICATIVE."""
    assert classify_disruption_severity(2, max_consecutive=2) == Severity.INDICATIVE


def test_odi_above_97_soft_caps_to_watch():
    """9 months below threshold but avg ODI 97.5% (>97) → soft-capped to WATCH."""
    assert classify_disruption_severity(9, max_consecutive=9, avg_odi_pct=97.5) == Severity.WATCH


def test_odi_at_97_does_not_soft_cap():
    """avg ODI exactly 97.0 is NOT above the cap (> boundary) → severity unchanged."""
    assert classify_disruption_severity(8, avg_odi_pct=97.0) == Severity.CONFIRMED


# ───────────────────────────── detect() assembly ────────────────────────────


@pytest.mark.asyncio
async def test_eight_low_months_two_years_is_confirmed():
    """8 months < 95% over 2 years → CONFIRMED (no more force-downgrade).

    avg ODI = 81.0% (well below the 97% soft cap), no PPA → branch C.
    Replaces the M1 ``..._is_force_downgraded_to_indicative`` expectation.
    """
    monthly = _months(
        ("2024-01", 80.0),
        ("2024-02", 82.0),
        ("2024-03", 70.0),
        ("2024-11", 88.0),
        ("2025-01", 79.0),
        ("2025-02", 81.0),
        ("2025-03", 60.0),
        ("2025-11", 90.0),
        ("2025-12", 99.0),
    )
    result = await detect(_ctx(monthly=monthly, ppa={}))
    assert result is not None
    assert _shape(result) == (
        SchemaCode.OPS_01,
        Severity.CONFIRMED,  # was INDICATIVE under the M1 force-downgrade bug
        "C",
        ("maintenance_schedule", "peer_odi_p50", "ppa_status", "wind_resource_index"),
        (
            "disruption_month_list",
            "odi_months_below_threshold",
            "odi_pct",
            "odi_threshold",
            "period",
            "ppa_status",
        ),
    )


@pytest.mark.asyncio
async def test_missing_wind_resource_index_does_not_cap_severity():
    """CONFIRMED stays CONFIRMED AND wind_resource_index is still in missing_slots."""
    monthly = _months(
        ("2024-01", 80.0),
        ("2024-02", 82.0),
        ("2024-03", 70.0),
        ("2024-11", 88.0),
        ("2025-01", 79.0),
        ("2025-02", 81.0),
        ("2025-03", 60.0),
        ("2025-11", 90.0),
    )
    result = await detect(_ctx(monthly=monthly, ppa={}))
    assert result is not None
    assert result.severity == Severity.CONFIRMED
    assert "wind_resource_index" in result.missing_slots


@pytest.mark.asyncio
async def test_one_low_month_single_year_is_none():
    """Only 1 month below the 95% ODI threshold → no OPS-01 finding (was WATCH)."""
    monthly = _months(("2025-03", 80.0), ("2025-12", 99.0))
    ppa = {
        "ppa_status": "active",
        "contract_type": "merchant",
        "has_availability_penalties": False,
        "ppa_duration_years": 3,
    }
    assert await detect(_ctx(monthly=monthly, ppa=ppa)) is None


@pytest.mark.asyncio
async def test_detect_returns_none_when_no_finding():
    """All months at/above the 95% ODI threshold → no low months → no finding.

    Also covers the empty-monthly short-circuit."""
    healthy = _months(("2024-06", 99.0), ("2024-07", 98.0), ("2025-06", 100.0))
    assert await detect(_ctx(monthly=healthy, ppa={})) is None
    assert await detect(_ctx(monthly=[], ppa={})) is None


# ──────────────────────────────── Suppression ───────────────────────────────


def test_fixed_price_long_ppa_suppression_reason():
    """A fixed-price, long-dated active PPA returns a suppression reason."""
    assert (
        check_disruption_suppression(
            {
                "contract_type": "fixed_price",
                "ppa_duration_years": 10,
                "ppa_status": "active",
            }
        )
        is not None
    )


def test_short_fixed_ppa_not_suppressed():
    """A short (<5yr) fixed-price PPA does not suppress."""
    assert (
        check_disruption_suppression(
            {
                "contract_type": "fixed_price",
                "ppa_duration_years": 3,
                "ppa_status": "active",
            }
        )
        is None
    )


@pytest.mark.asyncio
async def test_fixed_price_long_ppa_downgrades():
    """End-to-end: an 8-low-month farm with a fixed-price long-dated active PPA is
    suppressed (no OPS-01 row), per spec."""
    monthly = _months(
        ("2024-01", 80.0),
        ("2024-02", 82.0),
        ("2024-03", 70.0),
        ("2024-11", 88.0),
        ("2025-01", 79.0),
        ("2025-02", 81.0),
        ("2025-03", 60.0),
        ("2025-11", 90.0),
    )
    ppa = {
        "ppa_status": "active",
        "contract_type": "fixed_price",
        "has_availability_penalties": False,
        "ppa_duration_years": 12,
    }
    assert await detect(_ctx(monthly=monthly, ppa=ppa)) is None
