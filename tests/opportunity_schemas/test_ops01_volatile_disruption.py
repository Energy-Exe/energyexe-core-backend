"""OPS-01 detector tests (issue #92) — verbatim reproduction of legacy behaviour.

Each test builds a DB-free ``DetectionContext`` via ``prefetched`` (keys
``monthly_performance`` / ``ppa_info``) matching the scenarios frozen in the #91
characterization snapshot, then asserts ``await detect(ctx)`` returns a
``DetectorResult`` whose
``(schema_code, severity, branch, sorted(missing_slots), sorted(data_slots))``
equals the legacy outcome — proving the migration is verbatim, INCLUDING the
OPS-01 force-downgrade bug (CONFIRMED → INDICATIVE).
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops01_volatile_disruption import detect

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


@pytest.mark.asyncio
async def test_eight_low_months_two_years_is_force_downgraded_to_indicative():
    """BUG-DEMONSTRATING: 8 months < 95% over 2 years → determine=CONFIRMED, but
    wind_resource_index is hardcoded missing → severity forced to INDICATIVE
    (the #95 fix removes this). Matches snapshot 'ops01_should_be_confirmed_is_indicative'.
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
        Severity.INDICATIVE,  # NOT CONFIRMED — the preserved force-downgrade bug
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
    # wind_resource_index is always present in missing_slots (the bug's hardcode).
    assert "wind_resource_index" in result.missing_slots


@pytest.mark.asyncio
async def test_one_low_month_single_year_merchant_ppa_is_watch_branch_a():
    """1 low month, single year, merchant PPA → WATCH, branch A. ppa_status present
    so it is NOT added to missing_slots. Matches 'ops01_watch_with_ops03_followon'.
    """
    monthly = _months(("2025-03", 80.0), ("2025-12", 99.0))
    ppa = {
        "ppa_status": "active",
        "contract_type": "merchant",
        "has_availability_penalties": False,
        "ppa_duration_years": 3,
    }
    result = await detect(_ctx(monthly=monthly, ppa=ppa))
    assert result is not None
    assert _shape(result) == (
        SchemaCode.OPS_01,
        Severity.WATCH,
        "A",
        ("maintenance_schedule", "peer_odi_p50", "wind_resource_index"),
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
async def test_detect_returns_none_when_no_finding():
    """All months at/above the 95% ODI threshold → no low months → severity None →
    no OPS-01 finding. Also covers the empty-monthly short-circuit."""
    healthy = _months(("2024-06", 99.0), ("2024-07", 98.0), ("2025-06", 100.0))
    assert await detect(_ctx(monthly=healthy, ppa={})) is None
    # Empty monthly performance → None as well.
    assert await detect(_ctx(monthly=[], ppa={})) is None
