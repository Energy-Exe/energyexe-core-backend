"""OPS-03 detector tests (issue #92) — verbatim reproduction of legacy behaviour.

OPS-03 consumes the OPS-01 outcome (severity + odi_pct). In the registry model
``detect(ctx)`` re-runs OPS-01's detector against the same ctx-memoized data, so
the context must supply ``monthly_performance`` (to make OPS-01 fire) plus
``ppa_info``. These mirror the #91 snapshot's OPS-03 scenarios.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops03_misaligned_contracting import detect

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
async def test_no_ppa_watch_branch_c_when_ops01_fires():
    """OPS-01 fires (8 low months → INDICATIVE after downgrade) with no PPA →
    contract_type None → OPS-03 WATCH, branch C, contract_type +
    contract_penalty_clauses in missing_slots. Matches the OPS-03 row in snapshot
    'ops01_should_be_confirmed_is_indicative'.
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
        SchemaCode.OPS_03,
        Severity.WATCH,
        "C",
        (
            "am_location",
            "asset_age_years",
            "contract_penalty_clauses",
            "contract_type",
            "insource_benchmark",
            "oem_response_time",
            "peer_odi_p50",
        ),
        ("contract_type", "has_availability_penalties", "odi_pct", "period", "ppa_status"),
    )


@pytest.mark.asyncio
async def test_merchant_ppa_no_penalties_watch_branch_a():
    """OPS-01 WATCH + merchant PPA, no penalties → OPS-03 WATCH, branch A
    (incentive misalignment). contract_type known → not in missing_slots.

    Input updated for #95: OPS-01's WATCH tier now needs 2 (non-consecutive) low
    months (was 1 under the legacy 1/2/3-month bands). Two non-adjacent low months
    (Mar + Jun) keep ``max_consecutive < 2`` so OPS-01 stays WATCH (no escalation),
    which keeps OPS-03 at WATCH, branch A — the original assertion intent.
    """
    monthly = _months(("2025-03", 80.0), ("2025-06", 82.0), ("2025-12", 99.0))
    ppa = {
        "ppa_status": "active",
        "contract_type": "merchant",
        "has_availability_penalties": False,
        "ppa_duration_years": 3,
    }
    result = await detect(_ctx(monthly=monthly, ppa=ppa))
    assert result is not None
    assert _shape(result) == (
        SchemaCode.OPS_03,
        Severity.WATCH,
        "A",
        (
            "am_location",
            "asset_age_years",
            "insource_benchmark",
            "oem_response_time",
            "peer_odi_p50",
        ),
        ("contract_type", "has_availability_penalties", "odi_pct", "period", "ppa_status"),
    )


@pytest.mark.asyncio
async def test_suppressed_when_availability_penalties_present():
    """OPS-01 fires but the PPA has availability penalties → OPS-03 suppressed
    (returns None) — verbatim legacy ``has_penalties is True`` short-circuit."""
    monthly = _months(("2025-03", 80.0), ("2025-12", 99.0))
    ppa = {
        "ppa_status": "active",
        "contract_type": "fixed_price",
        "has_availability_penalties": True,
        "ppa_duration_years": 3,
    }
    assert await detect(_ctx(monthly=monthly, ppa=ppa)) is None


@pytest.mark.asyncio
async def test_detect_returns_none_when_no_finding():
    """When OPS-01 does not fire (no low months), OPS-03 produces nothing — it
    depends on the OPS-01 outcome being present."""
    healthy = _months(("2024-06", 99.0), ("2025-06", 100.0))
    assert await detect(_ctx(monthly=healthy, ppa={})) is None
    assert await detect(_ctx(monthly=[], ppa={})) is None
