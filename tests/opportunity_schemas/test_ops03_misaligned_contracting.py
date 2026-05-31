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
from app.services.opportunity_schemas.ops03_misaligned_contracting import (
    classify_contracting_severity,
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


# ─── #97 · classify_contracting_severity (pure helper) ───────────────────────


def test_classify_confirmed_requires_ops01_confirmed():
    """OPS-01 CONFIRMED + known contract + no penalties → CONFIRMED. CONFIRMED is
    only reachable once OPS-01 itself can reach CONFIRMED (post-#95)."""
    assert (
        classify_contracting_severity(Severity.CONFIRMED, "merchant", False) == Severity.CONFIRMED
    )
    # OPS-01 only INDICATIVE → cannot reach CONFIRMED, drops to INDICATIVE.
    assert (
        classify_contracting_severity(Severity.INDICATIVE, "merchant", False) == Severity.INDICATIVE
    )


def test_classify_indicative_inherits_ops01_indicative():
    """OPS-01 INDICATIVE + known contract → INDICATIVE (inherited tier)."""
    assert (
        classify_contracting_severity(Severity.INDICATIVE, "merchant", None) == Severity.INDICATIVE
    )
    # OPS-01 CONFIRMED but penalties unknown → no CONFIRMED bar → INDICATIVE.
    assert (
        classify_contracting_severity(Severity.CONFIRMED, "merchant", None) == Severity.INDICATIVE
    )


def test_classify_suppressed_when_penalties_true():
    """ODI-linked availability penalties suppress the finding regardless of the
    OPS-01 tier or contract type."""
    assert classify_contracting_severity(Severity.CONFIRMED, "merchant", True) is None
    assert classify_contracting_severity(Severity.INDICATIVE, "fixed_price", True) is None
    assert classify_contracting_severity(Severity.WATCH, None, True) is None


def test_classify_suppressed_when_ops01_absent():
    """No OPS-01 finding → nothing to inherit → suppressed (None)."""
    assert classify_contracting_severity(None, "merchant", False) is None


def test_classify_watch_when_contract_unknown():
    """Unknown contract type → WATCH (data-limited), even with OPS-01 CONFIRMED."""
    assert classify_contracting_severity(Severity.CONFIRMED, None, False) == Severity.WATCH
    assert classify_contracting_severity(Severity.INDICATIVE, None, None) == Severity.WATCH
    # OPS-01 only WATCH with a known contract also degrades to WATCH.
    assert classify_contracting_severity(Severity.WATCH, "merchant", False) == Severity.WATCH


# ─── #97 · detect() end-to-end (OPS-01 inheritance through the registry seam) ─


def _confirmed_months():
    """9 months, 8 below the 95% ODI threshold over 2 years, avg ODI 81.0% (< the
    97% soft cap) → OPS-01 classifies CONFIRMED post-#95."""
    return _months(
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


def _indicative_months():
    """6 months, 4 below the 95% ODI threshold (non-consecutive, single soft-cap
    miss) → OPS-01 classifies INDICATIVE (>=4, <8 low months)."""
    return _months(
        ("2024-01", 80.0),
        ("2024-03", 70.0),
        ("2024-05", 99.0),
        ("2025-01", 79.0),
        ("2025-03", 60.0),
        ("2025-12", 99.0),
    )


@pytest.mark.asyncio
async def test_confirmed_requires_ops01_confirmed():
    """OPS-01 CONFIRMED + known output-agnostic contract, no penalties → OPS-03
    CONFIRMED (now reachable post-#95), branch A."""
    ppa = {
        "ppa_status": "active",
        "contract_type": "merchant",
        "has_availability_penalties": False,
        "ppa_duration_years": 3,
    }
    result = await detect(_ctx(monthly=_confirmed_months(), ppa=ppa))
    assert result is not None
    assert result.schema_code == SchemaCode.OPS_03
    assert result.severity == Severity.CONFIRMED
    assert result.branch == "A"


@pytest.mark.asyncio
async def test_indicative_inherits_ops01_indicative():
    """OPS-01 INDICATIVE + known contract → OPS-03 INDICATIVE (inherits the tier)."""
    ppa = {
        "ppa_status": "active",
        "contract_type": "merchant",
        "has_availability_penalties": False,
        "ppa_duration_years": 3,
    }
    result = await detect(_ctx(monthly=_indicative_months(), ppa=ppa))
    assert result is not None
    assert result.severity == Severity.INDICATIVE


@pytest.mark.asyncio
async def test_suppressed_when_penalties_true():
    """has_availability_penalties=True suppresses OPS-03 even when OPS-01 CONFIRMED."""
    ppa = {
        "ppa_status": "active",
        "contract_type": "merchant",
        "has_availability_penalties": True,
        "ppa_duration_years": 3,
    }
    assert await detect(_ctx(monthly=_confirmed_months(), ppa=ppa)) is None


@pytest.mark.asyncio
async def test_watch_when_contract_unknown():
    """contract_type None (no PPA) → OPS-03 WATCH, branch C, even with OPS-01
    CONFIRMED upstream."""
    result = await detect(_ctx(monthly=_confirmed_months(), ppa={}))
    assert result is not None
    assert result.severity == Severity.WATCH
    assert result.branch == "C"
