"""MKT-03 detector tests (issue #93) — verbatim reproduction of legacy behaviour.

Each test builds a DB-free ``DetectionContext`` via ``prefetched`` (keys
``cannibalisation_index`` / ``ppa_info``) and asserts ``await detect(ctx)``
matches the legacy ``_detect_mkt03`` outcome — including the no-trend
graceful-degradation downgrade (CONFIRMED → INDICATIVE).
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.mkt03_high_cannibalisation import detect

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 101


def _ctx(cannibalisation=None, ppa=None):
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={
            "cannibalisation_index": cannibalisation,
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


_MKT03_MISSING = (
    "alternative_zone_assets",
    "peer_zone_ci",
    "portfolio_zone_correlation",
    "revenue_impact_eur",
    "zone_renewable_penetration_pct",
)
_MKT03_SLOTS = (
    "cannibalisation_index",
    "ci_trend_yoy",
    "ci_values_by_year",
    "period",
    "ppa_status",
    "price_zone",
)


@pytest.mark.asyncio
async def test_no_ci_data_returns_none():
    """No cannibalisation data → None."""
    assert await detect(_ctx(cannibalisation=None, ppa={})) is None


@pytest.mark.asyncio
async def test_confirmed_with_trend_branch_a():
    """CI 1.25, 2 yrs sustained, worsening trend (>0.02) → CONFIRMED branch A.
    Matches snapshot 'mkt03_confirmed'.
    """
    result = await detect(
        _ctx(
            cannibalisation={
                "ci_latest": 1.25,
                "ci_by_year": {"2024": 1.22, "2025": 1.25},
                "ci_trend": 0.03,
                "years_above_threshold": 2,
                "bidzone_code": "NO2",
            },
            ppa={},
        )
    )
    assert result is not None
    assert _shape(result) == (
        SchemaCode.MKT_03,
        Severity.CONFIRMED,
        "A",
        _MKT03_MISSING,
        _MKT03_SLOTS,
    )


@pytest.mark.asyncio
async def test_confirmed_eligible_no_trend_downgrades_to_indicative_branch_c():
    """CONFIRMED-eligible CI but ci_trend None (single year) → graceful-degradation
    downgrade to INDICATIVE, branch C. Matches snapshot
    'mkt03_confirmed_downgraded_no_trend'.
    """
    result = await detect(
        _ctx(
            cannibalisation={
                "ci_latest": 1.25,
                "ci_by_year": {"2025": 1.25},
                "ci_trend": None,
                "years_above_threshold": 2,
                "bidzone_code": "NO2",
            },
            ppa={},
        )
    )
    assert result is not None
    assert _shape(result) == (
        SchemaCode.MKT_03,
        Severity.INDICATIVE,
        "C",
        _MKT03_MISSING,
        _MKT03_SLOTS,
    )


@pytest.mark.asyncio
async def test_indicative_when_ci_above_110_without_two_years():
    """CI 1.15 (>=MKT03_CI_INDICATIVE 1.10) but only 1 yr sustained → INDICATIVE,
    branch C (trend not >0.02)."""
    result = await detect(
        _ctx(
            cannibalisation={
                "ci_latest": 1.15,
                "ci_by_year": {"2024": 1.14, "2025": 1.15},
                "ci_trend": 0.01,
                "years_above_threshold": 1,
                "bidzone_code": "NO2",
            },
            ppa={},
        )
    )
    assert result is not None
    assert result.severity == Severity.INDICATIVE
    assert result.branch == "C"


@pytest.mark.asyncio
async def test_below_watch_threshold_returns_none():
    """CI 1.04 < MKT03_CI_WATCH (1.05) → severity None → no row."""
    result = await detect(
        _ctx(
            cannibalisation={
                "ci_latest": 1.04,
                "ci_by_year": {"2025": 1.04},
                "ci_trend": None,
                "years_above_threshold": 0,
                "bidzone_code": "NO2",
            },
            ppa={},
        )
    )
    assert result is None


@pytest.mark.asyncio
async def test_suppressed_by_long_fixed_ppa_returns_none():
    """Fixed-price PPA >5yr active → check_mkt03_suppression suppresses → None."""
    result = await detect(
        _ctx(
            cannibalisation={
                "ci_latest": 1.25,
                "ci_by_year": {"2024": 1.22, "2025": 1.25},
                "ci_trend": 0.03,
                "years_above_threshold": 2,
                "bidzone_code": "NO2",
            },
            ppa={
                "contract_type": "fixed_price",
                "ppa_duration_years": 7,
                "ppa_status": "active",
            },
        )
    )
    assert result is None
