"""MKT-03 detector tests.

Originally (#93) a verbatim reproduction of the legacy ``_detect_mkt03``
behaviour; M2 (#98) recalibrates the thresholds and trend logic. Each
detector-level test builds a DB-free ``DetectionContext`` via ``prefetched``
(keys ``cannibalisation_index`` / ``ppa_info``) and asserts ``await detect(ctx)``
matches the recalibrated outcome. The pure-helper tests at the bottom lock the
recalibrated severity / trend-downgrade / outlier-exclusion semantics directly.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.mkt03_high_cannibalisation import (
    apply_ci_trend_downgrade,
    classify_cannibalisation_severity,
    compute_ci_trend,
    detect,
)

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
    """CI ≥ 1.20 but a single-year series (no sustained 2 yrs, no rising trend)
    → cannot reach CONFIRMED → INDICATIVE, branch C (ci_trend None). Matches
    snapshot 'mkt03_confirmed_downgraded_no_trend'. Under #98 this lands at
    INDICATIVE via the ``years_sustained < 2`` gate rather than the legacy
    no-trend graceful-degradation step; the observed outcome is unchanged.
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
    """CI 1.04 < MKT03_CI_WATCH (1.08 under #98) → severity None → no row."""
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


# ─────────────────── Recalibrated pure-helper tests (#98) ────────────────────


def test_watch_boundary_108():
    """CI 1.08 → WATCH; 1.079 → None (recalibrated WATCH entry raised to 1.08)."""
    assert classify_cannibalisation_severity(1.08, 0, False) == Severity.WATCH
    assert classify_cannibalisation_severity(1.079, 0, False) is None


def test_indicative_boundary_110():
    """CI 1.10 → INDICATIVE (regardless of sustained years / rising flag)."""
    assert classify_cannibalisation_severity(1.10, 0, False) == Severity.INDICATIVE
    # Just below the INDICATIVE entry but at/above WATCH → WATCH.
    assert classify_cannibalisation_severity(1.099, 0, False) == Severity.WATCH


def test_confirmed_requires_120_and_2yr_and_rising():
    """CONFIRMED needs all three: CI ≥ 1.20, ≥ 2 sustained years, rising."""
    # All three present → CONFIRMED.
    assert classify_cannibalisation_severity(1.20, 2, True) == Severity.CONFIRMED
    # Each one missing → drops to the next tier (INDICATIVE, since CI ≥ 1.10).
    assert classify_cannibalisation_severity(1.19, 2, True) == Severity.INDICATIVE
    assert classify_cannibalisation_severity(1.20, 1, True) == Severity.INDICATIVE
    assert classify_cannibalisation_severity(1.20, 2, False) == Severity.INDICATIVE


def test_trend_downgrade_when_yoy_le_minus_008():
    """CONFIRMED + YoY trend ≤ -0.08 → INDICATIVE; -0.079 leaves it unchanged."""
    assert apply_ci_trend_downgrade(Severity.CONFIRMED, -0.08) == Severity.INDICATIVE
    assert apply_ci_trend_downgrade(Severity.CONFIRMED, -0.079) == Severity.CONFIRMED
    # Non-CONFIRMED severities and a None trend are never downgraded.
    assert apply_ci_trend_downgrade(Severity.INDICATIVE, -0.20) == Severity.INDICATIVE
    assert apply_ci_trend_downgrade(Severity.CONFIRMED, None) == Severity.CONFIRMED


def test_prior_year_ci_above_2_excluded_from_trend():
    """Prior year CI > 2.0 is dropped → trend from {1.1, 1.2} = +0.1."""
    assert compute_ci_trend({2023: 2.5, 2024: 1.1, 2025: 1.2}) == pytest.approx(0.1)
    # String year keys behave identically.
    assert compute_ci_trend({"2023": 2.5, "2024": 1.1, "2025": 1.2}) == pytest.approx(0.1)
    # Latest year is never excluded, even if > 2.0; with only one survivor → None.
    assert compute_ci_trend({2024: 1.1, 2025: 2.5}) == pytest.approx(1.4)
    # Single-year series → no YoY trend.
    assert compute_ci_trend({2025: 1.25}) is None
