"""MKT-01 detector tests (issue #93) — verbatim reproduction of legacy behaviour.

Each test builds a DB-free ``DetectionContext`` via ``prefetched`` (keys
``capture_rate`` / ``cannibalisation_index`` / ``ppa_info``) and asserts
``await detect(ctx)`` matches the legacy ``_detect_mkt01`` outcome — proving the
migration is verbatim, INCLUDING:

* the never-fires zone-average bug (``capture_rate`` is ``None`` → ``None``); and
* the MKT-03 reclassification short-circuit (``ci > MKT03_CI_CONFIRMED`` → ``None``).

Cache-key note: a key present with value ``None`` (e.g. ``capture_rate=None``)
short-circuits the accessor to ``None`` without touching the DB — mirroring the
legacy ``_calc_capture_rate_gap`` returning ``None``.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.mkt01_low_capture_contracting import detect

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 101


def _ctx(capture_gap=None, cannibalisation=None, ppa=None):
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={
            "capture_rate": capture_gap,
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


@pytest.mark.asyncio
async def test_capture_rate_none_never_fires():
    """BUG 3 (never-fires): zone-average bug surfaces as ``capture_rate=None`` →
    MKT-01 returns ``None``. Matches snapshot 'mkt01_never_fires_no_opportunities'.
    """
    result = await detect(
        _ctx(capture_gap=None, cannibalisation=None, ppa={"ppa_status": "active"})
    )
    assert result is None


@pytest.mark.asyncio
async def test_gap_7pp_indicative_branch_c():
    """7pp gap, no CI → INDICATIVE branch C. Matches the would-fire reference shape
    'mkt01_would_fire_if_zone_average_present'.
    """
    result = await detect(
        _ctx(
            capture_gap={
                "capture_rate": 0.62,
                "zone_avg": 0.69,
                "gap_pp": 7.0,
                "bidzone_code": "NO2",
            },
            cannibalisation=None,
            ppa={"ppa_status": "active"},
        )
    )
    assert result is not None
    assert _shape(result) == (
        SchemaCode.MKT_01,
        Severity.INDICATIVE,
        "C",
        (
            "cannibalisation_index",
            "high_wind_capture_delta",
            "pcc_slope",
            "peer_capture_p50",
            "ppa_expiry_date",
            "revenue_impact_eur",
        ),
        (
            "cannibalisation_index",
            "capture_rate",
            "gap_pp",
            "period",
            "ppa_expiry_date",
            "ppa_status",
            "price_zone",
            "zone_avg_capture",
        ),
    )


@pytest.mark.asyncio
async def test_gap_below_watch_threshold_returns_none():
    """gap_pp 2.0 == MKT01_GAP_WATCH_PP (strict ``>``) → severity None → no row."""
    result = await detect(
        _ctx(
            capture_gap={
                "capture_rate": 0.67,
                "zone_avg": 0.69,
                "gap_pp": 2.0,
                "bidzone_code": "NO2",
            },
            cannibalisation=None,
            ppa={},
        )
    )
    assert result is None


@pytest.mark.asyncio
async def test_reclassification_short_circuit_when_ci_high():
    """MKT-01→MKT-03 reclassification: ci_latest > MKT03_CI_CONFIRMED (1.20) makes
    MKT-01 return ``None`` (MKT-03 handles it; proper hook is #111) — even though
    the gap alone would be CONFIRMED.
    """
    result = await detect(
        _ctx(
            capture_gap={
                "capture_rate": 0.50,
                "zone_avg": 0.70,
                "gap_pp": 20.0,  # would be CONFIRMED on gap alone
                "bidzone_code": "NO2",
            },
            cannibalisation={"ci_latest": 1.25},  # > MKT03_CI_CONFIRMED → reclassify
            ppa={},
        )
    )
    assert result is None


@pytest.mark.asyncio
async def test_ci_at_confirmed_threshold_does_not_short_circuit():
    """Reclassification is strict ``>``: ci_latest == 1.20 does NOT short-circuit;
    MKT-01 still fires (and ci >= MKT03_CI_WATCH → branch A)."""
    result = await detect(
        _ctx(
            capture_gap={
                "capture_rate": 0.55,
                "zone_avg": 0.70,
                "gap_pp": 15.0,
                "bidzone_code": "NO2",
            },
            cannibalisation={"ci_latest": 1.20},
            ppa={},
        )
    )
    assert result is not None
    assert result.schema_code == SchemaCode.MKT_01
    assert result.severity == Severity.CONFIRMED
    assert result.branch == "A"  # ci >= MKT03_CI_WATCH (1.05)
    assert "cannibalisation_index" not in result.missing_slots


@pytest.mark.asyncio
async def test_suppressed_by_long_fixed_ppa_returns_none():
    """Fixed-price PPA >5yr active → check_mkt01_suppression suppresses → None."""
    result = await detect(
        _ctx(
            capture_gap={
                "capture_rate": 0.55,
                "zone_avg": 0.70,
                "gap_pp": 15.0,
                "bidzone_code": "NO2",
            },
            cannibalisation=None,
            ppa={
                "contract_type": "fixed_price",
                "ppa_duration_years": 7,
                "ppa_status": "active",
            },
        )
    )
    assert result is None
