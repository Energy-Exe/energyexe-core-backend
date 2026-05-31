"""MKT-02 detector tests (issue #93) — verbatim reproduction of legacy behaviour.

MKT-02 depends on MKT-01: it re-runs MKT-01's ``detect(ctx)`` (which reads only
ctx-memoized data) to obtain the MKT-01 outcome, then caps its own severity off
``mkt01.severity`` and copies ``mkt01.data_slots["price_zone"]``. So the context
must carry the MKT-01 inputs too (``capture_rate`` / ``cannibalisation_index`` /
``ppa_info``).

Tests assert ``await detect(ctx)`` matches the legacy ``_detect_mkt02`` outcome,
including:

* MKT-02 returns ``None`` when MKT-01 did not fire (the orchestrator's dependency
  gate also enforces this; the detector re-checks it directly); and
* the severity cap: MKT-01 CONFIRMED → MKT-02 INDICATIVE, otherwise WATCH.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.mkt02_low_capture_storage import detect

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


_MKT02_MISSING = (
    "bess_revenue_potential_eur",
    "grid_headroom_mw",
    "intraday_price_spread",
    "mfrr_eligible",
    "optimal_bess_size_mwh",
)
_MKT02_SLOTS = ("mkt01_severity", "period", "ppa_status", "price_zone", "storage_present")


@pytest.mark.asyncio
async def test_returns_none_when_mkt01_absent():
    """MKT-01 does not fire (capture_rate None) → MKT-02 returns None."""
    assert await detect(_ctx(capture_gap=None, cannibalisation=None, ppa={})) is None


@pytest.mark.asyncio
async def test_watch_followon_when_mkt01_indicative():
    """MKT-01 INDICATIVE (7pp gap) → MKT-02 WATCH branch C, price_zone copied.
    Matches snapshot 'mkt01_would_fire_if_zone_average_present' (MKT-02 leg).
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
    assert _shape(result) == (SchemaCode.MKT_02, Severity.WATCH, "C", _MKT02_MISSING, _MKT02_SLOTS)
    assert result.data_slots["price_zone"] == "NO2"
    assert result.data_slots["mkt01_severity"] == Severity.INDICATIVE


@pytest.mark.asyncio
async def test_indicative_cap_when_mkt01_confirmed():
    """MKT-01 CONFIRMED (large gap, no high CI) → MKT-02 capped to INDICATIVE
    (no storage data). The follow-on cap is the only thing the legacy method
    does with MKT-01's severity."""
    result = await detect(
        _ctx(
            capture_gap={
                "capture_rate": 0.50,
                "zone_avg": 0.70,
                "gap_pp": 20.0,  # > MKT01_GAP_CONFIRMED_PP → CONFIRMED
                "bidzone_code": "NO2",
            },
            cannibalisation=None,
            ppa={"ppa_status": "active"},
        )
    )
    assert result is not None
    assert result.severity == Severity.INDICATIVE  # capped down from CONFIRMED
    assert result.branch == "C"
    assert result.data_slots["mkt01_severity"] == Severity.CONFIRMED
