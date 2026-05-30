"""MKT-01 detector tests (issue #93) — verbatim reproduction of legacy behaviour.

Each test builds a DB-free ``DetectionContext`` via ``prefetched`` (keys
``capture_rate`` / ``cannibalisation_index`` / ``ppa_info``) and asserts
``await detect(ctx)`` matches the legacy ``_detect_mkt01`` outcome — proving the
migration is verbatim, INCLUDING:

* the never-fires zone-average bug (``capture_rate`` is ``None`` → ``None``).

NOTE (#111): the inline MKT-03 reclassification short-circuit
(``ci > MKT03_CI_CONFIRMED`` → ``None``) has been REMOVED from the detector — the
cross-schema redirect now lives in the ``reclassify_capture_to_cannibalisation``
registry post-pass (tested in ``test_reclassification.py``). The two tests below
that used to assert the short-circuit now assert the detector FIRES regardless of
CI.

Cache-key note: a key present with value ``None`` (e.g. ``capture_rate=None``)
short-circuits the accessor to ``None`` without touching the DB — mirroring the
legacy ``_calc_capture_rate_gap`` returning ``None``.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.mkt01_low_capture_contracting import (
    check_capture_suppression,
    classify_capture_gap_severity,
    detect,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 101


def _ctx(capture_gap=None, cannibalisation=None, ppa=None, curtailment_pct=None):
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={
            "capture_rate": capture_gap,
            "cannibalisation_index": cannibalisation,
            "ppa_info": ppa if ppa is not None else {},
            # #94: present so the detector's load_curtailment_pct() short-circuits
            # without touching the DB (db=None here).
            "curtailment_pct": curtailment_pct,
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
async def test_detector_still_fires_when_ci_high_reclassification_moved_to_registry():
    """CHANGED #111: the inline MKT-01→MKT-03 short-circuit
    (``ci > MKT03_CI_CONFIRMED → None``) has been REMOVED from the detector.

    MKT-01 now ALWAYS fires on its own capture-gap signal even when ci_latest is
    high (1.25 > 1.20): the cross-schema redirect to MKT-03 is now the
    ``reclassify_capture_to_cannibalisation`` registry post-pass (over the full
    result set), NOT a buried ``return None`` here. So the detector returns a
    CONFIRMED finding (gap 20pp → CONFIRMED, ci >= MKT03_CI_WATCH → branch A);
    whether it ends up SUPPRESSED-into-MKT-03 is decided by the registry pass,
    tested in ``tests/opportunity_schemas/test_reclassification.py``.
    """
    result = await detect(
        _ctx(
            capture_gap={
                "capture_rate": 0.50,
                "zone_avg": 0.70,
                "gap_pp": 20.0,  # CONFIRMED on gap alone
                "bidzone_code": "NO2",
            },
            cannibalisation={"ci_latest": 1.25},  # high CI — no longer short-circuits here
            ppa={},
        )
    )
    assert result is not None
    assert result.schema_code == SchemaCode.MKT_01
    assert result.severity == Severity.CONFIRMED
    assert result.branch == "A"  # ci >= MKT03_CI_WATCH


@pytest.mark.asyncio
async def test_ci_at_confirmed_threshold_fires_branch_a():
    """ci_latest == 1.20 → MKT-01 fires CONFIRMED, branch A (ci >= MKT03_CI_WATCH).

    Unchanged by #111 (this never hit the old short-circuit, which was strict
    ``>``); retained as a branch-selection lock.
    """
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


# ─────────────────────── #94 corrected pure helpers ─────────────────────────


def test_classify_capture_gap_severity_boundaries():
    """Recalibrated thresholds (issue #94), strictly-greater-than: >10/>6/>3 pp.

    Boundary values land on the LOWER tier (strict ``>``); just-above values land
    on the HIGHER tier.
    """
    assert classify_capture_gap_severity(10.0) == Severity.INDICATIVE  # not >10
    assert classify_capture_gap_severity(10.01) == Severity.CONFIRMED
    assert classify_capture_gap_severity(6.0) == Severity.WATCH  # not >6
    assert classify_capture_gap_severity(6.01) == Severity.INDICATIVE
    assert classify_capture_gap_severity(3.0) is None  # not >3
    assert classify_capture_gap_severity(3.01) == Severity.WATCH


def test_suppressed_when_curtailment_above_15pct():
    """Curtailment >15% suppresses MKT-01 (grid-driven loss); 15.0 exactly does
    not (strict ``>``). PPA empty so only the curtailment rule is exercised."""
    assert check_capture_suppression({}, 15.0) is None  # boundary: not suppressed
    reason = check_capture_suppression({}, 15.01)
    assert reason == "MKT-01 suppressed: curtailment >15% — capture loss is grid-driven"
    # None curtailment (data unavailable) never triggers.
    assert check_capture_suppression({}, None) is None


@pytest.mark.asyncio
async def test_mkt01_fires_after_zone_average_fix():
    """End-to-end (DB-free): a 7pp gap now yields a MKT-01 INDICATIVE row.

    Pre-#94 the zone-average bug made ``load_capture_rate`` return None and this
    detector returned None. With the data-layer fix, a 7pp gap (>6 → INDICATIVE)
    and no curtailment / locked-PPA suppression now produces a finding.
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
            curtailment_pct=None,
        )
    )
    assert result is not None
    assert result.schema_code == SchemaCode.MKT_01
    assert result.severity == Severity.INDICATIVE
    assert result.branch == "C"


@pytest.mark.asyncio
async def test_mkt01_suppressed_when_curtailment_high_returns_none():
    """A confirmed-tier gap is suppressed when curtailment >15% (grid-driven)."""
    result = await detect(
        _ctx(
            capture_gap={
                "capture_rate": 0.50,
                "zone_avg": 0.70,
                "gap_pp": 20.0,  # would be CONFIRMED on gap alone
                "bidzone_code": "NO2",
            },
            cannibalisation=None,
            ppa={"ppa_status": "active"},
            curtailment_pct=18.0,
        )
    )
    assert result is None
