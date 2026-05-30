"""OPS-02 detector tests (issue #96) — HODI+SSR rewrite.

The legacy "summer CF > winter CF" inversion firing condition and the WATCH
force-cap are GONE. OPS-02 now measures seasonal skew of underperformance:

    HODI = mean monthly ODI-underperformance over the high-wind season.
    SSR  = HODI(high-wind months) / HODI(all months).

Tiers (both conditions, ``>=``): (9.0, 1.30)→CONFIRMED, (6.0, 1.20)→INDICATIVE,
(4.0, 1.10)→WATCH, else None. Requires >=12 months of data; a CONFIRMED result is
no longer downgraded.

Tests are DB-free: the ``DetectionContext`` is built with ``prefetched`` (key
``monthly_performance``) where each row's ODI-underperformance is
``100 - availability_pct``.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops02_performance_seasonality import (
    classify_seasonality_severity,
    compute_hodi_ssr,
    detect,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 101

# Default high-wind season (NO/UK / unknown): Oct–Mar.
HIGH_WIND_MONTHS = (10, 11, 12, 1, 2, 3)


def _ctx(monthly=None):
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={"monthly_performance": monthly, "ppa_info": {}},
    )


def _rows(month_underperf):
    """Build monthly_performance rows from a list of (month_key, underperf_pct).

    ``availability_pct = 100 - underperf_pct`` (the accessor's stored shape).
    """
    return [{"month": m, "availability_pct": 100.0 - underperf} for m, underperf in month_underperf]


def _full_year(high_underperf, low_underperf):
    """Build a 12-row year: 6 high-wind months + 6 low-wind months.

    ``high_underperf`` / ``low_underperf`` are 6-element lists of
    ODI-underperformance percentages for Oct–Mar and Apr–Sep respectively.
    """
    high_keys = ["2024-10", "2024-11", "2024-12", "2025-01", "2025-02", "2025-03"]
    low_keys = ["2024-04", "2024-05", "2024-06", "2024-07", "2024-08", "2024-09"]
    pairs = list(zip(high_keys, high_underperf)) + list(zip(low_keys, low_underperf))
    return _rows(pairs)


# ─────────────────────────── Pure-helper tests ──────────────────────────────


def test_hodi_is_mean_underperf_over_high_wind_months():
    """Oct–Mar ODI-underperf [10,8,12,6,9,7] → HODI = 8.667 (mean)."""
    rows = _full_year([10, 8, 12, 6, 9, 7], [0, 0, 0, 0, 0, 0])
    hodi, _ssr = compute_hodi_ssr(rows, HIGH_WIND_MONTHS)
    assert hodi == pytest.approx(8.667, abs=1e-3)


def test_ssr_is_highwind_over_allyear():
    """HODI_high = 9, HODI_all = 6 → SSR = 1.5.

    6 high-wind months at 9% underperf and 6 low-wind months at 3% underperf:
    HODI_high = 9, HODI_all = (9+3)/2 = 6 → SSR = 9/6 = 1.5.
    """
    rows = _full_year([9, 9, 9, 9, 9, 9], [3, 3, 3, 3, 3, 3])
    hodi, ssr = compute_hodi_ssr(rows, HIGH_WIND_MONTHS)
    assert hodi == pytest.approx(9.0, abs=1e-3)
    assert ssr == pytest.approx(1.5, abs=1e-3)


def test_severity_confirmed_boundary():
    """(9.0, 1.30)→CONFIRMED, (8.99, 1.30)→INDICATIVE, (9.0, 1.29)→INDICATIVE."""
    assert classify_seasonality_severity(9.0, 1.30) == Severity.CONFIRMED
    assert classify_seasonality_severity(8.99, 1.30) == Severity.INDICATIVE
    assert classify_seasonality_severity(9.0, 1.29) == Severity.INDICATIVE


def test_severity_indicative_and_watch_boundaries():
    """(6.0, 1.20)→INDICATIVE, (4.0, 1.10)→WATCH, (3.99, 1.10)→None."""
    assert classify_seasonality_severity(6.0, 1.20) == Severity.INDICATIVE
    assert classify_seasonality_severity(4.0, 1.10) == Severity.WATCH
    assert classify_seasonality_severity(3.99, 1.10) is None


# ─────────────────────────── detect() integration ───────────────────────────


@pytest.mark.asyncio
async def test_requires_min_12_months_else_none():
    """Fewer than 12 monthly observations → no finding (compute returns None)."""
    # 11 rows, all heavily skewed — would be CONFIRMED with 12 months, but the
    # <12-month guard fires first.
    rows = _rows([(f"2024-{m:02d}", 12.0) for m in range(1, 12)])  # Jan–Nov 2024 = 11 months
    assert len(rows) == 11
    hodi, ssr = compute_hodi_ssr(rows, HIGH_WIND_MONTHS)
    assert hodi is None and ssr is None
    assert await detect(_ctx(monthly=rows)) is None
    # No monthly data at all → None too.
    assert await detect(_ctx(monthly=None)) is None
    assert await detect(_ctx(monthly=[])) is None


@pytest.mark.asyncio
async def test_no_force_downgrade_to_watch():
    """A CONFIRMED-eligible HODI+SSR stays CONFIRMED (no WATCH force-cap).

    6 high-wind months at 12% underperf, 6 low-wind months at 2% underperf:
    HODI = 12, HODI_all = 7 → SSR ≈ 1.714 → (>=9.0, >=1.30) → CONFIRMED. The old
    detector would have capped this to WATCH; #96 leaves it CONFIRMED.
    """
    rows = _full_year([12, 12, 12, 12, 12, 12], [2, 2, 2, 2, 2, 2])
    result = await detect(_ctx(monthly=rows))
    assert result is not None
    assert result.schema_code == SchemaCode.OPS_02
    assert result.severity == Severity.CONFIRMED
    assert result.branch == "C"
    assert "wind_resource_index_monthly" in result.missing_slots
    assert result.data_slots["hodi_pct"] == pytest.approx(12.0, abs=1e-3)
    assert result.data_slots["ssr"] == pytest.approx(1.714, abs=1e-3)


@pytest.mark.asyncio
async def test_subthreshold_skew_does_not_fire():
    """Underperformance evenly spread across the year (SSR ≈ 1.0) → no finding."""
    rows = _full_year([5, 5, 5, 5, 5, 5], [5, 5, 5, 5, 5, 5])
    # HODI = 5 (>= WATCH floor) but SSR = 1.0 (< 1.10) → None.
    assert await detect(_ctx(monthly=rows)) is None
