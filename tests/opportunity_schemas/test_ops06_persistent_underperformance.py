"""OPS-06 detector tests (issue #101) — persistent power-curve underperformance.

OPS-06 reads the Module 4 empirical-P50 normalised index
(``performance_summaries.norm_index_p50``) via ``ctx.load_norm_index_series()`` and
flags windfarms sitting below their own P50 reference for a sustained run (>= 6
consecutive months below 80).

Tiers (require >= 6 consecutive months below 80; strict ``<`` on the index):
    (79, 6) → CONFIRMED   (79, 5) → None   (84, 6) → INDICATIVE
    (89, 6) → WATCH       (90, 6) → None

Data gaps / suppression (→ detect None): 0 / NULL months are dropped upstream as
gaps; < 2 years (24 months) of usable data; no 6-month below-80 run.

All tests are DB-free: the norm-index series is injected via
``DetectionContext(prefetched={"norm_index_series": [...]})``.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops06_persistent_underperformance import (
    classify_underperformance_severity,
    count_consecutive_months_below,
    detect,
)

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 101


def _ctx(series=None):
    """A DB-free DetectionContext with the norm-index series injected."""
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={"norm_index_series": series},
    )


# ─── count_consecutive_months_below (pure) ────────────────────────────────────


def test_consecutive_counter():
    """[95,79,78,77,81,76,75,74,73,72] threshold 80 → longest below-run is 5."""
    series = [95, 79, 78, 77, 81, 76, 75, 74, 73, 72]
    assert count_consecutive_months_below(series, 80) == 5


def test_consecutive_counter_none_breaks_run():
    """A None entry breaks a run (neither below nor extending)."""
    assert count_consecutive_months_below([78, 78, None, 78, 78], 80) == 2
    assert count_consecutive_months_below([95, 96, 97], 80) == 0


# ─── classify_underperformance_severity (pure) ────────────────────────────────


def test_severity_boundaries():
    """(79,6)→CONFIRMED, (79,5)→None, (84,6)→INDICATIVE, (89,6)→WATCH, (90,6)→None."""
    assert classify_underperformance_severity(79, 6) == Severity.CONFIRMED
    assert classify_underperformance_severity(79, 5) is None
    assert classify_underperformance_severity(84, 6) == Severity.INDICATIVE
    assert classify_underperformance_severity(89, 6) == Severity.WATCH
    assert classify_underperformance_severity(90, 6) is None


def test_severity_requires_six_months():
    """Any index, < 6 consecutive months → None regardless of depth."""
    assert classify_underperformance_severity(50, 5) is None
    assert classify_underperformance_severity(50, 0) is None
    assert classify_underperformance_severity(50, 6) == Severity.CONFIRMED


def test_severity_none_when_index_missing():
    """Missing index → None even with a long run."""
    assert classify_underperformance_severity(None, 12) is None


# ─── detect() ─────────────────────────────────────────────────────────────────


def _series_below(low_value, *, low_count, total=24):
    """24-month (>= 2yr) series: `low_count` months at `low_value`, rest at 100."""
    healthy = total - low_count
    return [100.0] * healthy + [float(low_value)] * low_count


@pytest.mark.asyncio
async def test_detect_fires_on_six_month_run():
    """A 6-month run below 80 (over a 2-year series) → CONFIRMED DetectorResult."""
    series = _series_below(75.0, low_count=6, total=24)
    result = await detect(_ctx(series))
    assert result is not None
    assert result.schema_code is SchemaCode.OPS_06
    assert result.severity is Severity.CONFIRMED
    assert result.data_slots["consecutive_months_below_threshold"] == 6
    assert result.data_slots["norm_index_p50"] == 75.0


@pytest.mark.asyncio
async def test_detect_accepts_dict_series():
    """The list-of-dicts shape from load_norm_index_series() is accepted."""
    series = [{"month": f"2024-{i:02d}", "norm_index_p50": 100.0} for i in range(1, 13)]
    series += [{"month": f"2025-{i:02d}", "norm_index_p50": 78.0} for i in range(1, 13)]
    result = await detect(_ctx(series))
    assert result is not None
    assert result.severity is Severity.CONFIRMED  # 12-month below-80 run
    assert result.data_slots["consecutive_months_below_threshold"] == 12


@pytest.mark.asyncio
async def test_detect_none_when_run_too_short():
    """A 5-month below-80 run is not sustained → no finding."""
    series = _series_below(75.0, low_count=5, total=24)
    assert await detect(_ctx(series)) is None


@pytest.mark.asyncio
async def test_detect_suppressed_under_two_years():
    """< 24 usable months → suppressed even with a clear 6-month run."""
    series = [100.0] * 6 + [75.0] * 6  # only 12 months
    assert await detect(_ctx(series)) is None


@pytest.mark.asyncio
async def test_detect_none_when_no_data():
    """No series injected → detect returns None."""
    assert await detect(_ctx(None)) is None
    assert await detect(_ctx([])) is None
