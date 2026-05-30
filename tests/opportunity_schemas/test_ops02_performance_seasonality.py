"""OPS-02 detector tests (issue #92) — verbatim reproduction of legacy behaviour.

DB-free ``DetectionContext`` built via ``prefetched`` (keys ``seasonal_capture``
/ ``ppa_info``) matching the #91 snapshot. Asserts the migrated detector
reproduces the legacy outcome verbatim, INCLUDING the OPS-02 WATCH-cap bug
(CONFIRMED-eligible gaps forced to WATCH) and the impossible-inversion firing
condition.
"""

from datetime import datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.ops02_performance_seasonality import detect

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WF_ID = 101


def _ctx(seasonal=None, ppa=None):
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={
            "seasonal_capture": seasonal,
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
async def test_confirmed_eligible_gap_is_capped_to_watch():
    """BUG-DEMONSTRATING: gap=15pp over 2 years → determine=CONFIRMED, but
    wind_resource_index_monthly is always missing → severity capped to WATCH
    (the #96 rewrite removes this). Matches 'ops02_should_be_confirmed_is_watch'.
    """
    seasonal = {"high_wind_cf": 0.30, "low_wind_cf": 0.45, "years_with_inversion": 2}
    result = await detect(_ctx(seasonal=seasonal, ppa={}))
    assert result is not None
    assert _shape(result) == (
        SchemaCode.OPS_02,
        Severity.WATCH,  # capped from CONFIRMED — the preserved bug
        "C",
        (
            "cannibalisation_index_seasonal",
            "maintenance_calendar",
            "revenue_uplift_potential_eur",
            "turbine_scatter_spread",
            "wind_resource_index_monthly",
        ),
        (
            "high_wind_season_capture",
            "low_wind_season_capture",
            "period",
            "seasonal_gap_pp",
            "years_with_inversion",
        ),
    )


@pytest.mark.asyncio
async def test_normal_farm_without_inversion_does_not_fire():
    """A normal farm (high-wind CF > low-wind CF → gap_pp <= 0) produces NO OPS-02
    row — the structurally-impossible inversion firing condition is preserved."""
    seasonal = {"high_wind_cf": 0.45, "low_wind_cf": 0.30, "years_with_inversion": 0}
    assert await detect(_ctx(seasonal=seasonal, ppa={})) is None


@pytest.mark.asyncio
async def test_detect_returns_none_when_no_finding():
    """No seasonal data, or a missing CF, or a sub-threshold gap → no finding."""
    # No seasonal data at all.
    assert await detect(_ctx(seasonal=None, ppa={})) is None
    # Missing one of the CFs.
    assert (
        await detect(
            _ctx(seasonal={"high_wind_cf": 0.30, "low_wind_cf": None, "years_with_inversion": 1})
        )
        is None
    )
    # Inversion present but gap below the WATCH-tier marginal threshold (4pp) and
    # zero years observed → determine_ops02_severity returns None.
    assert (
        await detect(
            _ctx(seasonal={"high_wind_cf": 0.40, "low_wind_cf": 0.42, "years_with_inversion": 0})
        )
        is None
    )
