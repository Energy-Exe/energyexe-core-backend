"""Pydantic schemas for the wind-farm map page (client-ui #44).

The map needs per-windfarm performance scores binned into 5 buckets,
peer-benchmarked against the windfarm's bidzone. This is computed by
`MapPerformanceService` over the existing `performance_summaries`,
`generation_concentration_summaries`, and `peer_group_aggregates` tables.
"""

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class MapPerformanceScore(BaseModel):
    """Per-windfarm performance score for one period, two views (commercial + generation)."""

    windfarm_id: int
    bidzone_id: Optional[int] = None
    bidzone_code: Optional[str] = None
    country_code: Optional[str] = None

    # Commercial view: capture_ratio from generation_concentration_summaries
    commercial_value: Optional[float] = None
    commercial_bucket: Optional[int] = Field(
        None,
        ge=1,
        le=5,
        description="1=underperf, 2=below benchmark, 3=on benchmark, 4=above benchmark, 5=outperf",
    )
    has_commercial_data: bool = False

    # Generation view: norm_index_p50 from performance_summaries
    generation_value: Optional[float] = None
    generation_bucket: Optional[int] = Field(None, ge=1, le=5)
    has_generation_data: bool = False

    period_type: str
    period_year: int
    period_month: Optional[int] = None


class MapCoverage(BaseModel):
    """Coverage indicators for the asymmetric NO/UK warning (spec §4.1)."""

    total_count: int
    commercial_count: int
    generation_count: int
    no_count: int
    no_with_generation_data: int
    no_coverage_pct: float
    uk_count: int
    uk_with_generation_data: int
    uk_coverage_pct: float
    asymmetric: bool = Field(
        False, description="True when |no_coverage_pct - uk_coverage_pct| >= 0.15"
    )


class MapPerformanceScoresResponse(BaseModel):
    """Top-level response for the map performance-scores endpoint."""

    period_type: str
    period_year: int
    period_month: Optional[int] = None
    scores: List[MapPerformanceScore]
    coverage: MapCoverage


class MapFinancialMetric(BaseModel):
    """Per-windfarm financial ratios for one period (spec §5 lower tier)."""

    windfarm_id: int
    has_data: bool = False
    ebitda_margin: Optional[float] = None
    revenue_per_mwh: Optional[float] = None
    opex_per_mwh: Optional[float] = None
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    currency: Optional[str] = None


class MapFinancialMetricsResponse(BaseModel):
    """Top-level response for the map financial-metrics endpoint."""

    period_type: str
    period_year: int
    metrics: List[MapFinancialMetric]
    total_count: int
    with_data_count: int


class MapStateFilter(BaseModel):
    """Subset of map filter state — used as part of the AI interpretation payload."""

    countries: Optional[List[str]] = None
    types: Optional[List[str]] = None
    zones: Optional[List[str]] = None
    statuses: Optional[List[str]] = None
    capacity_min: Optional[float] = None
    capacity_max: Optional[float] = None


class MapStatePayload(BaseModel):
    """Snapshot of the current map view used to prompt the brain agent."""

    windfarm_ids: List[int]
    view: str = Field("generation", description="commercial | generation | financial")
    color_by_metric: Optional[str] = None
    period_type: str = "year"
    period_year: int
    period_month: Optional[int] = None
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    filters: Optional[MapStateFilter] = None
    portfolio_id: Optional[int] = None
