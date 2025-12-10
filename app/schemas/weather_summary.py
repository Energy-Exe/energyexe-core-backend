"""Weather summary schemas for historical wind analysis."""
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict


def to_camel(string: str) -> str:
    """Convert snake_case to camelCase."""
    components = string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


# ============================================================================
# DIRECTION HISTOGRAM SCHEMAS
# ============================================================================


class DirectionBin(BaseModel):
    """Single bin in direction histogram (16 compass points)."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    direction: str = Field(description="Compass direction name: N, NNE, NE, etc.")
    degrees: float = Field(description="Center degrees: 0, 22.5, 45, etc.")
    count: int = Field(description="Number of hours in this direction")
    percentage: float = Field(ge=0, le=100, description="Percentage of total hours")


# ============================================================================
# PERIOD SUMMARY SCHEMAS
# ============================================================================


class PeriodSummary(BaseModel):
    """Weather summary for a single period (year or month)."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    year: int = Field(description="Year of the period")
    month: Optional[int] = Field(
        default=None, ge=1, le=12, description="Month (1-12), None for yearly"
    )
    period_label: str = Field(description="Human-readable label: '2024' or 'Jan 2024'")

    # Wind speed statistics (m/s)
    avg_wind_speed_ms: float = Field(description="Average wind speed in m/s")
    min_wind_speed_ms: float = Field(description="Minimum wind speed in m/s")
    max_wind_speed_ms: float = Field(description="Maximum wind speed in m/s")
    std_wind_speed_ms: float = Field(description="Standard deviation of wind speed")

    # Direction analysis
    prevailing_direction_deg: float = Field(
        ge=0, lt=360, description="Vector-averaged direction in degrees"
    )
    prevailing_direction_name: str = Field(description="Compass name: N, NNE, NE, ENE, etc.")
    direction_consistency: float = Field(
        ge=0, le=1, description="Direction consistency (0=random, 1=constant)"
    )
    direction_histogram: List[DirectionBin] = Field(description="16-bin compass histogram")

    # Data quality
    hours_with_data: int = Field(ge=0, description="Number of hours with weather data")
    data_completeness: float = Field(
        ge=0, le=1, description="Ratio of hours with data to total hours in period"
    )


# ============================================================================
# RESPONSE SCHEMAS
# ============================================================================


class WeatherSummaryResponse(BaseModel):
    """Response containing weather summaries for multiple periods."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    windfarm_id: int
    windfarm_name: str
    windfarm_code: str
    period_type: str = Field(description="'monthly' or 'yearly'")
    summaries: List[PeriodSummary] = Field(
        description="List of period summaries, sorted chronologically"
    )
