"""Weather data schemas."""
from datetime import datetime, date
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict


def to_camel(string: str) -> str:
    """Convert snake_case to camelCase."""
    components = string.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


# ============================================================================
# AVAILABILITY CALENDAR SCHEMAS
# ============================================================================

class DateAvailability(BaseModel):
    """Weather data availability for a specific date."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    date: date
    has_data: bool
    record_count: int
    expected_count: int = Field(default=38184, description="1,591 windfarms × 24 hours")
    is_complete: bool
    completion_percentage: float = Field(ge=0, le=100)
    windfarm_count: int


class WeatherFetchRequest(BaseModel):
    """Request to fetch weather data for a specific date."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    date: date
    force_refetch: bool = Field(default=False, description="Re-fetch even if data exists")


class WeatherFetchJobResponse(BaseModel):
    """Response for weather fetch job."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    job_id: str
    date: date
    status: str = Field(description="queued, running, completed, failed")
    message: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


# ============================================================================
# BASIC WEATHER DATA SCHEMAS
# ============================================================================

class WeatherDataPoint(BaseModel):
    """Single weather data point."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    hour: datetime
    windfarm_id: int
    wind_speed_100m: float
    wind_direction_deg: float
    temperature_2m_c: float


class WeatherTimeseries(BaseModel):
    """Weather time series data."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    timestamps: List[datetime]
    wind_speed: List[float]
    wind_direction: List[float]
    temperature: List[float]
    aggregation: str = Field(description="hourly, daily, monthly")


# ============================================================================
# WIND ROSE SCHEMAS
# ============================================================================

class WindRoseBin(BaseModel):
    """Single bin in wind rose."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    direction_bin: float  # 0, 22.5, 45, ..., 337.5
    speed_bin: str  # "0-5", "5-10", "10-15", "15-20", "20+"
    frequency: int
    percentage: float


class WindRoseData(BaseModel):
    """Complete wind rose data."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    direction_bins: List[float] = Field(description="16 compass points")
    speed_bins: List[dict] = Field(description="Speed ranges")
    frequency: List[List[float]] = Field(description="2D array [direction][speed]")
    total_hours: int
    calm_percentage: float = Field(description="% of time with wind < 0.5 m/s")


# ============================================================================
# WIND SPEED DISTRIBUTION SCHEMAS
# ============================================================================

class WindSpeedDistribution(BaseModel):
    """Wind speed distribution with Weibull fit."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    speed_bins: List[float]
    frequency: List[int]
    frequency_percentage: List[float]
    weibull_k: float = Field(description="Shape parameter (consistency)")
    weibull_c: float = Field(description="Scale parameter")
    weibull_fit: List[float] = Field(description="Fitted probability curve")
    mean_speed: float
    median_speed: float
    mode_speed: float
    std_dev: float


# ============================================================================
# PATTERN SCHEMAS
# ============================================================================

class DiurnalPattern(BaseModel):
    """Average wind pattern by hour of day."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    hours: List[int]  # 0-23
    avg_wind_speed: List[float]
    min_wind_speed: List[float]
    max_wind_speed: List[float]
    median_wind_speed: List[float]
    std_dev: List[float]


class SeasonalPattern(BaseModel):
    """Average wind pattern by month."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    months: List[str]  # "Jan", "Feb", ...
    month_numbers: List[int]  # 1-12
    avg_wind_speed: List[float]
    min_wind_speed: List[float]
    max_wind_speed: List[float]
    avg_temperature: List[float]


# ============================================================================
# STATISTICS SCHEMAS
# ============================================================================

class WindStatistics(BaseModel):
    """Comprehensive wind statistics."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    mean_speed: float
    median_speed: float
    mode_speed: Optional[float]
    p10_speed: float = Field(description="10th percentile")
    p50_speed: float = Field(description="50th percentile (median)")
    p90_speed: float = Field(description="90th percentile")
    max_speed: float
    min_speed: float
    std_dev: float
    variance: float

    # Temperature
    mean_temperature: float
    max_temperature: float
    min_temperature: float

    # Wind direction
    prevailing_direction: float = Field(description="Most common direction in degrees")
    prevailing_direction_name: str = Field(description="N, NE, E, SE, S, SW, W, NW")

    # Capacity estimation
    capacity_factor_estimate: float = Field(description="Based on wind distribution")
    total_hours: int
    calm_hours: int = Field(description="Hours with wind < 3 m/s")
    calm_percentage: float


# ============================================================================
# CORRELATION SCHEMAS
# ============================================================================

class CorrelationData(BaseModel):
    """Wind speed vs generation correlation."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    wind_speed_bins: List[float]
    avg_generation_mw: List[float]
    min_generation_mw: List[float]
    max_generation_mw: List[float]
    std_dev_generation: List[float]
    record_count: List[int]
    correlation_coefficient: float
    r_squared: float


class PowerCurvePoint(BaseModel):
    """Single point on power curve."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    wind_speed: float
    avg_generation: float
    sample_count: int
    std_dev: float


class PowerCurveData(BaseModel):
    """Power curve data (wind speed vs generation)."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    wind_speed: List[float]
    generation_mw: List[float]
    sample_count: List[int]
    std_dev: List[float]

    # Curve parameters
    cut_in_speed: Optional[float] = Field(description="Wind speed where generation starts")
    rated_speed: Optional[float] = Field(description="Wind speed at max power")
    cut_out_speed: Optional[float] = Field(description="Wind speed where turbine stops")
    rated_power: Optional[float] = Field(description="Maximum power output")

    # Fit quality
    correlation_coefficient: float
    r_squared: float


class CapacityFactorBin(BaseModel):
    """Capacity factor for a wind speed bin."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    wind_speed_bin: str
    wind_speed_center: float
    capacity_factor: float
    frequency: float = Field(description="% of time in this bin")
    generation_contribution: float = Field(description="% of total generation")


class CapacityFactorData(BaseModel):
    """Capacity factor analysis by wind speed."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    wind_speed_bins: List[str]
    wind_speed_centers: List[float]
    capacity_factors: List[float]
    frequencies: List[float]
    generation_contributions: List[float]
    overall_capacity_factor: float


# ============================================================================
# HEATMAP SCHEMAS
# ============================================================================

class HeatmapData(BaseModel):
    """Hour × Month heatmap data."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    hours: List[int]  # 0-23
    months: List[str]  # "Jan", "Feb", ...
    values: List[List[float]] = Field(description="2D array [hour][month]")
    metric: str = Field(description="wind_speed, temperature, or generation")
    unit: str


# ============================================================================
# ADVANCED ANALYTICS SCHEMAS
# ============================================================================

class WindSpeedDurationCurve(BaseModel):
    """Wind speed duration curve data."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    hours: List[int]  # 0 to 8760
    wind_speed: List[float]  # Sorted descending
    cumulative_percentage: List[float]


class EnergyRoseData(BaseModel):
    """Energy rose - generation by direction."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    direction_bins: List[float]
    generation_by_direction: List[float]
    percentage_by_direction: List[float]
    frequency_by_direction: List[float]


class TemperatureImpactData(BaseModel):
    """Temperature impact on generation at constant wind speed."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    reference_wind_speed: float
    temperature_bins: List[float]
    avg_generation: List[float]
    sample_count: List[int]
    impact_percentage: float = Field(description="Generation change per 10°C")


# ============================================================================
# COMPARATIVE ANALYSIS SCHEMAS
# ============================================================================

class WindfarmComparisonData(BaseModel):
    """Compare multiple windfarms."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    windfarm_ids: List[int]
    windfarm_names: List[str]
    mean_wind_speeds: List[float]
    p90_speeds: List[float]
    capacity_factors: List[float]
    correlation_coefficients: List[float]


# ============================================================================
# EXPORT SCHEMAS
# ============================================================================

class WeatherDataExportRequest(BaseModel):
    """Request to export weather data."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    windfarm_id: int
    start_date: datetime
    end_date: datetime
    format: str = Field(description="csv, excel, pdf")
    include_charts: bool = Field(default=False, description="Include chart images")


class WeatherDataExportResponse(BaseModel):
    """Response with export file."""
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    file_url: str
    file_size_bytes: int
    expires_at: datetime
