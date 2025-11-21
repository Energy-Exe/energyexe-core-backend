"""Schemas for windfarm performance reports."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class PeerGroupInfo(BaseModel):
    """Information about a peer group."""
    group_type: str = Field(..., description="Type of peer group: bidzone, country, owner, turbine")
    group_id: int = Field(..., description="ID of the group (bidzone_id, country_id, etc)")
    group_name: str = Field(..., description="Name of the group")
    group_code: Optional[str] = Field(None, description="Code of the group")
    total_windfarms: int = Field(..., description="Total number of windfarms in peer group")


class BoxPlotData(BaseModel):
    """Statistical box plot data."""
    group: str = Field(..., description="Group name (e.g., 'Tellenes', 'NO2 Average')")
    min: float = Field(..., description="Lower whisker value")
    q1: float = Field(..., description="First quartile (25th percentile)")
    median: float = Field(..., description="Median (50th percentile)")
    q3: float = Field(..., description="Third quartile (75th percentile)")
    max: float = Field(..., description="Upper whisker value")
    outliers: List[float] = Field(default_factory=list, description="Outlier values")
    mean: Optional[float] = Field(None, description="Mean value")
    std_dev: Optional[float] = Field(None, description="Standard deviation")


class TimeseriesDataPoint(BaseModel):
    """Single data point in a timeseries."""
    date: str = Field(..., description="Date label (e.g., '2024-01')")
    value: float = Field(..., description="Value (e.g., capacity factor %)")


class PeerComparisonTimeseries(BaseModel):
    """Timeseries comparison data with peer group."""
    target_name: str
    target_data: List[TimeseriesDataPoint]
    peer_group_name: str
    peer_average_data: List[TimeseriesDataPoint]
    peer_min_data: List[TimeseriesDataPoint]
    peer_max_data: List[TimeseriesDataPoint]


class RankingRow(BaseModel):
    """Single row in rankings table."""
    rank: int
    windfarm_id: int
    windfarm_name: str
    windfarm_code: str
    avg_capacity_factor: float = Field(..., description="Average capacity factor as percentage")
    bidzone_code: Optional[str] = None
    country_code: Optional[str] = None
    monthly_trend: List[float] = Field(default_factory=list, description="Monthly CF values for sparkline")
    total_generation_gwh: Optional[float] = None


class WindfarmRankings(BaseModel):
    """Rankings of windfarm within various peer groups."""
    bidzone_rank: Optional[int] = Field(None, description="Rank within bidzone")
    total_in_bidzone: Optional[int] = Field(None, description="Total windfarms in bidzone")
    country_rank: int = Field(..., description="Rank within country")
    total_in_country: int = Field(..., description="Total windfarms in country")
    owner_rank: Optional[int] = Field(None, description="Rank within owner's portfolio")
    total_in_owner: Optional[int] = Field(None, description="Total windfarms owned by same owner")
    turbine_rank: Optional[int] = Field(None, description="Rank among same turbine model")
    total_in_turbine: Optional[int] = Field(None, description="Total windfarms with same turbine")

    # Tables for display
    bidzone_table: List[RankingRow] = Field(default_factory=list)
    country_table: List[RankingRow] = Field(default_factory=list)
    owner_table: List[RankingRow] = Field(default_factory=list)
    turbine_table: List[RankingRow] = Field(default_factory=list)


class PeerComparisonData(BaseModel):
    """Complete peer comparison data for one peer group."""
    peer_group_info: PeerGroupInfo
    timeseries: PeerComparisonTimeseries
    distribution: List[BoxPlotData]
    heatmap_matrix: List[List[float]] = Field(..., description="2D matrix: [windfarms x months]")
    heatmap_windfarm_names: List[str] = Field(..., description="Y-axis labels")
    heatmap_month_labels: List[str] = Field(..., description="X-axis labels")
    target_heatmap_index: int = Field(..., description="Index of target windfarm in heatmap")


class TurbineModelInfo(BaseModel):
    """Turbine model information."""
    model: str = Field(..., description="Turbine model name")
    manufacturer: str = Field(..., description="Turbine manufacturer")
    count: int = Field(..., description="Number of turbines")
    rated_capacity_mw: float = Field(..., description="Rated capacity per turbine in MW")
    hub_height_m: Optional[float] = Field(None, description="Hub height in meters")
    rotor_diameter_m: Optional[float] = Field(None, description="Rotor diameter in meters")


class MonthlyTimeseries(BaseModel):
    """Monthly timeseries data point."""
    month: str = Field(..., description="Month in YYYY-MM format")
    value: float = Field(..., description="Value for that month")


class AnnualSummaryRow(BaseModel):
    """Annual summary statistics."""
    year: int = Field(..., description="Year")
    installed_capacity_mw: float = Field(..., description="Installed capacity in MW")
    total_generation_gwh: float = Field(..., description="Total generation in GWh")
    avg_monthly_generation_gwh: float = Field(..., description="Average monthly generation in GWh")
    avg_capacity_factor: float = Field(..., description="Average capacity factor %")
    avg_wind_speed_ms: float = Field(..., description="Average wind speed in m/s")
    median_wind_speed_ms: float = Field(..., description="Median wind speed in m/s")
    avg_wind_direction_deg: float = Field(..., description="Average wind direction in degrees")


class OwnershipHistoryEntry(BaseModel):
    """Ownership history entry."""
    date: str = Field(..., description="Transaction date")
    owner: str = Field(..., description="Owner name")
    transaction_type: str = Field(..., description="Type of transaction")


class AdditionalChartsData(BaseModel):
    """Additional chart data for enhanced visualizations."""
    # Performance charts
    annual_comparison: List[Dict[str, Any]] = Field(default_factory=list, description="Year-over-year data")
    seasonal_patterns: List[Dict[str, Any]] = Field(default_factory=list, description="Quarterly patterns")
    monthly_heatmap: List[Dict[str, Any]] = Field(default_factory=list, description="Monthly heatmap data")
    hourly_generation_profile: List[Dict[str, Any]] = Field(default_factory=list, description="Hourly generation pattern")
    capacity_factor_distribution: List[Dict[str, Any]] = Field(default_factory=list, description="CF histogram data")
    rolling_average: List[Dict[str, Any]] = Field(default_factory=list, description="12-month rolling average")

    # Weather & correlation charts
    power_curve: Dict[str, Any] = Field(default_factory=dict, description="Wind speed vs generation with Gompertz fit")
    wind_rose: List[Dict[str, Any]] = Field(default_factory=list, description="Wind direction distribution")
    wind_speed_heatmap: List[Dict[str, Any]] = Field(default_factory=list, description="Wind speed by hour & month")

    # Simplified report additions
    turbine_model_info: Optional[TurbineModelInfo] = Field(None, description="Turbine model information")
    monthly_generation_timeseries: List[Dict[str, Any]] = Field(default_factory=list, description="Monthly generation timeseries")
    monthly_wind_speed_timeseries: List[Dict[str, Any]] = Field(default_factory=list, description="Monthly wind speed timeseries")
    wind_speed_distribution_weibull: Optional[Dict[str, Any]] = Field(None, description="Wind speed distribution with Weibull fit")
    annual_summary_table: List[AnnualSummaryRow] = Field(default_factory=list, description="Annual summary statistics")
    turbine_model_comparison: List[Dict[str, Any]] = Field(default_factory=list, description="Turbine model comparison across country")
    turbine_size_analysis: List[Dict[str, Any]] = Field(default_factory=list, description="Turbine size vs performance analysis")
    country_context: Optional[Dict[str, Any]] = Field(None, description="Country-level wind generation context")
    all_peers_timeseries: Optional[Dict[int, List[Dict[str, Any]]]] = Field(None, description="All peers monthly timeseries for spaghetti chart")
    ownership_history: List[OwnershipHistoryEntry] = Field(default_factory=list, description="Ownership history")


class PerformanceSummary(BaseModel):
    """Summary statistics for windfarm performance."""
    avg_capacity_factor: float = Field(..., description="Average capacity factor %")
    avg_monthly_generation_gwh: float = Field(..., description="Average monthly generation in GWh")
    total_generation_gwh: float = Field(..., description="Total generation in period")
    max_monthly_cf: float = Field(..., description="Maximum monthly capacity factor %")
    min_monthly_cf: float = Field(..., description="Minimum monthly capacity factor %")
    months_above_peer_average: int = Field(..., description="Number of months above peer average")
    total_months: int = Field(..., description="Total months in analysis period")


class CommentarySection(BaseModel):
    """AI-generated commentary for a report section."""
    section_type: str
    commentary_text: str
    generated_at: Optional[datetime] = None
    word_count: int = 0


class WindfarmReportData(BaseModel):
    """Complete report data for a windfarm."""
    windfarm_id: int
    windfarm_name: str
    windfarm_code: str
    date_range_start: datetime
    date_range_end: datetime

    # Geographic context
    country: Dict[str, Any] = Field(..., description="Country info: id, name, code")
    bidzone: Optional[Dict[str, Any]] = Field(None, description="Bidzone info if applicable")

    # Summary metrics
    summary: PerformanceSummary

    # Rankings
    rankings: WindfarmRankings

    # Peer comparisons by group type
    peer_comparisons: Dict[str, PeerComparisonData] = Field(
        default_factory=dict,
        description="Keyed by peer group type: bidzone, country, owner, turbine"
    )

    # Performance highlights (text summaries)
    highlights: List[str] = Field(default_factory=list)

    # Additional charts (optional)
    additional_charts: Optional[AdditionalChartsData] = None

    # AI-generated commentary (optional)
    commentaries: Dict[str, CommentarySection] = Field(
        default_factory=dict,
        description="AI-generated narrative sections keyed by section_type"
    )


class CapacityFactorDistributionRequest(BaseModel):
    """Request parameters for capacity factor distribution."""
    peer_group: str = Field(..., pattern="^(bidzone|country|owner|turbine)$")
    start_date: datetime
    end_date: datetime


class PeerComparisonRequest(BaseModel):
    """Request parameters for peer comparison timeseries."""
    peer_group: str = Field(..., pattern="^(bidzone|country|owner|turbine)$")
    start_date: datetime
    end_date: datetime
    granularity: str = Field("monthly", pattern="^(daily|weekly|monthly)$")


class ReportGenerationRequest(BaseModel):
    """Request to generate full report."""
    start_date: datetime = Field(..., description="Start date for analysis period")
    end_date: datetime = Field(..., description="End date for analysis period")
    include_peer_groups: Optional[List[str]] = Field(
        None,
        description="List of peer groups to include: bidzone, country, owner, turbine. If None, includes all available"
    )
    selected_sections: Optional[List[str]] = Field(
        None,
        description="List of section IDs to include in report. If None, includes default sections"
    )
    generate_commentary: bool = Field(
        default=False,
        description="Whether to generate AI commentary (requires API key and costs money)"
    )
