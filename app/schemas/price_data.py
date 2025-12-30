"""Pydantic schemas for price data."""

from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Literal
from uuid import UUID

from pydantic import BaseModel, Field


# ============================================================
# Request Schemas
# ============================================================

class PriceFetchRequest(BaseModel):
    """Request schema for fetching prices from ENTSOE API."""
    bidzone_codes: List[str] = Field(..., description="List of bidzone codes (e.g., DE_LU, FR)")
    start_date: datetime = Field(..., description="Start datetime (UTC)")
    end_date: datetime = Field(..., description="End datetime (UTC)")
    price_types: Optional[List[str]] = Field(
        default=["day_ahead", "intraday"],
        description="Price types to fetch"
    )


class PriceProcessRequest(BaseModel):
    """Request schema for processing raw prices to windfarm level."""
    windfarm_ids: Optional[List[int]] = Field(None, description="Windfarm IDs to process")
    bidzone_codes: Optional[List[str]] = Field(None, description="Bidzone codes to process")
    start_date: Optional[datetime] = Field(None, description="Start datetime filter")
    end_date: Optional[datetime] = Field(None, description="End datetime filter")
    force_reprocess: bool = Field(False, description="Reprocess even if data exists")


class CaptureRateRequest(BaseModel):
    """Request schema for capture rate calculation."""
    windfarm_id: int = Field(..., description="Windfarm ID")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")
    aggregation: Literal["hour", "day", "week", "month", "year"] = Field(
        "month", description="Time aggregation level"
    )
    price_type: Literal["day_ahead", "intraday"] = Field(
        "day_ahead", description="Price type to use"
    )


class CaptureRateCompareRequest(BaseModel):
    """Request schema for comparing capture rates across windfarms."""
    windfarm_ids: List[int] = Field(..., description="List of windfarm IDs to compare")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")
    aggregation: Literal["hour", "day", "week", "month", "year"] = Field(
        "month", description="Time aggregation level"
    )


class RevenueMetricsRequest(BaseModel):
    """Request schema for revenue metrics calculation."""
    windfarm_id: int = Field(..., description="Windfarm ID")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")
    aggregation: Literal["hour", "day", "week", "month", "year"] = Field(
        "month", description="Time aggregation level"
    )


class PriceProfileRequest(BaseModel):
    """Request schema for price profile analysis."""
    bidzone_id: int = Field(..., description="Bidzone ID")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")
    aggregation: Literal["hour", "day"] = Field(
        "hour", description="Aggregation level"
    )


# ============================================================
# Response Schemas
# ============================================================

class PriceDataRawResponse(BaseModel):
    """Response schema for raw price data record."""
    id: int
    source: str
    source_type: str
    price_type: str
    period_start: datetime
    period_end: Optional[datetime]
    identifier: str
    value_extracted: Optional[Decimal]
    unit: Optional[str]
    currency: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PriceDataResponse(BaseModel):
    """Response schema for processed price data record."""
    id: UUID
    hour: datetime
    windfarm_id: int
    bidzone_id: Optional[int]
    day_ahead_price: Optional[Decimal]
    intraday_price: Optional[Decimal]
    currency: str
    source: str
    quality_flag: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PriceFetchResponse(BaseModel):
    """Response schema for price fetch operation."""
    success: bool
    bidzone_codes: List[str]
    date_range: Dict[str, str]
    price_types: List[str]
    total_records_stored: int
    total_records_updated: int
    by_bidzone: Dict[str, Any]
    errors: List[str]
    duration_seconds: Optional[float] = None


class PriceProcessResponse(BaseModel):
    """Response schema for price processing operation."""
    success: bool
    windfarms_processed: int
    records_created: int
    records_updated: int
    errors: List[str]
    by_windfarm: Dict[str, Any]
    duration_seconds: Optional[float] = None


class BidzoneAvailabilityResponse(BaseModel):
    """Response schema for bidzone price data availability."""
    code: str
    name: str
    earliest_date: Optional[str]
    latest_date: Optional[str]
    record_count: int


class PriceStatisticsResponse(BaseModel):
    """Response schema for price statistics."""
    hours_with_data: int
    day_ahead: Dict[str, Optional[float]]
    intraday: Dict[str, Optional[float]]


class PriceCoverageResponse(BaseModel):
    """Response schema for price data coverage."""
    total_hours: int
    hours_with_data: int
    hours_with_day_ahead: int
    hours_with_intraday: int
    coverage_percent: float
    day_ahead_coverage_percent: float
    intraday_coverage_percent: float


# ============================================================
# Analytics Response Schemas
# ============================================================

class CaptureRatePeriod(BaseModel):
    """Capture rate data for a single period."""
    period: Optional[str]
    total_generation_mwh: float
    revenue_eur: float
    achieved_price: Optional[float]
    market_average_price: Optional[float]
    hours_in_period: int
    capture_rate: Optional[float]


class CaptureRateOverall(BaseModel):
    """Overall capture rate metrics."""
    total_generation_mwh: float
    total_revenue_eur: float
    achieved_price: Optional[float]
    market_average_price: Optional[float]
    capture_rate: Optional[float]


class CaptureRateResponse(BaseModel):
    """Response schema for capture rate calculation."""
    windfarm_id: int
    windfarm_name: Optional[str]
    start_date: str
    end_date: str
    aggregation: str
    price_type: str
    overall: CaptureRateOverall
    periods: List[CaptureRatePeriod]


class WindfarmCaptureRateSummary(BaseModel):
    """Summary of capture rate for a single windfarm in comparison."""
    windfarm_id: int
    windfarm_name: Optional[str]
    overall_capture_rate: Optional[float]
    total_generation_mwh: float
    total_revenue_eur: float


class CaptureRateCompareResponse(BaseModel):
    """Response schema for capture rate comparison."""
    start_date: str
    end_date: str
    aggregation: str
    windfarms: List[WindfarmCaptureRateSummary]


class RevenuePeriod(BaseModel):
    """Revenue data for a single period."""
    period: Optional[str]
    total_generation_mwh: float
    day_ahead_revenue_eur: float
    total_revenue_eur: float
    avg_day_ahead_price: Optional[float]
    avg_intraday_price: Optional[float]
    hours_with_generation: int


class RevenueMetricsResponse(BaseModel):
    """Response schema for revenue metrics."""
    windfarm_id: int
    windfarm_name: Optional[str]
    start_date: str
    end_date: str
    aggregation: str
    periods: List[RevenuePeriod]


class PriceProfileEntry(BaseModel):
    """Price profile entry for an hour or day."""
    hour_of_day: Optional[int] = None
    day_of_week: Optional[int] = None
    day_name: Optional[str] = None
    avg_price: Optional[float]
    min_price: Optional[float]
    max_price: Optional[float]
    stddev: Optional[float]
    sample_count: int


class PriceProfileResponse(BaseModel):
    """Response schema for price profile."""
    bidzone_id: int
    bidzone_code: Optional[str]
    bidzone_name: Optional[str]
    start_date: str
    end_date: str
    aggregation: str
    profile: List[PriceProfileEntry]


class CorrelationResponse(BaseModel):
    """Response schema for generation-price correlation."""
    windfarm_id: int
    windfarm_name: Optional[str]
    start_date: str
    end_date: str
    correlation: Optional[float]
    sample_size: int
    interpretation: Optional[str] = None
    message: Optional[str] = None


# ============================================================
# List Response Schemas
# ============================================================

class PriceDataRawListResponse(BaseModel):
    """Response schema for list of raw price data."""
    items: List[PriceDataRawResponse]
    total: int
    limit: int
    offset: int


class PriceDataListResponse(BaseModel):
    """Response schema for list of processed price data."""
    items: List[PriceDataResponse]
    total: int
    limit: int
    offset: int


class BidzoneListResponse(BaseModel):
    """Response schema for list of bidzones with price data."""
    items: List[BidzoneAvailabilityResponse]
    total: int


# ============================================================
# Price Availability Schemas
# ============================================================

class PriceAvailabilityDayEntry(BaseModel):
    """Daily price data availability entry."""
    bidzones: List[str]
    recordCount: int
    priceTypes: List[str]


class PriceAvailabilitySummary(BaseModel):
    """Summary of price data availability."""
    totalDays: int
    daysWithData: int
    coverage: float
    bidzones: List[str]
    priceTypes: List[str]


class PriceAvailabilityResponse(BaseModel):
    """Response schema for price availability endpoint."""
    availability: Dict[str, PriceAvailabilityDayEntry]
    summary: PriceAvailabilitySummary


# ============================================================
# Fetch Day Schemas
# ============================================================

class PriceFetchDayRequest(BaseModel):
    """Request schema for fetching prices for specific date(s)."""
    dates: List[date] = Field(..., description="List of dates to fetch (YYYY-MM-DD)")
    bidzone_codes: List[str] = Field(..., description="List of bidzone codes (e.g., NO_1, SE_1)")
    price_types: Optional[List[str]] = Field(
        default=["day_ahead"],
        description="Price types to fetch: day_ahead, intraday"
    )


class PriceFetchDayBidzoneResult(BaseModel):
    """Result for a single bidzone fetch operation."""
    bidzone_code: str
    records_stored: int
    records_updated: int
    by_price_type: Dict[str, Dict[str, int]]
    errors: List[str]


class PriceFetchDayDateResult(BaseModel):
    """Result for a single date fetch operation."""
    date: str
    success: bool
    by_bidzone: Dict[str, PriceFetchDayBidzoneResult]
    total_records: int
    errors: List[str]


class PriceFetchDayResponse(BaseModel):
    """Response schema for fetch-day operation."""
    success: bool
    dates_requested: List[str]
    bidzone_codes: List[str]
    price_types: List[str]
    results: List[PriceFetchDayDateResult]
    total_records_stored: int
    total_records_updated: int
    duration_seconds: float
    errors: List[str]
