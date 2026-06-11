from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


# --- CRUD schemas ---


class P50TargetCreate(BaseModel):
    """Schema for creating a new P50 target."""

    p50_target_start_date: Optional[date] = None  # Default computed from windfarm COD if not provided
    p50_target_end_date: Optional[date] = None  # None = ongoing
    p50_target_volume_gwh: Decimal = Field(..., gt=0, decimal_places=3)
    source: Optional[str] = Field(None, max_length=500)
    comment: Optional[str] = None


class P50TargetUpdate(BaseModel):
    """Schema for updating a P50 target. All fields are optional."""

    p50_target_start_date: Optional[date] = None
    p50_target_end_date: Optional[date] = None
    p50_target_volume_gwh: Optional[Decimal] = Field(None, gt=0, decimal_places=3)
    source: Optional[str] = Field(None, max_length=500)
    comment: Optional[str] = None


class P50TargetResponse(BaseModel):
    """Schema for P50 target response with computed monthly value."""

    id: int
    windfarm_id: int
    p50_target_start_date: date
    p50_target_end_date: Optional[date]
    p50_target_volume_gwh: float
    monthly_p50_gwh: float  # = annual / 12 (computed)
    source: Optional[str]
    comment: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# --- Analysis schemas ---


class P50MonthlyDataPoint(BaseModel):
    """A single month in the P50 cumulative timeseries."""

    month: str  # "YYYY-MM"
    monthly_p50_gwh: float  # Annual P50 / 12
    actual_generation_gwh: float  # Actual generation for this month
    aggregated_p50_gwh: float  # Cumulative P50 from start
    aggregated_actual_gwh: float  # Cumulative actual from start
    aggregated_gap_gwh: float  # aggregated_actual - aggregated_p50 (positive = above target)


class P50YearlyGap(BaseModel):
    """Per-year P50 gap breakdown."""

    year: int
    actual_generation_gwh: float
    p50_target_gwh: float  # Prorated if partial year
    gap_gwh: float  # actual - p50 (positive = overperforming)
    gap_months: float  # gap / monthly_p50


class P50AnalysisResult(BaseModel):
    """Complete P50 analysis result for a windfarm."""

    windfarm_id: int
    windfarm_name: str
    installed_capacity_mw: Optional[float]
    p50_target: P50TargetResponse
    p50_capacity_factor_pct: Optional[float]  # P50 / (capacity_mw * 8760) * 100
    avg_annual_generation_gwh: float  # Average actual annual gen (excl first year after COD)
    avg_annual_gap_gwh: float  # Average (actual - P50) per year
    gap_from_p50_gwh: float  # Total cumulative gap (actual - P50; positive = above target)
    gap_pct_of_annual_avg: Optional[float]  # gap / avg_annual_generation * 100
    gap_in_months: float  # total gap / monthly P50 (positive = months ahead of target)
    monthly_data: List[P50MonthlyDataPoint]
    yearly_gaps: List[P50YearlyGap]
