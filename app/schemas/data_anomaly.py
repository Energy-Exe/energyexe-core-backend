"""Pydantic schemas for data anomalies."""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator


class DataAnomalyBase(BaseModel):
    """Base schema for data anomaly."""

    anomaly_type: str = Field(..., description="Type of anomaly detected")
    severity: str = Field(default="medium", description="Severity level: low, medium, high, critical")
    status: str = Field(default="pending", description="Status: pending, investigating, resolved, ignored, false_positive")
    windfarm_id: Optional[int] = Field(None, description="Related windfarm ID")
    generation_unit_id: Optional[int] = Field(None, description="Related generation unit ID")
    period_start: datetime = Field(..., description="Start of affected period")
    period_end: datetime = Field(..., description="End of affected period")
    description: Optional[str] = Field(None, description="Human-readable description")
    anomaly_metadata: Optional[Dict[str, Any]] = Field(None, description="Additional anomaly-specific data")


class DataAnomalyCreate(DataAnomalyBase):
    """Schema for creating a data anomaly."""
    pass


class DataAnomalyUpdate(BaseModel):
    """Schema for updating a data anomaly."""

    status: Optional[str] = None
    severity: Optional[str] = None
    description: Optional[str] = None
    resolution_notes: Optional[str] = None
    anomaly_metadata: Optional[Dict[str, Any]] = None


class DataAnomalyStatusUpdate(BaseModel):
    """Schema for updating anomaly status."""

    status: str = Field(..., description="New status: pending, investigating, resolved, ignored, false_positive")
    resolution_notes: Optional[str] = Field(None, description="Notes about the resolution")


class DataAnomalyResponse(DataAnomalyBase):
    """Schema for data anomaly response (saved in database)."""

    id: int
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[int] = None
    resolution_notes: Optional[str] = None
    is_active: bool
    detected_at: datetime
    created_at: datetime
    updated_at: datetime

    # Related entities
    windfarm_name: Optional[str] = None
    generation_unit_name: Optional[str] = None

    class Config:
        from_attributes = True


class DataAnomalyDetectionResult(DataAnomalyBase):
    """Schema for anomaly detection results (not yet saved to database)."""

    id: Optional[int] = None
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[int] = None
    resolution_notes: Optional[str] = None
    is_active: bool = True
    detected_at: datetime
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Related entities
    windfarm_name: Optional[str] = None
    generation_unit_name: Optional[str] = None


class AnomalyDetectionRequest(BaseModel):
    """Schema for requesting anomaly detection."""

    windfarm_ids: Optional[List[int]] = Field(None, description="List of windfarm IDs to check, None for all")
    start_date: Optional[datetime] = Field(None, description="Start date for detection range")
    end_date: Optional[datetime] = Field(None, description="End date for detection range")
    anomaly_types: Optional[List[str]] = Field(
        None,
        description="Specific anomaly types to detect. If None, detects all types"
    )
    capacity_factor_threshold: float = Field(
        default=1.2,
        description="Threshold for capacity factor anomalies (default 1.2 = 120%)"
    )

    @field_validator('capacity_factor_threshold')
    @classmethod
    def validate_threshold(cls, v):
        """Validate capacity factor threshold is reasonable."""
        if v <= 0 or v > 10:
            raise ValueError('Capacity factor threshold must be between 0 and 10')
        return v


class AnomalyDetectionResponse(BaseModel):
    """Schema for anomaly detection response."""

    anomalies_detected: int
    anomalies_created: int
    anomalies: List[DataAnomalyDetectionResult]
    detection_summary: Dict[str, Any]


class ReaggregationRequest(BaseModel):
    """Schema for requesting data re-aggregation."""

    windfarm_id: Optional[int] = Field(None, description="Windfarm ID for re-aggregation")
    generation_unit_id: Optional[int] = Field(None, description="Generation unit ID for re-aggregation")
    start_date: datetime = Field(..., description="Start date for re-aggregation")
    end_date: datetime = Field(..., description="End date for re-aggregation")
    source: Optional[str] = Field(None, description="Specific source to re-aggregate (ENTSOE, ELEXON, etc.)")
    sources: Optional[List[str]] = Field(None, description="Multiple sources to re-aggregate")

    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        """Validate end date is after start date."""
        if 'start_date' in info.data and v < info.data['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class ReaggregationResponse(BaseModel):
    """Schema for re-aggregation response."""

    success: bool
    message: str
    records_processed: int
    records_created: int
    period_start: datetime
    period_end: datetime
    sources_processed: List[str]
    errors: Optional[List[str]] = None


class AnomalyListFilters(BaseModel):
    """Schema for filtering anomaly list."""

    windfarm_id: Optional[int] = None
    generation_unit_id: Optional[int] = None
    anomaly_type: Optional[str] = None
    status: Optional[str] = None
    severity: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    is_active: Optional[bool] = True
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=200)


class AnomalyListResponse(BaseModel):
    """Schema for paginated anomaly list response."""

    anomalies: List[DataAnomalyResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
