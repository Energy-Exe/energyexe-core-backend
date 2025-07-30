"""Pydantic schemas for ENTSOE data."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class GenerationDataPoint(BaseModel):
    """Schema for a single generation data point."""

    timestamp: datetime
    value: float = Field(..., description="Generation value in MW")
    production_type: str = Field(..., description="Type of production (wind, solar)")
    area_code: str = Field(..., description="Area/bidding zone code")


class GenerationDataRequest(BaseModel):
    """Schema for generation data request."""

    start_date: datetime = Field(..., description="Start date for data query")
    end_date: datetime = Field(..., description="End date for data query")
    area_codes: List[str] = Field(..., description="List of area codes to query")
    production_types: List[str] = Field(
        default=["wind", "solar"], description="List of production types to query"
    )


class AreaMetadata(BaseModel):
    """Metadata for a single area's fetch results."""

    success: bool
    records: int
    errors: List[str]


class GenerationDataMetadata(BaseModel):
    """Metadata for generation data response."""

    fetch_id: int
    areas: Dict[str, AreaMetadata]
    total_records: int
    errors: List[Dict[str, Any]]


class GenerationDataResponse(BaseModel):
    """Schema for generation data response."""

    data: List[Dict[str, Any]] = Field(..., description="Generation data points")
    metadata: GenerationDataMetadata


class FetchHistoryResponse(BaseModel):
    """Schema for fetch history response."""

    id: int
    request_type: str = Field(..., pattern="^(real_time|historical_batch)$")
    start_datetime: datetime
    end_datetime: datetime
    area_code: str
    production_type: str
    status: str = Field(..., pattern="^(pending|success|failed|partial)$")
    records_fetched: int
    error_message: Optional[str] = None
    response_time_ms: Optional[int] = None
    requested_by_user_id: Optional[int] = None
    created_at: datetime
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class AreaCodeResponse(BaseModel):
    """Schema for area code information."""

    code: str = Field(..., description="Area/bidding zone code")
    name: str = Field(..., description="Human-readable area name")
