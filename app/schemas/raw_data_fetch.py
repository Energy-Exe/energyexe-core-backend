"""Schemas for raw data fetching from external APIs."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class RawDataFetchRequest(BaseModel):
    """Request to fetch raw data from external API."""

    windfarm_ids: List[int] = Field(..., description="List of windfarm IDs to fetch data for")
    start_date: datetime = Field(..., description="Start date for data fetch")
    end_date: datetime = Field(..., description="End date for data fetch")


class UnifiedRawDataFetchRequest(BaseModel):
    """Request to fetch raw data for windfarms (auto-detects sources)."""

    windfarm_ids: Optional[List[int]] = Field(
        None,
        description="List of windfarm IDs to fetch data for. If not provided, must specify source."
    )
    source: Optional[str] = Field(
        None,
        description="Source to fetch all windfarms for (ENTSOE, ELEXON, EIA, TAIPOWER). If provided without windfarm_ids, fetches all windfarms for this source."
    )
    start_date: datetime = Field(..., description="Start date for data fetch")
    end_date: datetime = Field(..., description="End date for data fetch")

    class Config:
        json_schema_extra = {
            "example": {
                "source": "ENTSOE",
                "start_date": "2025-10-18T00:00:00Z",
                "end_date": "2025-10-19T23:59:59Z"
            }
        }


class GenerationUnitSummary(BaseModel):
    """Summary of generation unit processing."""

    id: int
    code: str
    name: str
    records_stored: int
    records_updated: int


class RawDataFetchResponse(BaseModel):
    """Response from raw data fetch operation."""

    success: bool
    source: str
    windfarm_ids: List[int]
    windfarm_names: List[str]
    date_range: Dict[str, str]
    records_stored: int
    records_updated: int
    generation_units_processed: List[GenerationUnitSummary]
    summary: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional summary information like API calls, response time, errors"
    )
    errors: List[str] = Field(default_factory=list)


class RawDataFetchJob(BaseModel):
    """Job tracking for raw data fetch operation."""

    job_id: str
    source: str
    windfarm_ids: List[int]
    start_date: datetime
    end_date: datetime
    status: str  # 'pending', 'in_progress', 'completed', 'failed'
    created_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[RawDataFetchResponse] = None


class UnifiedRawDataFetchResponse(BaseModel):
    """Response from unified raw data fetch (auto-detected sources)."""

    success: bool
    windfarm_ids: List[int]
    windfarm_names: List[str]
    date_range: Dict[str, str]
    total_records_stored: int
    total_records_updated: int
    sources_processed: List[str] = Field(
        default_factory=list,
        description="List of sources that were processed"
    )
    by_source: Dict[str, RawDataFetchResponse] = Field(
        default_factory=dict,
        description="Results grouped by source"
    )
    overall_summary: Dict[str, Any] = Field(
        default_factory=dict,
        description="Overall summary across all sources"
    )
    errors: List[str] = Field(default_factory=list)


class FileUploadRequest(BaseModel):
    """Request parameters for file upload (passed as form data)."""

    start_date: datetime = Field(..., description="Start date for filtering data from file")
    end_date: datetime = Field(..., description="End date for filtering data from file")
    clean_first: bool = Field(default=True, description="Whether to clear existing data before import")
    workers: int = Field(default=4, ge=1, le=8, description="Number of parallel workers for processing")


class FileUploadProgressUpdate(BaseModel):
    """Progress update for file upload processing."""

    status: str  # 'validating', 'processing', 'inserting', 'completed', 'error'
    message: str
    progress_percent: Optional[float] = None
    current_step: Optional[str] = None
    records_processed: Optional[int] = None
    total_records: Optional[int] = None


class FileUploadResponse(BaseModel):
    """Response from file upload operation."""

    success: bool
    source: str
    file_info: Dict[str, Any] = Field(
        default_factory=dict,
        description="Information about uploaded file"
    )
    date_range_requested: Dict[str, str]
    date_range_processed: Dict[str, str]  # Actual date range from data
    records_stored: int
    records_updated: int
    generation_units_processed: List[GenerationUnitSummary]
    summary: Dict[str, Any] = Field(
        default_factory=dict,
        description="Processing summary (duration, rate, etc.)"
    )
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
