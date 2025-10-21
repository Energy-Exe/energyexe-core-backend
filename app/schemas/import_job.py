"""Schemas for import job management."""

from datetime import datetime, date
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ImportJobCreate(BaseModel):
    """Request to create a manual import job."""

    source: str = Field(..., description="Data source (ENTSOE, Taipower, EIA, ELEXON)")
    import_start_date: datetime = Field(..., description="Start date for data import")
    import_end_date: datetime = Field(..., description="End date for data import")
    job_metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")

    class Config:
        json_schema_extra = {
            "example": {
                "source": "ENTSOE",
                "import_start_date": "2025-10-15T00:00:00Z",
                "import_end_date": "2025-10-15T23:59:59Z",
                "metadata": {"zones": ["DK", "FR", "BE"]}
            }
        }


class ImportJobResponse(BaseModel):
    """Response for import job execution."""

    id: int
    job_name: str
    source: str
    job_type: str
    import_start_date: datetime
    import_end_date: datetime
    status: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    records_imported: int
    records_updated: int
    api_calls_made: Optional[int]
    error_message: Optional[str]
    retry_count: int
    max_retries: int
    job_metadata: Optional[Dict[str, Any]]
    created_by_id: Optional[int]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ImportJobFilter(BaseModel):
    """Filters for querying import job executions."""

    source: Optional[str] = Field(None, description="Filter by source")
    status: Optional[str] = Field(None, description="Filter by status")
    job_type: Optional[str] = Field(None, description="Filter by type (scheduled/manual)")
    start_date: Optional[date] = Field(None, description="Filter executions from this date")
    end_date: Optional[date] = Field(None, description="Filter executions to this date")
    limit: int = Field(50, ge=1, le=500, description="Number of records to return")
    offset: int = Field(0, ge=0, description="Offset for pagination")


class ImportJobSummary(BaseModel):
    """Summary of import job status."""

    job_name: str
    source: str
    last_execution: Optional[ImportJobResponse]
    next_scheduled_run: Optional[datetime]
    total_executions: int
    success_count: int
    failed_count: int
    last_24h_success_rate: float  # Percentage


class ImportJobHealth(BaseModel):
    """Overall health status of import jobs."""

    total_jobs: int
    running_jobs: int
    recent_failures: int  # Last 24 hours
    jobs_behind_schedule: List[str]  # Jobs that haven't run when expected
    overall_health: str  # 'healthy', 'degraded', 'critical'
    last_updated: datetime


class ImportJobExecuteRequest(BaseModel):
    """Request to execute a job."""

    force: bool = Field(
        default=False,
        description="Force execution even if already running or recently completed"
    )


class ImportJobRetryRequest(BaseModel):
    """Request to retry a failed job."""

    reset_retry_count: bool = Field(
        default=False,
        description="Reset retry counter to 0"
    )


class ImportJobListResponse(BaseModel):
    """Paginated list of import job executions."""

    items: List[ImportJobResponse]
    total: int
    limit: int
    offset: int
    has_more: bool
