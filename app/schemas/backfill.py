"""Schemas for data backfill functionality."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

from app.models.backfill_job import BackfillJobStatus, BackfillTaskStatus


class BackfillJobCreate(BaseModel):
    """Schema for creating a backfill job."""
    windfarm_id: int
    start_year: int = Field(..., ge=2000, le=2100)
    end_year: int = Field(..., ge=2000, le=2100)
    sources: Optional[List[str]] = None  # If None, use all available sources

    class Config:
        json_schema_extra = {
            "example": {
                "windfarm_id": 1,
                "start_year": 2020,
                "end_year": 2023,
                "sources": ["entsoe", "elexon"]
            }
        }


class BackfillTaskBase(BaseModel):
    """Base schema for backfill task."""
    generation_unit_id: int
    source: str
    start_date: datetime
    end_date: datetime
    status: BackfillTaskStatus
    attempt_count: int
    records_fetched: Optional[int] = None
    error_message: Optional[str] = None


class BackfillTask(BackfillTaskBase):
    """Schema for backfill task response."""
    id: int
    job_id: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BackfillJobBase(BaseModel):
    """Base schema for backfill job."""
    windfarm_id: int
    start_date: datetime
    end_date: datetime
    status: BackfillJobStatus
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    error_message: Optional[str] = None


class BackfillJob(BackfillJobBase):
    """Schema for backfill job response."""
    id: int
    created_by_id: int
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    tasks: List[BackfillTask] = []

    class Config:
        from_attributes = True


class BackfillJobSummary(BaseModel):
    """Summary of a backfill job."""
    id: int
    windfarm_id: int
    windfarm_name: str
    windfarm_code: str
    start_date: datetime
    end_date: datetime
    status: BackfillJobStatus
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    progress_percentage: float
    created_at: datetime
    
    class Config:
        from_attributes = True


class BackfillPreview(BaseModel):
    """Preview of what will be backfilled."""
    windfarm_id: int
    windfarm_name: str
    windfarm_code: str
    generation_units: List[Dict[str, Any]]
    date_ranges: List[Dict[str, Any]]
    total_tasks: int
    estimated_time_minutes: float


class DataAvailability(BaseModel):
    """Data availability for a windfarm."""
    windfarm_id: int
    source: str
    year: int
    month: int
    has_data: bool
    record_count: int
    first_record: Optional[datetime] = None
    last_record: Optional[datetime] = None
    coverage_percentage: float


class DataAvailabilityResponse(BaseModel):
    """Response for data availability check."""
    windfarm_id: int
    windfarm_name: str
    windfarm_code: str
    sources: List[str]
    availability: List[DataAvailability]


class BackfillRetryRequest(BaseModel):
    """Request to retry failed tasks."""
    task_ids: Optional[List[int]] = None  # If None, retry all failed tasks
    
    
class BackfillStatusResponse(BaseModel):
    """Response for backfill job status."""
    job: BackfillJob
    failed_tasks: List[BackfillTask]
    in_progress_tasks: List[BackfillTask]
    completed_tasks_count: int
    can_retry: bool