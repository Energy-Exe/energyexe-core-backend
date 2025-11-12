"""API endpoints for weather import jobs."""

import asyncio
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.weather_import_job import WeatherImportJob, WeatherImportStatus
from app.services.weather_import_service import WeatherImportService

router = APIRouter()


# Schemas
class WeatherImportRequest(BaseModel):
    """Request schema for creating a weather import job."""
    start_date: date = Field(..., description="Start date for import (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date for import (YYYY-MM-DD)")


class WeatherImportJobSummary(BaseModel):
    """Lightweight summary response for listing jobs."""
    id: int
    job_name: str
    status: str
    import_start_date: str
    import_end_date: str
    progress_percentage: float = 0.0
    records_imported: int
    created_at: str
    completed_at: Optional[str] = None
    error_message: Optional[str] = None

    @classmethod
    def from_model(cls, job: WeatherImportJob) -> "WeatherImportJobSummary":
        """Create lightweight summary from database model."""
        return cls(
            id=job.id,
            job_name=job.job_name,
            status=job.status,
            import_start_date=job.import_start_date.isoformat(),
            import_end_date=job.import_end_date.isoformat(),
            progress_percentage=job.get_progress_percentage(),
            records_imported=job.records_imported,
            created_at=job.created_at.isoformat(),
            completed_at=job.completed_at.isoformat() if job.completed_at else None,
            error_message=job.error_message,
        )


class WeatherImportJobResponse(BaseModel):
    """Response schema for weather import job."""
    id: int
    job_name: str
    source: str
    import_start_date: str
    import_end_date: str
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    records_imported: int
    files_downloaded: int
    files_deleted: int
    api_calls_made: int
    progress_percentage: float = 0.0
    current_phase: Optional[str] = None
    current_date: Optional[str] = None
    dates_completed: Optional[int] = None
    total_dates: Optional[int] = None
    error_message: Optional[str] = None
    created_at: str

    @classmethod
    def from_model(cls, job: WeatherImportJob) -> "WeatherImportJobResponse":
        """Create response from database model."""
        metadata = job.job_metadata or {}
        return cls(
            id=job.id,
            job_name=job.job_name,
            source=job.source,
            import_start_date=job.import_start_date.isoformat(),
            import_end_date=job.import_end_date.isoformat(),
            status=job.status,
            started_at=job.started_at.isoformat() if job.started_at else None,
            completed_at=job.completed_at.isoformat() if job.completed_at else None,
            duration_seconds=job.duration_seconds,
            records_imported=job.records_imported,
            files_downloaded=job.files_downloaded,
            files_deleted=job.files_deleted,
            api_calls_made=job.api_calls_made,
            progress_percentage=job.get_progress_percentage(),
            current_phase=metadata.get('current_phase'),
            current_date=metadata.get('current_date'),
            dates_completed=metadata.get('dates_completed'),
            total_dates=metadata.get('total_dates'),
            error_message=job.error_message,
            created_at=job.created_at.isoformat(),
        )


@router.post("/", response_model=WeatherImportJobResponse, status_code=status.HTTP_201_CREATED)
async def trigger_weather_import(
    request: WeatherImportRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger a weather data import for the specified date range.

    The import runs in the background. Use the status endpoint to
    monitor progress.

    Raises:
        400: Invalid date range
    """
    # Validation
    if request.start_date > request.end_date:
        raise HTTPException(400, "start_date must be before or equal to end_date")

    days_diff = (request.end_date - request.start_date).days
    if days_diff > 365:
        raise HTTPException(400, "Date range cannot exceed 365 days")

    # Create service and job
    service = WeatherImportService(db)
    job = await service.create_job(
        start_date=request.start_date,
        end_date=request.end_date,
        user_id=current_user.id,
    )

    # Execute in background (non-blocking)
    asyncio.create_task(service.execute_job_async(job.id))

    return WeatherImportJobResponse.from_model(job)


@router.get("/{job_id}", response_model=WeatherImportJobResponse)
async def get_import_status(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get current status of a weather import job.

    Returns real-time progress including:
    - Overall progress percentage
    - Current phase (downloading/processing/storing)
    - Current date being processed
    - Records processed so far

    Raises:
        404: Job not found
    """
    service = WeatherImportService(db)
    job = await service.get_job_by_id(job_id)

    if not job:
        raise HTTPException(404, "Job not found")

    return WeatherImportJobResponse.from_model(job)


@router.get("/", response_model=List[WeatherImportJobSummary])
async def list_import_jobs(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    List all weather import jobs with optional filtering.

    Returns lightweight summaries with only essential fields.
    Use GET /{job_id} for full details of a specific job.

    Args:
        status: Filter by status (pending/running/success/failed/cancelled)
        limit: Maximum results to return (default 50)
        offset: Number of results to skip (for pagination)

    Returns:
        List of weather import job summaries, ordered by created_at desc
    """
    service = WeatherImportService(db)
    jobs = await service.list_jobs(status=status, limit=limit, offset=offset)

    return [WeatherImportJobSummary.from_model(job) for job in jobs]


@router.delete("/{job_id}", response_model=WeatherImportJobResponse)
async def cancel_import_job(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Cancel a running import job.

    Note: This marks the job as cancelled in the database. The actual
    subprocess may continue until it completes or times out.

    Raises:
        404: Job not found
    """
    service = WeatherImportService(db)
    job = await service.cancel_job(job_id)

    return WeatherImportJobResponse.from_model(job)
