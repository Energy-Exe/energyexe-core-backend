"""API endpoints for scheduled import job management."""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.import_job_execution import ImportJobType
from app.schemas.import_job import (
    ImportJobCreate,
    ImportJobExecuteRequest,
    ImportJobFilter,
    ImportJobHealth,
    ImportJobListResponse,
    ImportJobResponse,
    ImportJobRetryRequest,
    ImportJobSummary,
)
from app.services.import_job_service import ImportJobService

logger = structlog.get_logger()
router = APIRouter()


@router.post("/", response_model=ImportJobResponse)
async def create_import_job(
    request: ImportJobCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new manual import job.

    This creates a job record but doesn't execute it yet.
    Use POST /import-jobs/{id}/execute to run it.
    """
    service = ImportJobService(db)

    try:
        job = await service.create_job(request, user_id=current_user.id)
        return ImportJobResponse.model_validate(job)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating import job: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create import job")


@router.post("/{job_id}/execute", response_model=ImportJobResponse)
async def execute_import_job(
    job_id: int,
    request: ImportJobExecuteRequest = ImportJobExecuteRequest(),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Execute an import job.

    This runs the actual import script and updates the job status.
    """
    service = ImportJobService(db)

    try:
        job = await service.execute_job(job_id)
        return ImportJobResponse.model_validate(job)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error executing import job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to execute job: {str(e)}")


@router.post("/{job_id}/retry", response_model=ImportJobResponse)
async def retry_import_job(
    job_id: int,
    request: ImportJobRetryRequest = ImportJobRetryRequest(),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retry a failed import job.

    Increments retry counter and re-executes the job.
    """
    service = ImportJobService(db)

    try:
        job = await service.retry_job(job_id, reset_retry_count=request.reset_retry_count)
        return ImportJobResponse.model_validate(job)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error retrying import job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retry job")


@router.get("/", response_model=ImportJobListResponse)
async def list_import_jobs(
    source: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    job_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    List import job executions with filtering and pagination.

    Filters:
    - source: Filter by data source (ENTSOE, Taipower, EIA, ELEXON)
    - status: Filter by status (pending, running, success, failed)
    - job_type: Filter by type (scheduled, manual)
    - start_date/end_date: Filter by execution date range
    """
    from datetime import date as date_type

    service = ImportJobService(db)

    filters = ImportJobFilter(
        source=source,
        status=status,
        job_type=job_type,
        start_date=date_type.fromisoformat(start_date) if start_date else None,
        end_date=date_type.fromisoformat(end_date) if end_date else None,
        limit=limit,
        offset=offset,
    )

    try:
        jobs, total = await service.get_jobs(filters)

        return ImportJobListResponse(
            items=[ImportJobResponse.model_validate(job) for job in jobs],
            total=total,
            limit=limit,
            offset=offset,
            has_more=(offset + limit) < total,
        )
    except Exception as e:
        logger.error(f"Error listing import jobs: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to list import jobs")


@router.get("/{job_id}", response_model=ImportJobResponse)
async def get_import_job(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get import job by ID."""
    service = ImportJobService(db)

    job = await service.get_job_by_id(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return ImportJobResponse.model_validate(job)


@router.get("/latest/status", response_model=List[ImportJobSummary])
async def get_latest_job_status(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get latest execution status for each scheduled job.

    Returns summary cards for dashboard display.
    """
    service = ImportJobService(db)

    try:
        summaries = await service.get_latest_status_per_job()
        return summaries
    except Exception as e:
        logger.error(f"Error getting latest job status: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get job status")


@router.get("/health/status", response_model=ImportJobHealth)
async def get_system_health(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get overall system health for import jobs.

    Returns health indicators, running jobs, recent failures.
    """
    service = ImportJobService(db)

    try:
        health = await service.get_system_health()
        return health
    except Exception as e:
        logger.error(f"Error getting system health: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get system health")


# Public endpoint for automated triggers (GitHub Actions, external cron services)
@router.post("/trigger/{job_name}", response_model=ImportJobResponse)
async def trigger_scheduled_job(
    job_name: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Public endpoint to trigger a scheduled import job.

    This endpoint is PUBLIC (no authentication required) for use with:
    - GitHub Actions scheduled workflows
    - External cron services
    - Automated triggers

    WARNING: This endpoint has no authentication. Only use for scheduled jobs.

    Args:
        job_name: Name of job to trigger (entsoe-daily, taipower-hourly, etc.)

    Returns:
        Created and executed job details
    """
    # Job configurations with appropriate delays
    job_configs = {
        "entsoe-daily": {
            "source": "ENTSOE",
            "delay_days": 3,
        },
        "elexon-daily": {
            "source": "ELEXON",
            "delay_days": 3,
        },
        "taipower-hourly": {
            "source": "Taipower",
            "delay_days": 0,
        },
        "eia-monthly": {
            "source": "EIA",
            "delay_months": 2,
        },
    }

    if job_name not in job_configs:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job: {job_name}. Available: {', '.join(job_configs.keys())}"
        )

    config = job_configs[job_name]
    service = ImportJobService(db)

    try:
        # Calculate import date range based on delay
        today = datetime.now(timezone.utc)

        if "delay_days" in config:
            import_date = today - timedelta(days=config["delay_days"])
            import_start = import_date.replace(hour=0, minute=0, second=0, microsecond=0)
            import_end = import_date.replace(hour=23, minute=59, second=59, microsecond=0)
        elif "delay_months" in config:
            # For monthly jobs
            months_ago = config["delay_months"]
            year = today.year
            month = today.month - months_ago

            while month < 1:
                month += 12
                year -= 1

            import_start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)

            if month == 12:
                import_end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc) - timedelta(
                    seconds=1
                )
            else:
                import_end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=timezone.utc) - timedelta(
                    seconds=1
                )
        else:
            import_start = today
            import_end = today

        # Create job
        job_request = ImportJobCreate(
            source=config["source"],
            import_start_date=import_start,
            import_end_date=import_end,
            job_metadata={"job_config": job_name, "trigger": "external"},
        )

        job = await service.create_job(
            job_request,
            user_id=None,
            job_type=ImportJobType.SCHEDULED,
        )

        logger.info(
            f"Triggered scheduled job via public endpoint",
            job_name=job_name,
            job_id=job.id,
        )

        # Execute immediately
        result = await service.execute_job(job.id)

        return ImportJobResponse.model_validate(result)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error triggering job {job_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {str(e)}")
