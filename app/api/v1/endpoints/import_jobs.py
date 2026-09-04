"""API endpoints for scheduled import job management."""

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

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

# One lock per job key serialises the "is one already in flight?" check with
# the row creation, so two concurrent triggers (a Lambda retry landing while
# the first request is still running) cannot both start an import. Process-
# local is enough: the API runs one task with one worker. Only the check +
# create happen under the lock; the import itself runs outside it.
_trigger_locks: Dict[str, asyncio.Lock] = {}


def _trigger_lock(job_name: str) -> asyncio.Lock:
    return _trigger_locks.setdefault(job_name, asyncio.Lock())


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
    start: Optional[date] = Query(None, description="Override start date (YYYY-MM-DD) for backfill"),
    end: Optional[date] = Query(None, description="Override end date (YYYY-MM-DD) for backfill"),
    db: AsyncSession = Depends(get_db),
):
    """
    Public endpoint to trigger a scheduled import job.

    This endpoint is PUBLIC (no authentication required) for use with:
    - GitHub Actions scheduled workflows
    - External cron services
    - Automated triggers
    - Manual backfills (pass `start` and `end` to override the default date window)

    WARNING: This endpoint has no authentication. Only use for scheduled jobs.

    Args:
        job_name: Name of job to trigger (entsoe-daily, taipower-hourly, etc.)
        start: Optional explicit start date — overrides the default delay-based window
        end: Optional explicit end date — overrides the default delay-based window

    Returns:
        Created and executed job details
    """
    if (start is None) != (end is None):
        raise HTTPException(
            status_code=400,
            detail="Both `start` and `end` must be provided together",
        )
    if start is not None and end is not None and start > end:
        raise HTTPException(
            status_code=400,
            detail="`start` must be on or before `end`",
        )
    # Job configurations with appropriate delays
    job_configs = {
        "entsoe-daily": {
            "source": "ENTSOE",
            "delay_days": 3,
        },
        "elexon-daily": {
            "source": "ELEXON",
            "delay_days": 10,  # ELEXON has 10-day publication lag
        },
        "taipower-hourly": {
            "source": "Taipower",
            "delay_days": 0,
        },
        "eia-monthly": {
            "source": "EIA",
            "delay_months": 2,
        },
        "entsoe-prices-daily": {
            "source": "ENTSOE_PRICES",
            "delay_days": 2,
        },
        "elexon-prices-daily": {
            "source": "ELEXON_PRICES",
            "delay_days": 1,
        },
        "ecb-rates-daily": {
            "source": "ECB_RATES",
            "delay_days": 0,
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

        if start is not None and end is not None:
            # Explicit override (manual backfill)
            import_start = datetime.combine(start, datetime.min.time())
            import_end = datetime.combine(end, datetime.max.time()).replace(microsecond=0)
        elif "delay_days" in config:
            import_date = today - timedelta(days=config["delay_days"])
            import_start = import_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
            import_end = import_date.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=None)
        elif "delay_months" in config:
            # For monthly jobs
            months_ago = config["delay_months"]
            year = today.year
            month = today.month - months_ago

            while month < 1:
                month += 12
                year -= 1

            import_start = datetime(year, month, 1, 0, 0, 0)

            if month == 12:
                import_end = datetime(year + 1, 1, 1, 0, 0, 0) - timedelta(seconds=1)
            else:
                import_end = datetime(year, month + 1, 1, 0, 0, 0) - timedelta(seconds=1)
        else:
            import_start = today.replace(tzinfo=None)
            import_end = today.replace(tzinfo=None)

        # Create job
        metadata = {"job_config": job_name, "trigger": "external"}
        if start is not None and end is not None:
            metadata["backfill"] = True
        job_request = ImportJobCreate(
            source=config["source"],
            import_start_date=import_start,
            import_end_date=import_end,
            job_metadata=metadata,
        )

        async with _trigger_lock(job_name):
            inflight = await service.find_inflight_scheduled(job_name)
            if inflight is not None:
                if start is not None:
                    # A backfill must not silently reuse a scheduled run's window.
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"{job_name} is already running (job {inflight.id}, "
                            f"status {inflight.status}); retry when it has finished"
                        ),
                    )
                # Scheduled re-trigger (Lambda retry after a read timeout, or a
                # double fire): hand back the live row. 200 rather than 409 so
                # the Lambda/workflow do not count a healthy import as failed.
                logger.info(
                    "import_trigger_dedup",
                    job_name=job_name,
                    job_id=inflight.id,
                    status=inflight.status,
                )
                return ImportJobResponse.model_validate(inflight)

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

        # Execute now; the response is the terminal row (the Lambda and the
        # manual workflow both key on it). execute_job waits for the import on a
        # worker thread, so the event loop keeps serving meanwhile.
        result = await service.execute_job(job.id)

        return ImportJobResponse.model_validate(result)

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error triggering job {job_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {str(e)}")
