"""API endpoints for data backfill management."""

from typing import Any, Dict, List, Optional

import structlog
from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.celery_app import celery_app
from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.backfill_job import BackfillJobStatus
from app.schemas.backfill import (
    BackfillJobCreate,
    BackfillJob,
    BackfillJobSummary,
    BackfillPreview,
    DataAvailabilityResponse,
    BackfillRetryRequest,
    BackfillStatusResponse,
)
from app.services.backfill_service import BackfillService

logger = structlog.get_logger()
router = APIRouter()


@router.post("/jobs", response_model=BackfillJob)
async def create_backfill_job(
    request: BackfillJobCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new backfill job for a windfarm.
    
    This will:
    1. Create a job with tasks for each generation unit and month
    2. Start processing the tasks asynchronously
    3. Return the job details immediately
    """
    service = BackfillService(db)
    
    try:
        job = await service.create_backfill_job(request, current_user)
        return job
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating backfill job: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create backfill job")


@router.post("/jobs/{job_id}/process", response_model=BackfillJob)
async def process_backfill_job(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Start processing a backfill job.
    
    This will process all pending tasks in the job sequentially.
    """
    service = BackfillService(db)
    
    try:
        job = await service.process_backfill_job(job_id)
        return job
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing backfill job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to process backfill job")


@router.post("/preview", response_model=BackfillPreview)
async def preview_backfill(
    request: BackfillJobCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Preview what will be backfilled without creating a job.
    
    Shows:
    - Generation units that will be processed
    - Date ranges (monthly chunks)
    - Total number of tasks
    - Estimated time
    """
    service = BackfillService(db)
    
    try:
        preview = await service.get_backfill_preview(request)
        return preview
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/jobs", response_model=List[BackfillJobSummary])
async def list_backfill_jobs(
    windfarm_id: Optional[int] = Query(None, description="Filter by windfarm ID"),
    status: Optional[BackfillJobStatus] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    List backfill jobs with optional filters.
    """
    service = BackfillService(db)
    
    jobs = await service.get_backfill_jobs(
        windfarm_id=windfarm_id,
        status=status,
        limit=limit,
        offset=offset,
    )
    
    # Convert to summary format
    summaries = []
    for job in jobs:
        # Get windfarm details from metadata - handle it as a dict
        metadata = job.job_metadata if job.job_metadata else {}
        windfarm_name = metadata.get("windfarm_name", "Unknown") if isinstance(metadata, dict) else "Unknown"
        windfarm_code = metadata.get("windfarm_code", "Unknown") if isinstance(metadata, dict) else "Unknown"
        
        progress = (job.completed_tasks / job.total_tasks * 100) if job.total_tasks > 0 else 0
        
        summaries.append(BackfillJobSummary(
            id=job.id,
            windfarm_id=job.windfarm_id,
            windfarm_name=windfarm_name,
            windfarm_code=windfarm_code,
            start_date=job.start_date,
            end_date=job.end_date,
            status=job.status,
            total_tasks=job.total_tasks,
            completed_tasks=job.completed_tasks,
            failed_tasks=job.failed_tasks,
            progress_percentage=progress,
            created_at=job.created_at,
        ))
    
    return summaries


@router.get("/jobs/{job_id}", response_model=BackfillStatusResponse)
async def get_backfill_job_status(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed status of a backfill job including tasks.
    """
    service = BackfillService(db)
    
    job = await service.get_backfill_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    
    # Separate tasks by status
    failed_tasks = [t for t in job.tasks if t.status == "failed"]
    in_progress_tasks = [t for t in job.tasks if t.status == "in_progress"]
    completed_tasks_count = len([t for t in job.tasks if t.status == "completed"])
    
    # Can retry if there are failed tasks
    can_retry = len(failed_tasks) > 0
    
    return BackfillStatusResponse(
        job=job,
        failed_tasks=failed_tasks,
        in_progress_tasks=in_progress_tasks,
        completed_tasks_count=completed_tasks_count,
        can_retry=can_retry,
    )


@router.post("/jobs/{job_id}/retry", response_model=BackfillJob)
async def retry_failed_tasks(
    job_id: int,
    request: BackfillRetryRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retry failed tasks in a backfill job.
    
    If task_ids is not provided, all failed tasks will be retried.
    """
    service = BackfillService(db)
    
    try:
        job = await service.retry_failed_tasks(job_id, request.task_ids)
        return job
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/availability/{windfarm_id}", response_model=DataAvailabilityResponse)
async def check_data_availability(
    windfarm_id: int,
    year: Optional[int] = Query(None, description="Filter by specific year"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Check data availability for a windfarm.
    
    Shows which months have data for each source.
    If year is not provided, checks the last 2 years and current year.
    """
    service = BackfillService(db)
    
    try:
        availability = await service.get_data_availability(windfarm_id, year)
        return availability
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/jobs/{job_id}/cancel", response_model=BackfillJob)
async def cancel_backfill_job(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Cancel a pending or in-progress backfill job.
    
    This will:
    1. Mark the job as failed with a cancellation message
    2. Skip all pending/in-progress tasks
    3. Stop any further processing
    """
    service = BackfillService(db)
    
    try:
        job = await service.cancel_backfill_job(job_id)
        return job
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error cancelling backfill job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to cancel job")


@router.post("/jobs/{job_id}/reset-stuck", response_model=BackfillJob)
async def reset_stuck_tasks(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Reset stuck tasks in a backfill job.
    
    This will:
    1. Find tasks that are stuck in 'in_progress' state
    2. Mark them as 'failed' so they can be retried
    3. Handle pending tasks if the job is not running
    """
    service = BackfillService(db)
    
    try:
        job = await service.reset_stuck_tasks(job_id)
        return job
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error resetting stuck tasks for job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to reset stuck tasks")


@router.delete("/jobs/{job_id}")
async def delete_backfill_job(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Delete a backfill job and all its tasks.
    
    Note: Can only delete jobs that are not currently in progress.
    To delete an in-progress job, cancel it first.
    """
    service = BackfillService(db)
    
    try:
        success = await service.delete_backfill_job(job_id)
        return {"success": success, "message": f"Job {job_id} deleted successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting backfill job {job_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete job")


@router.get("/jobs/{job_id}/celery-status")
async def get_celery_task_status(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """
    Get Celery task status for a backfill job.
    
    Returns the current state and progress of the Celery task.
    """
    service = BackfillService(db)
    
    # Get the job to find the Celery task ID
    job = await service.get_backfill_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    
    if not job.celery_task_id:
        return {
            "job_id": job_id,
            "celery_task_id": None,
            "state": "NOT_QUEUED",
            "info": "Job was not queued to Celery",
        }
    
    # Get Celery task result
    result = AsyncResult(job.celery_task_id, app=celery_app)
    
    response = {
        "job_id": job_id,
        "celery_task_id": job.celery_task_id,
        "state": result.state,
        "ready": result.ready(),
        "successful": result.successful() if result.ready() else None,
        "failed": result.failed() if result.ready() else None,
    }
    
    # Add progress info if available
    if result.state == "PROGRESS":
        response["progress"] = result.info
    elif result.state == "SUCCESS":
        response["result"] = result.result
    elif result.state == "FAILURE":
        response["error"] = str(result.info) if result.info else "Unknown error"
        response["traceback"] = result.traceback
    
    return response


@router.post("/jobs/{job_id}/refresh-progress")
async def refresh_job_progress(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """
    Refresh job progress by checking all task statuses.
    
    This triggers an update of the job's progress based on current task states.
    """
    from app.tasks.backfill import update_job_progress
    
    # Queue the progress update task
    task = update_job_progress.delay(job_id)
    
    return {
        "job_id": job_id,
        "progress_task_id": task.id,
        "message": "Progress update queued",
    }