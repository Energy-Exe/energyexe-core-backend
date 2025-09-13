"""Celery tasks for backfill operations."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import structlog
from celery import chain, group
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.celery_app import celery_app
from app.core.database import get_session_factory
from app.models.backfill_job import (
    BackfillJob,
    BackfillJobStatus,
    BackfillTask,
    BackfillTaskStatus,
)
from app.models.generation_unit import GenerationUnit
from app.services.backfill_service import BackfillService
from app.tasks.base import BackfillTask as CeleryBackfillTask

logger = structlog.get_logger()


@celery_app.task(base=CeleryBackfillTask, bind=True, name="app.tasks.backfill.process_job")
def process_backfill_job(self, job_id: int) -> Dict[str, Any]:
    """
    Process an entire backfill job by orchestrating individual tasks.
    
    This task:
    1. Loads the job and its tasks
    2. Creates a group of individual task processing
    3. Updates job status on completion
    """
    logger.info(f"Starting backfill job processing", job_id=job_id)
    
    # Run async function in sync context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(_process_backfill_job_async(self, job_id))
        return result
    finally:
        loop.close()


async def _process_backfill_job_async(celery_task, job_id: int) -> Dict[str, Any]:
    """Async implementation of job processing."""
    async_session = get_session_factory()
    
    # Get job and update status in one session
    async with async_session() as db:
        # Get job with tasks
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job_id)
        )
        result = await db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Update job status to in_progress
        job.status = BackfillJobStatus.IN_PROGRESS
        job.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        job.celery_task_id = celery_task.request.id
        await db.commit()
        
        # Get pending task IDs
        pending_task_ids = [t.id for t in job.tasks if t.status == BackfillTaskStatus.PENDING]
    
    total_tasks = len(pending_task_ids)
    
    if total_tasks == 0:
        logger.info(f"No pending tasks for job {job_id}")
        return {
            "job_id": job_id,
            "status": "completed",
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
        }
    
    # Import tasks here to avoid circular import
    from app.tasks.backfill import process_backfill_task, monitor_job_completion
    
    # Queue tasks individually (not as a chord)
    queued_tasks = []
    for idx, task_id in enumerate(pending_task_ids):
        # Update progress
        celery_task.update_progress(
            current=idx,
            total=total_tasks,
            status=f"Queueing task {task_id}",
            meta={"job_id": job_id, "task_id": task_id},
        )
        
        # Queue individual task
        result = process_backfill_task.delay(task_id)
        queued_tasks.append({"task_id": task_id, "celery_id": result.id})
        logger.info(f"Queued task {task_id} with Celery ID {result.id}")
    
    # Queue a monitoring task to check completion periodically
    monitor_result = monitor_job_completion.delay(job_id)
    
    logger.info(f"Queued {total_tasks} tasks for job {job_id}")
    
    return {
        "job_id": job_id,
        "status": "tasks_queued",
        "total_tasks": total_tasks,
        "monitor_task_id": monitor_result.id,
        "queued_tasks": queued_tasks,
    }


@celery_app.task(base=CeleryBackfillTask, bind=True, name="app.tasks.backfill.process_task")
def process_backfill_task(self, task_id: int) -> Dict[str, Any]:
    """
    Process a single backfill task.
    
    This task:
    1. Fetches data from the appropriate external API
    2. Stores the data in the database
    3. Updates task status
    """
    logger.info(f"Processing backfill task", task_id=task_id)
    
    # Run async function in sync context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(_process_backfill_task_async(self, task_id))
        return result
    finally:
        loop.close()


async def _process_backfill_task_async(celery_task, task_id: int) -> Dict[str, Any]:
    """Async implementation of task processing."""
    async_session = get_session_factory()
    
    # First, update task status to IN_PROGRESS
    async with async_session() as db:
        # Get task
        stmt = (
            select(BackfillTask)
            .options(selectinload(BackfillTask.generation_unit))
            .where(BackfillTask.id == task_id)
        )
        result = await db.execute(stmt)
        task = result.scalar_one_or_none()
        
        if not task:
            raise ValueError(f"Backfill task {task_id} not found")
        
        # Update task status
        task.status = BackfillTaskStatus.IN_PROGRESS
        task.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        task.celery_task_id = celery_task.request.id
        task.attempt_count += 1
        await db.commit()
    
    # Process the task - let the service create its own sessions
    try:
        # Create a new service instance that will manage its own sessions
        async with async_session() as db:
            service = BackfillService(db)
            # Process the task by ID, let the service handle database operations
            await service._process_single_task(task_id)
        
        # Get the final task state with a fresh session
        async with async_session() as db:
            stmt = select(BackfillTask).where(BackfillTask.id == task_id)
            result = await db.execute(stmt)
            task = result.scalar_one()
            
            return {
                "task_id": task_id,
                "status": task.status,
                "records_fetched": task.records_fetched or 0,
                "error_message": task.error_message,
            }
            
    except Exception as e:
        logger.error(f"Error processing task {task_id}: {str(e)}")
        
        # Update task status with a new session
        async with async_session() as db:
            stmt = select(BackfillTask).where(BackfillTask.id == task_id)
            result = await db.execute(stmt)
            task = result.scalar_one()
            
            task.status = BackfillTaskStatus.FAILED
            task.error_message = str(e)
            task.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            await db.commit()
            
            # Check if we should retry
            if task.attempt_count < task.max_attempts:
                # Calculate retry delay
                retry_delay = celery_task.calculate_retry_delay(task.attempt_count)
                logger.info(f"Retrying task {task_id} in {retry_delay} seconds")
                
                # Reset status for retry
                task.status = BackfillTaskStatus.PENDING
                await db.commit()
                
                # Retry the task
                raise celery_task.retry(countdown=retry_delay, exc=e)
            
            # Max retries exceeded
            return {
                "task_id": task_id,
                "status": "failed",
                "error": str(e),
                "attempts": task.attempt_count,
            }


@celery_app.task(base=CeleryBackfillTask, name="app.tasks.backfill.finalize_job")
def finalize_backfill_job(task_results: list, job_id: int) -> Dict[str, Any]:
    """
    Finalize a backfill job after all tasks complete.
    
    This task:
    1. Aggregates results from all tasks
    2. Updates job status based on task results
    3. Calculates final statistics
    """
    logger.info(f"Finalizing backfill job", job_id=job_id)
    
    # Run async function in sync context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(_finalize_backfill_job_async(task_results, job_id))
        return result
    finally:
        loop.close()


async def _finalize_backfill_job_async(task_results: list, job_id: int) -> Dict[str, Any]:
    """Async implementation of job finalization."""
    async_session = get_session_factory()
    async with async_session() as db:
        # Get job with tasks
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job_id)
        )
        result = await db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Count task statuses
        completed_count = 0
        failed_count = 0
        total_records = 0
        
        for task in job.tasks:
            if task.status == BackfillTaskStatus.COMPLETED:
                completed_count += 1
                total_records += task.records_fetched or 0
            elif task.status == BackfillTaskStatus.FAILED:
                failed_count += 1
        
        # Update job status
        job.completed_tasks = completed_count
        job.failed_tasks = failed_count
        
        if failed_count == 0:
            job.status = BackfillJobStatus.COMPLETED
        elif completed_count > 0:
            job.status = BackfillJobStatus.PARTIALLY_COMPLETED
        else:
            job.status = BackfillJobStatus.FAILED
        
        job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        
        # Store summary in metadata
        if not job.job_metadata:
            job.job_metadata = {}
        
        job.job_metadata.update({
            "total_records_fetched": total_records,
            "task_results_summary": {
                "completed": completed_count,
                "failed": failed_count,
                "total": job.total_tasks,
            },
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        })
        
        await db.commit()
        
        logger.info(
            f"Backfill job {job_id} finalized",
            status=job.status,
            completed_tasks=completed_count,
            failed_tasks=failed_count,
            total_records=total_records,
        )
        
        return {
            "job_id": job_id,
            "status": job.status,
            "completed_tasks": completed_count,
            "failed_tasks": failed_count,
            "total_records": total_records,
        }


@celery_app.task(base=CeleryBackfillTask, name="app.tasks.backfill.update_progress")
def update_job_progress(job_id: int) -> Dict[str, Any]:
    """
    Update job progress by checking task statuses.
    
    This is a helper task that can be called periodically to update progress.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(_update_job_progress_async(job_id))
        return result
    finally:
        loop.close()


async def _update_job_progress_async(job_id: int) -> Dict[str, Any]:
    """Async implementation of progress update."""
    async_session = get_session_factory()
    async with async_session() as db:
        # Get job with tasks
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job_id)
        )
        result = await db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Count task statuses
        status_counts = {
            "pending": 0,
            "in_progress": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
        }
        
        for task in job.tasks:
            status_counts[task.status] += 1
        
        # Calculate progress percentage
        progress_percentage = 0
        if job.total_tasks > 0:
            processed_tasks = status_counts["completed"] + status_counts["failed"] + status_counts["skipped"]
            progress_percentage = round((processed_tasks / job.total_tasks) * 100, 2)
        
        # Update job counters
        job.completed_tasks = status_counts["completed"]
        job.failed_tasks = status_counts["failed"]
        
        # Store progress in metadata
        if not job.job_metadata:
            job.job_metadata = {}
        
        job.job_metadata["progress"] = {
            "percentage": progress_percentage,
            "status_counts": status_counts,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        
        await db.commit()
        
        return {
            "job_id": job_id,
            "progress_percentage": progress_percentage,
            "status_counts": status_counts,
            "job_status": job.status,
        }


@celery_app.task(base=CeleryBackfillTask, bind=True, name="app.tasks.backfill.monitor_job_completion")
def monitor_job_completion(self, job_id: int) -> Dict[str, Any]:
    """
    Monitor job completion and finalize when all tasks are done.
    
    This task runs periodically to check if all tasks are complete.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(_monitor_job_completion_async(self, job_id))
        return result
    finally:
        loop.close()


async def _monitor_job_completion_async(celery_task, job_id: int) -> Dict[str, Any]:
    """Async implementation of job monitoring."""
    async_session = get_session_factory()
    
    async with async_session() as db:
        # Get job with tasks
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job_id)
        )
        result = await db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Count task statuses
        pending_count = 0
        in_progress_count = 0
        completed_count = 0
        failed_count = 0
        
        for task in job.tasks:
            if task.status == BackfillTaskStatus.PENDING:
                pending_count += 1
            elif task.status == BackfillTaskStatus.IN_PROGRESS:
                in_progress_count += 1
            elif task.status == BackfillTaskStatus.COMPLETED:
                completed_count += 1
            elif task.status == BackfillTaskStatus.FAILED:
                failed_count += 1
        
        logger.info(
            f"Job {job_id} status: pending={pending_count}, in_progress={in_progress_count}, "
            f"completed={completed_count}, failed={failed_count}"
        )
        
        # Check if all tasks are done (no pending or in_progress)
        if pending_count == 0 and in_progress_count == 0:
            # All tasks are done, finalize the job
            total_records = sum(task.records_fetched or 0 for task in job.tasks if task.status == BackfillTaskStatus.COMPLETED)
            
            # Update job status
            job.completed_tasks = completed_count
            job.failed_tasks = failed_count
            
            if failed_count == 0:
                job.status = BackfillJobStatus.COMPLETED
            elif completed_count > 0:
                job.status = BackfillJobStatus.PARTIALLY_COMPLETED
            else:
                job.status = BackfillJobStatus.FAILED
            
            job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            
            # Store summary in metadata
            if not job.job_metadata:
                job.job_metadata = {}
            
            job.job_metadata.update({
                "total_records_fetched": total_records,
                "final_status_counts": {
                    "completed": completed_count,
                    "failed": failed_count,
                    "total": job.total_tasks,
                },
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            })
            
            await db.commit()
            
            logger.info(
                f"Backfill job {job_id} finalized",
                status=job.status,
                completed_tasks=completed_count,
                failed_tasks=failed_count,
                total_records=total_records,
            )
            
            return {
                "job_id": job_id,
                "status": "finalized",
                "job_status": job.status,
                "completed_tasks": completed_count,
                "failed_tasks": failed_count,
                "total_records": total_records,
            }
        else:
            # Still have tasks running, schedule another check
            logger.info(f"Job {job_id} still has {pending_count + in_progress_count} tasks running, scheduling recheck")
            
            # Retry in 10 seconds
            raise celery_task.retry(countdown=10, max_retries=None)
            
            return {
                "job_id": job_id,
                "status": "monitoring",
                "pending_tasks": pending_count,
                "in_progress_tasks": in_progress_count,
                "completed_tasks": completed_count,
                "failed_tasks": failed_count,
            }