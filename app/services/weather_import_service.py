"""Service layer for weather data import job execution."""

import asyncio
from datetime import datetime, date, timezone, timedelta
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from app.models.weather_import_job import WeatherImportJob, WeatherImportStatus
from app.core.database import get_session_factory

logger = structlog.get_logger()


class WeatherImportService:
    """Service for managing weather import jobs."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_job(
        self,
        start_date: date,
        end_date: date,
        user_id: Optional[int] = None,
    ) -> WeatherImportJob:
        """
        Create a new weather import job.

        Args:
            start_date: Start date for import
            end_date: End date for import
            user_id: User creating the job

        Returns:
            Created WeatherImportJob
        """
        # Calculate total dates
        total_dates = (end_date - start_date).days + 1

        job = WeatherImportJob(
            job_name=f"Weather Import {start_date} to {end_date}",
            source="ERA5",
            import_start_date=datetime.combine(start_date, datetime.min.time()),
            import_end_date=datetime.combine(end_date, datetime.min.time()),
            status=WeatherImportStatus.PENDING,
            created_by_id=user_id,
            job_metadata={
                'total_dates': total_dates,
                'dates_completed': 0,
                'current_phase': 'pending',
            },
            created_at=datetime.now(timezone.utc).replace(tzinfo=None),
            updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )

        self.db.add(job)
        await self.db.commit()
        await self.db.refresh(job)

        logger.info(
            f"Created weather import job",
            job_id=job.id,
            date_range=f"{start_date} to {end_date}",
            total_dates=total_dates
        )
        return job

    async def execute_job_async(self, job_id: int):
        """
        Execute job asynchronously (fire-and-forget).

        This method is called via asyncio.create_task() and handles
        all errors internally, updating the job status in the database.

        Args:
            job_id: Job ID to execute
        """
        try:
            await self.execute_job(job_id)
        except Exception as e:
            logger.error(f"Background job {job_id} failed", error=str(e))
            # Update job as failed
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as db:
                job = await db.get(WeatherImportJob, job_id)
                if job:
                    job.mark_failed(str(e))
                    await db.commit()

    async def execute_job(self, job_id: int) -> WeatherImportJob:
        """
        Execute weather import job (blocking).

        This runs the import using the core weather import module.
        Creates its own database sessions as needed.

        Args:
            job_id: Job ID to execute

        Returns:
            Updated WeatherImportJob
        """
        from app.core.weather_import import WeatherImportCore

        # Create a new session for this job execution
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            # Get job and mark as running
            job = await db.get(WeatherImportJob, job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")

            job.mark_running()
            await db.commit()

            start_date = job.import_start_date.date()
            end_date = job.import_end_date.date()

        logger.info(
            f"Executing weather import job",
            job_id=job_id,
            start_date=start_date,
            end_date=end_date
        )

        try:
            # Run weather import using core module
            weather_import = WeatherImportCore()
            stats = await weather_import.fetch_and_process_date_range(
                start_date=start_date,
                end_date=end_date,
                job_id=job_id
            )

            # Create new session to update results
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as new_db:
                # Re-fetch job in new session
                job = await new_db.get(WeatherImportJob, job_id)

                if stats.get('errors'):
                    # Partial failure - some dates failed
                    error_msg = "; ".join(stats['errors'][:5])  # First 5 errors
                    if len(stats['errors']) > 5:
                        error_msg += f" ... and {len(stats['errors']) - 5} more"

                    job.mark_failed(error_msg)
                    logger.error(
                        f"Job completed with errors",
                        job_id=job_id,
                        errors=len(stats['errors']),
                        **{k: v for k, v in stats.items() if k != 'errors'}
                    )
                else:
                    # Full success
                    job.mark_success(
                        records_imported=stats['records'],
                        files_downloaded=stats['files_downloaded'],
                        files_deleted=stats['files_deleted'],
                        api_calls=stats['api_calls'],
                    )
                    logger.info(f"Job completed successfully", job_id=job_id, **stats)

                await new_db.commit()
                return job

        except Exception as e:
            # Unexpected error
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as new_db:
                job = await new_db.get(WeatherImportJob, job_id)
                error_msg = str(e)[:1000]
                job.mark_failed(error_msg)
                await new_db.commit()
                logger.error(f"Job failed with exception", job_id=job_id, error=str(e))
                return job


    async def get_job_by_id(self, job_id: int) -> Optional[WeatherImportJob]:
        """
        Get job by ID.

        Args:
            job_id: Job ID

        Returns:
            WeatherImportJob or None if not found
        """
        return await self.db.get(WeatherImportJob, job_id)

    async def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[WeatherImportJob]:
        """
        List jobs with optional filtering.

        Args:
            status: Optional status filter
            limit: Max results
            offset: Results offset

        Returns:
            List of WeatherImportJob
        """
        query = select(WeatherImportJob).order_by(WeatherImportJob.created_at.desc())

        if status:
            query = query.where(WeatherImportJob.status == status)

        query = query.limit(limit).offset(offset)

        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def cancel_job(self, job_id: int) -> WeatherImportJob:
        """
        Cancel a running job.

        Note: Currently just marks as cancelled in DB. Process termination
        would require tracking PIDs, which adds complexity.

        Args:
            job_id: Job ID to cancel

        Returns:
            Updated WeatherImportJob
        """
        job = await self.db.get(WeatherImportJob, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        if job.status == WeatherImportStatus.RUNNING:
            job.mark_cancelled()
            await self.db.commit()
            logger.info(f"Cancelled job", job_id=job_id)

        return job
