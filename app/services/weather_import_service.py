"""Service layer for weather data import job execution."""

import asyncio
import subprocess
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

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

        This runs the import script as a subprocess, parses the output,
        and updates the job status in the database.

        Args:
            job_id: Job ID to execute

        Returns:
            Updated WeatherImportJob
        """
        # Get job and mark as running
        job = await self.db.get(WeatherImportJob, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job.mark_running()
        await self.db.commit()

        # Close session before long-running subprocess
        await self.db.close()

        # Build command
        command = self._build_command(job)

        logger.info(
            f"Executing weather import job",
            job_id=job_id,
            command=command
        )

        try:
            # Run subprocess with 2-hour timeout
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=7200,  # 2 hours for weather jobs
            )

            # Create new session to update results
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as new_db:
                # Re-fetch job in new session
                job = await new_db.get(WeatherImportJob, job_id)

                if result.returncode == 0:
                    # Parse output
                    stats = self._parse_output(result.stdout)
                    job.mark_success(
                        records_imported=stats['records'],
                        files_downloaded=stats['files_downloaded'],
                        files_deleted=stats['files_deleted'],
                        api_calls=stats['api_calls'],
                    )
                    logger.info(f"Job completed successfully", job_id=job_id, **stats)
                else:
                    error_msg = result.stderr[:1000] if result.stderr else "Unknown error"
                    job.mark_failed(error_msg)
                    logger.error(f"Job failed", job_id=job_id, error=error_msg)

                await new_db.commit()
                return job

        except subprocess.TimeoutExpired:
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as new_db:
                job = await new_db.get(WeatherImportJob, job_id)
                job.mark_failed("Job timeout after 2 hours")
                await new_db.commit()
                logger.error(f"Job timeout", job_id=job_id)
                return job

    def _build_command(self, job: WeatherImportJob) -> str:
        """
        Build command to execute import script.

        Args:
            job: WeatherImportJob to execute

        Returns:
            Shell command string
        """
        script_path = Path(__file__).parent.parent.parent / "scripts" / "seeds" / "weather_data" / "fetch_daily_all_windfarms.py"

        start_date = job.import_start_date.strftime('%Y-%m-%d')
        end_date = job.import_end_date.strftime('%Y-%m-%d')

        command = (
            f"python {script_path} "
            f"--start {start_date} "
            f"--end {end_date} "
            f"--job-id {job.id}"
        )

        return command

    def _parse_output(self, output: str) -> dict:
        """
        Parse script output to extract statistics.

        Looks for structured output lines like:
        - RECORDS: 38184
        - FILES_DOWNLOADED: 1
        - FILES_DELETED: 1
        - API_CALLS: 1

        Args:
            output: Script stdout

        Returns:
            Dict with records, files_downloaded, files_deleted, api_calls
        """
        records = 0
        files_downloaded = 0
        files_deleted = 0
        api_calls = 0

        for line in output.split("\n"):
            if "RECORDS:" in line:
                try:
                    records += int(line.split(":")[-1].strip())
                except ValueError:
                    pass
            elif "FILES_DOWNLOADED:" in line:
                try:
                    files_downloaded += int(line.split(":")[-1].strip())
                except ValueError:
                    pass
            elif "FILES_DELETED:" in line:
                try:
                    files_deleted += int(line.split(":")[-1].strip())
                except ValueError:
                    pass
            elif "API_CALLS:" in line:
                try:
                    api_calls += int(line.split(":")[-1].strip())
                except ValueError:
                    pass

        return {
            'records': records,
            'files_downloaded': files_downloaded,
            'files_deleted': files_deleted,
            'api_calls': api_calls,
        }

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
