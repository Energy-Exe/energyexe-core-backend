"""Service for managing scheduled import job executions."""

import subprocess
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from pathlib import Path

import structlog
from sqlalchemy import and_, desc, func, select, Integer
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.models.import_job_execution import (
    ImportJobExecution,
    ImportJobStatus,
    ImportJobType,
)
from app.schemas.import_job import (
    ImportJobCreate,
    ImportJobFilter,
    ImportJobHealth,
    ImportJobSummary,
    ImportJobResponse,
)

logger = structlog.get_logger()


class ImportJobService:
    """Service for import job management."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_job(
        self,
        request: ImportJobCreate,
        user_id: Optional[int] = None,
        job_type: ImportJobType = ImportJobType.MANUAL,
    ) -> ImportJobExecution:
        """
        Create a new import job execution record.

        Args:
            request: Job creation request
            user_id: User ID if manually created
            job_type: Type of job (scheduled or manual)

        Returns:
            Created job execution
        """
        # Generate job name
        job_name = f"{request.source.lower()}-{job_type.value}"

        job = ImportJobExecution(
            job_name=job_name,
            source=request.source,
            job_type=job_type,
            import_start_date=request.import_start_date,
            import_end_date=request.import_end_date,
            status=ImportJobStatus.PENDING,
            job_metadata=request.job_metadata or {},
            created_by_id=user_id,
        )

        self.db.add(job)
        await self.db.commit()
        await self.db.refresh(job)

        logger.info(
            "Created import job",
            job_id=job.id,
            source=request.source,
            job_type=job_type,
        )

        return job

    async def execute_job(self, job_id: int) -> ImportJobExecution:
        """
        Execute an import job by running the appropriate import script.

        Args:
            job_id: ID of job to execute

        Returns:
            Updated job with execution results
        """
        # Get job and mark as running
        result = await self.db.execute(select(ImportJobExecution).where(ImportJobExecution.id == job_id))
        job = result.scalar_one_or_none()

        if not job:
            raise ValueError(f"Job {job_id} not found")

        if job.status == ImportJobStatus.RUNNING:
            raise ValueError("Job is already running")

        # Mark as running and commit
        job.mark_running()
        await self.db.commit()

        # Close this session - subprocess will take a long time
        await self.db.close()

        try:
            # Build and execute command
            command = self._build_import_command(job)

            logger.info(
                "Executing import job",
                job_id=job.id,
                command=command,
            )

            # Run command (this can take minutes)
            process_result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )

            # Parse results from output
            records_imported, records_updated, api_calls = self._parse_import_output(
                process_result.stdout
            )

            # Create NEW session to update job (old session is closed)
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as new_db:
                # Re-fetch job in new session
                result = await new_db.execute(
                    select(ImportJobExecution).where(ImportJobExecution.id == job_id)
                )
                job = result.scalar_one_or_none()

                if not job:
                    raise ValueError(f"Job {job_id} not found after execution")

                if process_result.returncode == 0:
                    job.mark_success(records_imported, records_updated, api_calls)
                    logger.info(
                        "Job completed successfully",
                        job_id=job.id,
                        records=records_imported,
                    )
                else:
                    job.mark_failed(process_result.stderr[:1000])
                    logger.error(
                        "Job failed",
                        job_id=job.id,
                        error=process_result.stderr[:500],
                    )

                await new_db.commit()
                await new_db.refresh(job)

                return job

        except subprocess.TimeoutExpired:
            # Create new session for timeout update
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as new_db:
                result = await new_db.execute(
                    select(ImportJobExecution).where(ImportJobExecution.id == job_id)
                )
                job = result.scalar_one_or_none()
                if job:
                    job.mark_failed("Job timeout after 1 hour")
                    await new_db.commit()
                    await new_db.refresh(job)

            logger.error("Job timeout", job_id=job_id)
            return job

        except Exception as e:
            # Create new session for error update
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as new_db:
                result = await new_db.execute(
                    select(ImportJobExecution).where(ImportJobExecution.id == job_id)
                )
                job = result.scalar_one_or_none()
                if job:
                    job.mark_failed(str(e))
                    await new_db.commit()
                    await new_db.refresh(job)

            logger.error("Job execution error", job_id=job_id, error=str(e))
            return job

    async def retry_job(self, job_id: int, reset_retry_count: bool = False) -> ImportJobExecution:
        """
        Retry a failed job.

        Args:
            job_id: ID of job to retry
            reset_retry_count: Whether to reset retry counter

        Returns:
            Updated job
        """
        # Get job in current session
        result = await self.db.execute(select(ImportJobExecution).where(ImportJobExecution.id == job_id))
        job = result.scalar_one_or_none()

        if not job:
            raise ValueError(f"Job {job_id} not found")

        if not job.can_retry() and not reset_retry_count:
            raise ValueError(
                f"Job cannot be retried (status: {job.status}, retries: {job.retry_count}/{job.max_retries})"
            )

        if reset_retry_count:
            job.retry_count = 0

        job.retry_count += 1
        job.status = ImportJobStatus.PENDING
        job.error_message = None
        job.started_at = None
        job.completed_at = None
        job.duration_seconds = None

        await self.db.commit()

        # Execute the retry
        return await self.execute_job(job_id)

    async def get_jobs(
        self,
        filters: ImportJobFilter,
    ) -> Tuple[List[ImportJobExecution], int]:
        """
        Get import job executions with filtering and pagination.

        Args:
            filters: Filter criteria

        Returns:
            Tuple of (jobs list, total count)
        """
        # Build query
        stmt = select(ImportJobExecution)

        # Apply filters
        conditions = []
        if filters.source:
            conditions.append(ImportJobExecution.source == filters.source)
        if filters.status:
            conditions.append(ImportJobExecution.status == filters.status)
        if filters.job_type:
            conditions.append(ImportJobExecution.job_type == filters.job_type)
        if filters.start_date:
            conditions.append(ImportJobExecution.created_at >= filters.start_date)
        if filters.end_date:
            conditions.append(
                ImportJobExecution.created_at
                <= datetime.combine(filters.end_date, datetime.max.time())
            )

        if conditions:
            stmt = stmt.where(and_(*conditions))

        # Get total count
        count_stmt = select(func.count()).select_from(stmt.alias())
        count_result = await self.db.execute(count_stmt)
        total = count_result.scalar() or 0

        # Apply pagination and ordering
        stmt = stmt.order_by(desc(ImportJobExecution.created_at))
        stmt = stmt.limit(filters.limit).offset(filters.offset)

        # Execute query
        result = await self.db.execute(stmt)
        jobs = result.scalars().all()

        return list(jobs), total

    async def get_latest_status_per_job(self) -> List[ImportJobSummary]:
        """
        Get latest execution status for each job name.

        Returns:
            List of job summaries
        """
        # Get unique job names
        job_names_result = await self.db.execute(
            select(ImportJobExecution.job_name).distinct()
        )
        job_names = [row[0] for row in job_names_result]

        summaries = []

        for job_name in job_names:
            # Get latest execution
            latest_result = await self.db.execute(
                select(ImportJobExecution)
                .where(ImportJobExecution.job_name == job_name)
                .order_by(desc(ImportJobExecution.created_at))
                .limit(1)
            )
            latest = latest_result.scalar_one_or_none()

            if not latest:
                continue

            # Get statistics
            stats_result = await self.db.execute(
                select(
                    func.count(ImportJobExecution.id),
                    func.sum(
                        func.cast(
                            ImportJobExecution.status == ImportJobStatus.SUCCESS,
                            Integer
                        )
                    ),
                    func.sum(
                        func.cast(
                            ImportJobExecution.status == ImportJobStatus.FAILED,
                            Integer
                        )
                    ),
                )
                .where(ImportJobExecution.job_name == job_name)
            )
            total, success, failed = stats_result.first()

            # Calculate last 24h success rate
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            recent_result = await self.db.execute(
                select(
                    func.count(ImportJobExecution.id),
                    func.sum(
                        func.cast(
                            ImportJobExecution.status == ImportJobStatus.SUCCESS,
                            Integer
                        )
                    ),
                )
                .where(ImportJobExecution.job_name == job_name)
                .where(ImportJobExecution.created_at >= yesterday.replace(tzinfo=None))
            )
            recent_total, recent_success = recent_result.first()
            success_rate = (
                (recent_success / recent_total * 100) if recent_total and recent_total > 0 else 0
            )

            # Calculate next scheduled run (based on job name pattern)
            next_run = self._calculate_next_run(job_name, latest.completed_at or latest.created_at)

            summary = ImportJobSummary(
                job_name=job_name,
                source=latest.source,
                last_execution=ImportJobResponse.model_validate(latest),
                next_scheduled_run=next_run,
                total_executions=total or 0,
                success_count=success or 0,
                failed_count=failed or 0,
                last_24h_success_rate=success_rate,
            )

            summaries.append(summary)

        return summaries

    async def get_system_health(self) -> ImportJobHealth:
        """
        Get overall system health status.

        Returns:
            Health status summary
        """
        # Count running jobs
        running_result = await self.db.execute(
            select(func.count(ImportJobExecution.id)).where(
                ImportJobExecution.status == ImportJobStatus.RUNNING
            )
        )
        running_count = running_result.scalar() or 0

        # Count recent failures (last 24h)
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        failures_result = await self.db.execute(
            select(func.count(ImportJobExecution.id))
            .where(ImportJobExecution.status == ImportJobStatus.FAILED)
            .where(ImportJobExecution.created_at >= yesterday.replace(tzinfo=None))
        )
        recent_failures = failures_result.scalar() or 0

        # Total unique jobs
        jobs_result = await self.db.execute(
            select(func.count(func.distinct(ImportJobExecution.job_name)))
        )
        total_jobs = jobs_result.scalar() or 0

        # Determine health
        if recent_failures == 0 and running_count < 5:
            health = "healthy"
        elif recent_failures < 3:
            health = "degraded"
        else:
            health = "critical"

        return ImportJobHealth(
            total_jobs=total_jobs,
            running_jobs=running_count,
            recent_failures=recent_failures,
            jobs_behind_schedule=[],  # TODO: Implement schedule checking
            overall_health=health,
            last_updated=datetime.now(timezone.utc),
        )

    def _build_import_command(self, job: ImportJobExecution) -> str:
        """Build the command to execute based on source."""
        base_path = Path(__file__).parent.parent.parent / "scripts/seeds/raw_generation_data"

        start_date = job.import_start_date.strftime("%Y-%m-%d")
        end_date = job.import_end_date.strftime("%Y-%m-%d")

        # Use python directly (works in Docker without poetry)
        # The app is already in PYTHONPATH when FastAPI starts
        commands = {
            "ENTSOE": f"python {base_path}/entsoe/import_from_api.py --start {start_date} --end {end_date}",
            "Taipower": f"python {base_path}/taipower/import_from_api.py",
            "ELEXON": f"python {base_path}/elexon/import_from_api.py --start {start_date} --end {end_date}",
            "EIA": f"python {base_path}/eia/import_from_api.py --start-year {job.import_start_date.year} --start-month {job.import_start_date.month} --end-year {job.import_end_date.year} --end-month {job.import_end_date.month}",
        }

        command = commands.get(job.source)
        if not command:
            raise ValueError(f"Unknown source: {job.source}")

        return command

    def _parse_import_output(self, output: str) -> Tuple[int, int, int]:
        """Parse import script output to extract results."""
        records_imported = 0
        records_updated = 0
        api_calls = 0

        # Parse common patterns
        for line in output.split("\n"):
            if "Total Records Stored:" in line or "Records Stored:" in line:
                try:
                    records_imported = int(line.split(":")[-1].strip().replace(",", ""))
                except ValueError:
                    pass
            elif "Total API Calls:" in line or "API Calls:" in line:
                try:
                    api_calls = int(line.split(":")[-1].strip())
                except ValueError:
                    pass
            elif "Records Updated:" in line:
                try:
                    records_updated = int(line.split(":")[-1].strip().replace(",", ""))
                except ValueError:
                    pass

        return records_imported, records_updated, api_calls

    def _calculate_next_run(self, job_name: str, last_run: datetime) -> Optional[datetime]:
        """Calculate next scheduled run time based on job name."""
        now = datetime.now(timezone.utc)

        if "daily" in job_name:
            # Daily jobs run at specific hours
            if "entsoe" in job_name:
                next_run = now.replace(hour=6, minute=0, second=0, microsecond=0)
            elif "elexon" in job_name:
                next_run = now.replace(hour=7, minute=0, second=0, microsecond=0)
            else:
                next_run = now.replace(hour=6, minute=0, second=0, microsecond=0)

            # If today's time has passed, schedule for tomorrow
            if next_run <= now:
                next_run += timedelta(days=1)

        elif "hourly" in job_name:
            # Hourly jobs run at :05 minutes
            next_run = now.replace(minute=5, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(hours=1)

        elif "monthly" in job_name:
            # Monthly jobs run on 1st at 2 AM
            next_run = now.replace(day=1, hour=2, minute=0, second=0, microsecond=0)
            # Next month
            if next_run <= now:
                if now.month == 12:
                    next_run = next_run.replace(year=now.year + 1, month=1)
                else:
                    next_run = next_run.replace(month=now.month + 1)

        else:
            # Manual jobs don't have next run
            return None

        return next_run

    async def get_job_by_id(self, job_id: int) -> Optional[ImportJobExecution]:
        """Get job by ID."""
        result = await self.db.execute(
            select(ImportJobExecution).where(ImportJobExecution.id == job_id)
        )
        return result.scalar_one_or_none()
