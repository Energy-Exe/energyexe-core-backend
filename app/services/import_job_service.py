"""Service for managing scheduled import job executions."""

import asyncio
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import structlog
from sqlalchemy import and_, desc, func, or_, select, update, Integer
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

# A trigger for a job key that already has a PENDING/RUNNING row younger than
# this is a duplicate (Lambda retry, double click) and reuses that row instead
# of starting a second import. Covers the longest import (~4 min) plus the
# Lambda's async-retry spread (<= 30 min). Anything older is treated as wedged:
# the 1 h subprocess timeout or the boot sweeper will fail it.
INFLIGHT_GUARD_WINDOW = timedelta(minutes=30)
SUBPROCESS_TIMEOUT_SECONDS = 3600
_INFLIGHT_STATUSES = (ImportJobStatus.PENDING, ImportJobStatus.RUNNING)


def _utcnow() -> datetime:
    """Naive UTC, matching the DateTime columns on import_job_executions."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class ImportSchedule:
    """One EventBridge cron from infra/scheduled_imports.tf, expressed in UTC.

    ``hour=None`` means hourly; ``day_of_month`` set means monthly;
    ``weekdays_only`` maps to the ``MON-FRI`` day-of-week field.
    """

    minute: int
    hour: Optional[int] = None
    day_of_month: Optional[int] = None
    weekdays_only: bool = False

    def to_cron(self) -> str:
        """The exact EventBridge expression, so a test can diff it against the .tf."""
        if self.hour is None:
            return f"cron({self.minute} * * * ? *)"
        if self.day_of_month is not None:
            return f"cron({self.minute} {self.hour} {self.day_of_month} * ? *)"
        if self.weekdays_only:
            return f"cron({self.minute} {self.hour} ? * MON-FRI *)"
        return f"cron({self.minute} {self.hour} * * ? *)"

    def next_run(self, now: Optional[datetime] = None) -> datetime:
        """First firing strictly after ``now`` (tz-aware UTC)."""
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)

        if self.hour is None:
            candidate = now.replace(minute=self.minute, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(hours=1)
            return candidate

        if self.day_of_month is not None:
            candidate = now.replace(
                day=self.day_of_month, hour=self.hour, minute=self.minute,
                second=0, microsecond=0,
            )
            if candidate <= now:
                year, month = (now.year + 1, 1) if now.month == 12 else (now.year, now.month + 1)
                candidate = candidate.replace(year=year, month=month)
            return candidate

        candidate = now.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)
        if self.weekdays_only:
            while candidate.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
                candidate += timedelta(days=1)
        return candidate


# Mirror of local.import_schedules in infra/scheduled_imports.tf, keyed by the
# EventBridge job name (== job_metadata["job_config"] on the execution row).
# The daily batch runs ~midnight Oslo (22:10-22:55 UTC) so the worker-blocking
# imports never coincide with Norwegian working hours; see the .tf header.
# tests/test_import_schedules.py fails when this table and the .tf drift.
IMPORT_SCHEDULES: Dict[str, ImportSchedule] = {
    "taipower-hourly": ImportSchedule(minute=5),
    "entsoe-daily": ImportSchedule(minute=10, hour=22),
    "elexon-daily": ImportSchedule(minute=20, hour=22),
    "entsoe-prices-daily": ImportSchedule(minute=30, hour=22),
    "elexon-prices-daily": ImportSchedule(minute=40, hour=22),
    "ecb-rates-daily": ImportSchedule(minute=50, hour=22, weekdays_only=True),
    "eia-monthly": ImportSchedule(minute=55, hour=22, day_of_month=1),
}


def next_scheduled_run(
    job_config: Optional[str], now: Optional[datetime] = None
) -> Optional[datetime]:
    """Next EventBridge firing for a scheduled job key, or None for unscheduled jobs."""
    schedule = IMPORT_SCHEDULES.get(job_config or "")
    return schedule.next_run(now) if schedule else None


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

        # Remove timezone info from dates (database expects naive datetime)
        import_start = request.import_start_date.replace(tzinfo=None) if request.import_start_date.tzinfo else request.import_start_date
        import_end = request.import_end_date.replace(tzinfo=None) if request.import_end_date.tzinfo else request.import_end_date

        job = ImportJobExecution(
            job_name=job_name,
            source=request.source,
            job_type=job_type,
            import_start_date=import_start,
            import_end_date=import_end,
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

            # Run the command (minutes) on a worker thread. subprocess.run is a
            # blocking C call; awaited inline it froze the single uvicorn
            # worker's event loop for the whole import, so /health timed out,
            # the ALB marked the task unhealthy after 150 s and ECS killed it
            # (prod, twice a day, until 2026-09-05). Off the loop, the API and
            # the Brain-agent SSE streams keep serving while the import runs.
            process_result = await asyncio.to_thread(self._run_import_command, command)

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

    @staticmethod
    def _run_import_command(command: str) -> subprocess.CompletedProcess:
        """Blocking; only ever called via asyncio.to_thread from execute_job."""
        return subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT_SECONDS,
        )

    async def find_inflight_scheduled(
        self, job_config: str, window: timedelta = INFLIGHT_GUARD_WINDOW
    ) -> Optional[ImportJobExecution]:
        """Newest PENDING/RUNNING row for an EventBridge job key created inside ``window``.

        PENDING is included so the create -> mark_running gap cannot be raced by
        a concurrent trigger. Keyed on job_metadata["job_config"] because the DB
        job_name ("entsoe-scheduled") is shared by manual and scheduled runs.
        """
        cutoff = _utcnow() - window
        stmt = (
            select(ImportJobExecution)
            .where(
                ImportJobExecution.status.in_(_INFLIGHT_STATUSES),
                ImportJobExecution.job_metadata["job_config"].as_string() == job_config,
                ImportJobExecution.created_at >= cutoff,
            )
            .order_by(desc(ImportJobExecution.created_at))
            .limit(1)
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

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

            # Next scheduled run — keyed by the EventBridge job name stored on the
            # row by the trigger endpoint (the DB job_name is "<source>-scheduled"
            # and carries no schedule information).
            metadata = latest.job_metadata if isinstance(latest.job_metadata, dict) else {}
            next_run = next_scheduled_run(metadata.get("job_config"))

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
        """Build the command to execute based on source.

        Each command chains raw data import with aggregation using &&.
        This ensures aggregation only runs if raw import succeeds.

        Sources:
        - ENTSOE, ELEXON, Taipower: Hourly data → process_generation_data_robust.py
        - EIA: Monthly data → process_generation_data_monthly.py
        """
        base_path = Path(__file__).parent.parent.parent / "scripts/seeds/raw_generation_data"
        agg_path = Path(__file__).parent.parent.parent / "scripts/seeds/aggregate_generation_data"
        prices_path = Path(__file__).parent.parent.parent / "scripts/seeds/power_prices"
        rates_path = Path(__file__).parent.parent.parent / "scripts/seeds/exchange_rates"

        start_date = job.import_start_date.strftime("%Y-%m-%d")
        end_date = job.import_end_date.strftime("%Y-%m-%d")
        # The price PROCESS step filters `period_start < end_date` (exclusive — see
        # PriceProcessingService._get_raw_prices_for_bidzone), so it needs the day
        # AFTER the last day we want included. Without this a single-day job asks
        # for `>= D AND < D` and silently writes zero rows to price_data.
        # The FETCH scripts pad --end to 23:59:59 themselves, so they keep end_date.
        #
        # NOT app.utils.date_bounds.exclusive_end() — that returns +1 microsecond
        # for a timestamped end, which disappears under %Y-%m-%d formatting and
        # restores the bug. This value is serialized to a DATE and re-parsed as
        # midnight downstream, so the bound has to be the next calendar day.
        prices_end_date = (job.import_end_date + timedelta(days=1)).strftime("%Y-%m-%d")

        # Use python directly (works in Docker without poetry)
        # The app is already in PYTHONPATH when FastAPI starts
        # Each command: raw import && aggregation (aggregation runs only if import succeeds)
        commands = {
            "ENTSOE": (
                f"python {base_path}/entsoe/import_from_api.py --start {start_date} --end {end_date} && "
                f"python {agg_path}/process_generation_data_robust.py --source ENTSOE --start {start_date} --end {end_date}"
            ),
            "Taipower": (
                f"python {base_path}/taipower/import_from_api.py && "
                f"python {agg_path}/process_generation_data_robust.py --source TAIPOWER --start {start_date} --end {end_date}"
            ),
            "ELEXON": (
                f"python {base_path}/elexon/import_from_api.py --start {start_date} --end {end_date} && "
                f"python {base_path}/elexon/import_acceptance_volumes.py --start {start_date} --end {end_date} && "
                f"python {agg_path}/process_generation_data_robust.py --source ELEXON --start {start_date} --end {end_date}"
            ),
            "EIA": (
                f"python {base_path}/eia/import_from_api.py --start-year {job.import_start_date.year} "
                f"--start-month {job.import_start_date.month} --end-year {job.import_end_date.year} "
                f"--end-month {job.import_end_date.month} && "
                f"python {agg_path}/process_generation_data_monthly.py --source EIA "
                f"--start {job.import_start_date.strftime('%Y-%m')} --end {job.import_end_date.strftime('%Y-%m')}"
            ),
            "ENTSOE_PRICES": (
                f"python {prices_path}/import_prices_from_api.py --start {start_date} --end {end_date} --price-types day_ahead && "
                f"python {prices_path}/process_to_hourly.py --start-date {start_date} --end-date {prices_end_date} --force"
            ),
            "ELEXON_PRICES": (
                f"python {prices_path}/elexon/import_elexon_prices.py --start {start_date} --end {end_date} && "
                f"python {prices_path}/process_to_hourly.py --source ELEXON --bidzone-codes 10YGB----------A "
                f"--start-date {start_date} --end-date {prices_end_date} --force"
            ),
            "ECB_RATES": (
                f"python {rates_path}/import_ecb_rates.py --start {start_date} --end {end_date}"
            ),
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

    async def get_job_by_id(self, job_id: int) -> Optional[ImportJobExecution]:
        """Get job by ID."""
        result = await self.db.execute(
            select(ImportJobExecution).where(ImportJobExecution.id == job_id)
        )
        return result.scalar_one_or_none()


async def sweep_stuck_import_jobs() -> int:
    """Startup sweeper: fail import rows orphaned by a task death.

    Mirrors app.services.reports.orchestrator.sweep_stuck_reports. Imports run
    inside the API process (execute_job -> subprocess on a worker thread), so
    a task kill or deploy mid-import leaves the row PENDING/RUNNING forever:
    the in-flight guard then keeps returning it and the health status counts
    it as running. No time cutoff: the service deploys with min-healthy 0% /
    max 100%, so the previous process is dead before this one boots and every
    in-process row is an orphan. Only rows this process would own are touched:
    external triggers (job_metadata.trigger == "external") and the in-process
    opportunity-detection job. The nightly performance pipeline writes RUNNING
    rows into the same table from its own ECS task and is left alone.

    Multi-task caveat: with >1 API task (or a >100% rolling deploy) this would
    fail another task's live import; that needs a task-identity/heartbeat
    column, not a cutoff.
    """
    factory = get_session_factory()
    now = _utcnow()
    async with factory() as db:
        result = await db.execute(
            update(ImportJobExecution)
            .where(
                ImportJobExecution.status.in_(_INFLIGHT_STATUSES),
                or_(
                    ImportJobExecution.job_metadata["trigger"].as_string() == "external",
                    ImportJobExecution.job_name == "opportunity-detection",
                ),
            )
            .values(
                status=ImportJobStatus.FAILED,
                error_message="Orphaned by service restart",
                completed_at=now,
            )
            .returning(ImportJobExecution.id, ImportJobExecution.job_name)
        )
        rows = result.all()
        await db.commit()
    if rows:
        logger.warning(
            "import_job_sweeper_failed_orphans",
            count=len(rows),
            jobs=[(row[0], row[1]) for row in rows],
        )
    return len(rows)
