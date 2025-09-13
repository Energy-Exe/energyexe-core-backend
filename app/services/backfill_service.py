"""Service for managing data backfill operations."""

from datetime import datetime, timezone
from typing import List, Optional
from calendar import monthrange

import pandas as pd
import structlog
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.backfill_job import BackfillJob, BackfillTask, BackfillJobStatus, BackfillTaskStatus
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm
from app.models.user import User
from app.models.generation_data import GenerationDataRaw, GenerationData
from app.schemas.backfill import (
    BackfillJobCreate,
    BackfillPreview,
    DataAvailability,
    DataAvailabilityResponse,
)
from app.services.entsoe_client import ENTSOEClient
from app.services.elexon_client import ElexonClient

logger = structlog.get_logger()


class BackfillService:
    """Service for managing data backfill operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_backfill_job(
        self,
        request: BackfillJobCreate,
        current_user: User,
        use_celery: bool = True,
    ) -> BackfillJob:
        """Create a new backfill job with tasks and optionally queue for async processing."""
        # Get windfarm with generation units
        stmt = (
            select(Windfarm)
            .options(selectinload(Windfarm.generation_units))
            .where(Windfarm.id == request.windfarm_id)
        )
        result = await self.db.execute(stmt)
        windfarm = result.scalar_one_or_none()
        
        if not windfarm:
            raise ValueError(f"Windfarm {request.windfarm_id} not found")
        
        if not windfarm.generation_units:
            raise ValueError(f"No generation units found for windfarm {windfarm.name}")
        
        # Create date range
        start_date = datetime(request.start_year, 1, 1)
        end_date = datetime(request.end_year, 12, 31, 23, 59, 59)
        
        # Filter generation units by source if specified
        generation_units = windfarm.generation_units
        if request.sources:
            generation_units = [
                gu for gu in generation_units 
                if gu.source.lower() in [s.lower() for s in request.sources]
            ]
        
        if not generation_units:
            raise ValueError(f"No generation units found with sources {request.sources}")
        
        # Create backfill job
        job = BackfillJob(
            windfarm_id=request.windfarm_id,
            start_date=start_date,
            end_date=end_date,
            status=BackfillJobStatus.PENDING,
            total_tasks=0,
            completed_tasks=0,
            failed_tasks=0,
            created_by_id=current_user.id,
            job_metadata={
                "sources": request.sources or [gu.source for gu in generation_units],
                "windfarm_name": windfarm.name,
                "windfarm_code": windfarm.code,
                "use_celery": use_celery,
            }
        )
        self.db.add(job)
        await self.db.flush()
        
        # Create tasks for each generation unit and month
        tasks = []
        current_date = start_date
        
        while current_date <= end_date:
            # Get the last day of the current month
            last_day = monthrange(current_date.year, current_date.month)[1]
            month_end = current_date.replace(day=last_day, hour=23, minute=59, second=59)
            
            # Don't go beyond the end date
            if month_end > end_date:
                month_end = end_date
            
            for generation_unit in generation_units:
                task = BackfillTask(
                    job_id=job.id,
                    generation_unit_id=generation_unit.id,
                    source=generation_unit.source,
                    start_date=current_date,
                    end_date=month_end,
                    status=BackfillTaskStatus.PENDING,
                    attempt_count=0,
                    max_attempts=3,
                    task_metadata={
                        "generation_unit_code": generation_unit.code,
                        "generation_unit_name": generation_unit.name,
                        "year": current_date.year,
                        "month": current_date.month,
                    }
                )
                tasks.append(task)
            
            # Move to next month
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)
        
        # Add all tasks
        for task in tasks:
            self.db.add(task)
        
        # Update job total tasks
        job.total_tasks = len(tasks)
        
        await self.db.commit()
        
        # Refresh the job and eagerly load tasks to avoid detached instance issues
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job.id)
        )
        result = await self.db.execute(stmt)
        job = result.scalar_one()
        
        logger.info(f"Created backfill job {job.id} with {job.total_tasks} tasks")
        
        # Queue for Celery processing or process synchronously
        if use_celery:
            # Import here to avoid circular dependency
            from app.tasks.backfill import process_backfill_job
            
            # Queue the job for async processing
            celery_task = process_backfill_job.delay(job.id)
            
            # Store the Celery task ID
            job.celery_task_id = celery_task.id
            job.status = BackfillJobStatus.PENDING
            await self.db.commit()
            
            logger.info(f"Queued backfill job {job.id} with Celery task ID {celery_task.id}")
            
            # Refresh job with tasks after commit
            stmt = (
                select(BackfillJob)
                .options(selectinload(BackfillJob.tasks))
                .where(BackfillJob.id == job.id)
            )
            result = await self.db.execute(stmt)
            job = result.scalar_one()
        else:
            # Fallback to synchronous processing (for backwards compatibility)
            try:
                logger.info(f"Starting synchronous processing of job {job.id}")
                await self.process_backfill_job(job.id)
            except Exception as e:
                logger.error(f"Error during synchronous processing of job {job.id}: {str(e)}")
            
            # Refresh the job to get the latest status
            await self.db.refresh(job)
        
        return job

    async def process_backfill_job(self, job_id: int) -> BackfillJob:
        """Process a backfill job synchronously."""
        try:
            # Get job with tasks
            stmt = (
                select(BackfillJob)
                .options(selectinload(BackfillJob.tasks))
                .where(BackfillJob.id == job_id)
            )
            result = await self.db.execute(stmt)
            job = result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Database error when fetching job {job_id}: {str(e)}")
            # Try to reconnect/refresh the session
            await self.db.rollback()
            stmt = (
                select(BackfillJob)
                .options(selectinload(BackfillJob.tasks))
                .where(BackfillJob.id == job_id)
            )
            result = await self.db.execute(stmt)
            job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Check if job is already being processed
        if job.status == BackfillJobStatus.IN_PROGRESS:
            logger.warning(f"Job {job_id} is already in progress")
            return job
        
        # Update job status
        job.status = BackfillJobStatus.IN_PROGRESS
        # Store as naive UTC datetime for PostgreSQL TIMESTAMP WITHOUT TIME ZONE
        job.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        await self.db.commit()
        
        # Process tasks sequentially
        task_count = 0
        for task in job.tasks:
            if task.status != BackfillTaskStatus.PENDING:
                continue
            
            try:
                await self._process_backfill_task_with_db(task, self.db)
                job.completed_tasks += 1
            except Exception as e:
                logger.error(f"Error processing task {task.id}: {str(e)}")
                task.status = BackfillTaskStatus.FAILED
                task.error_message = str(e)
                job.failed_tasks += 1
            
            # Commit after each task
            await self.db.commit()
            
            # Every 5 tasks, refresh the session to avoid connection issues
            task_count += 1
            if task_count % 5 == 0:
                logger.info(f"Processed {task_count} tasks, refreshing database session")
                await self.db.commit()
                # Refresh job object to maintain session
                await self.db.refresh(job)
        
        # Update job status
        if job.failed_tasks > 0:
            job.status = BackfillJobStatus.PARTIALLY_COMPLETED
        else:
            job.status = BackfillJobStatus.COMPLETED
        
        job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        await self.db.commit()
        
        return job

    async def _process_single_task(self, task_id: int):
        """Process a single backfill task by ID."""
        # Get the task
        stmt = (
            select(BackfillTask)
            .options(selectinload(BackfillTask.generation_unit))
            .where(BackfillTask.id == task_id)
        )
        result = await self.db.execute(stmt)
        task = result.scalar_one_or_none()
        
        if not task:
            raise ValueError(f"Task {task_id} not found")
        
        await self._process_backfill_task_with_db(task, self.db)
    
    async def _process_backfill_task_with_db(self, task: BackfillTask, db: AsyncSession):
        """Process a single backfill task with provided database session."""
        # Check if the session is still active
        try:
            # Simple query to test connection
            await db.execute(select(1))
        except Exception as e:
            logger.warning(f"Database connection issue detected, attempting to recover: {str(e)}")
            await db.rollback()
        
        task.status = BackfillTaskStatus.IN_PROGRESS
        task.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        task.attempt_count += 1
        await db.commit()
        
        try:
            # Get generation unit
            stmt = select(GenerationUnit).where(GenerationUnit.id == task.generation_unit_id)
            result = await db.execute(stmt)
            generation_unit = result.scalar_one_or_none()
            
            if not generation_unit:
                raise ValueError(f"Generation unit {task.generation_unit_id} not found")
            
            # Fetch data based on source
            records_fetched = 0
            
            if task.source.lower() == "entsoe":
                records_fetched = await self._fetch_entsoe_data_with_db(generation_unit, task, db)
            elif task.source.lower() == "elexon":
                records_fetched = await self._fetch_elexon_data_with_db(generation_unit, task, db)
            elif task.source.lower() == "eia":
                records_fetched = await self._fetch_eia_data_with_db(generation_unit, task, db)
            elif task.source.lower() == "taipower":
                records_fetched = await self._fetch_taipower_data_with_db(generation_unit, task, db)
            else:
                raise ValueError(f"Unknown source: {task.source}")
            
            # Update task status
            task.status = BackfillTaskStatus.COMPLETED
            task.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            task.records_fetched = records_fetched
            await db.commit()
            
        except Exception as e:
            logger.error(f"Error in task {task.id}: {str(e)}")
            task.error_message = str(e)
            
            if task.attempt_count >= task.max_attempts:
                task.status = BackfillTaskStatus.FAILED
            else:
                task.status = BackfillTaskStatus.PENDING  # Will be retried
            
            await db.commit()
            raise
    
    async def _process_backfill_task(self, task: BackfillTask):
        """Process a single backfill task (legacy method for compatibility)."""
        await self._process_backfill_task_with_db(task, self.db)

    async def _fetch_entsoe_data_with_db(self, generation_unit: GenerationUnit, task: BackfillTask, db: AsyncSession) -> int:
        """Fetch ENTSOE data for a generation unit."""
        client = ENTSOEClient()
        
        # For ENTSOE, we need a valid EIC code for the unit
        if not generation_unit.code:
            raise ValueError(f"Generation unit {generation_unit.name} has no code")
        
        eic_code = generation_unit.code
        
        # Check if it's an EIC code (starts with digit and contains 'W')
        if eic_code[0].isdigit() and 'W' in eic_code:
            # This is an EIC code for a specific unit, we need to use per-unit fetching
            # Determine the control area based on the windfarm location
            # For Danish windfarms like Anholt, use DK control area
            area_code = "10Y1001A1001A796"  # Denmark control area code for per-unit queries
            
            # Determine fuel type from generation unit
            production_types = []
            if generation_unit.fuel_type and "wind" in generation_unit.fuel_type.lower():
                production_types = ["wind"]
            elif generation_unit.fuel_type and "solar" in generation_unit.fuel_type.lower():
                production_types = ["solar"]
            else:
                production_types = ["wind"]  # Default to wind
            
            # Fetch per-unit data
            df, metadata = await client.fetch_generation_per_unit(
                start=task.start_date,
                end=task.end_date,
                area_code=area_code,
                eic_codes=[eic_code],
                production_types=production_types
            )
        else:
            # This is an area code, use regular generation data fetching
            valid_area_codes = ["DE_LU", "FR", "ES", "GB", "IT", "NL", "BE", "AT", "CH", "PL", 
                               "DK_1", "DK_2", "NO_1", "SE_1", "SE_2", "SE_3", "SE_4"]
            
            if eic_code not in valid_area_codes:
                raise ValueError(f"Invalid ENTSOE area code '{eic_code}' for generation unit {generation_unit.name}. Valid codes: {', '.join(valid_area_codes)}")
            
            # Fetch aggregate data for the area
            df, metadata = await client.fetch_generation_data(
                start=task.start_date,
                end=task.end_date,
                area_code=eic_code,
                production_types=["wind", "solar"]
            )
        
        if df.empty:
            return 0
        
        # Store data
        records = []
        
        # Check if this is per-unit data (has eic_code column) or aggregate data
        if "eic_code" in df.columns:
            # Per-unit data
            for idx, row in df.iterrows():
                # For per-unit data, store with the actual area code (DK_1 or DK_2 for Denmark)
                # and include the EIC code in the metadata or as part of the record
                # Ensure timestamp is timezone-aware
                timestamp = pd.to_datetime(row.get("timestamp", idx))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.tz_localize('UTC')
                
                record = ENTSOEGenerationData(
                    timestamp=timestamp,
                    area_code="DK_1",  # Use DK_1 for Danish windfarms (can be made configurable)
                    production_type="wind",  # From generation unit fuel type
                    value=float(row.get("value", 0)),
                    unit="MW",
                )
                records.append(record)
        else:
            # Aggregate area data
            for idx, row in df.iterrows():
                # Use timestamp from index if not in columns
                timestamp = row.get("timestamp", idx)
                if not isinstance(timestamp, datetime):
                    timestamp = pd.to_datetime(timestamp)
                
                # Ensure timestamp is timezone-aware
                if timestamp.tzinfo is None:
                    timestamp = timestamp.tz_localize('UTC')
                    
                record = ENTSOEGenerationData(
                    timestamp=timestamp,
                    area_code=row.get("area_code", generation_unit.code),
                    production_type=row.get("production_type", "wind"),
                    value=float(row.get("value", 0)),
                    unit="MW",
                )
                records.append(record)
        
        # Check for existing data and only insert new records
        inserted_count = 0
        for record in records:
            # Check if this data point already exists
            existing_stmt = select(ENTSOEGenerationData).where(
                and_(
                    ENTSOEGenerationData.timestamp == record.timestamp,
                    ENTSOEGenerationData.area_code == record.area_code,
                    ENTSOEGenerationData.production_type == record.production_type
                )
            )
            existing = await db.execute(existing_stmt)
            existing_record = existing.scalar_one_or_none()
            
            if not existing_record:
                db.add(record)
                inserted_count += 1
            else:
                # Optionally update the existing record if value is different
                if existing_record.value != record.value:
                    existing_record.value = record.value
                    logger.debug(f"Updated existing ENTSOE record: {record.timestamp} - {record.area_code}")
        
        await db.commit()
        logger.info(f"Inserted {inserted_count} new ENTSOE records out of {len(records)} total")
        return inserted_count

    async def _fetch_elexon_data_with_db(self, generation_unit: GenerationUnit, task: BackfillTask, db: AsyncSession) -> int:
        """Fetch Elexon data for a generation unit."""
        client = ElexonClient()
        
        # Fetch data
        df, metadata = await client.fetch_physical_data(
            start=task.start_date,
            end=task.end_date,
            bm_units=[generation_unit.code]
        )
        
        if df.empty:
            return 0
        
        # Store data
        records = []
        for idx, row in df.iterrows():
            record = ElexonGenerationData(
                timestamp=row.get("timestamp", idx),
                bm_unit=generation_unit.code,
                generation_unit_id=generation_unit.id,
                level_from=row.get("levelFrom", 0),
                level_to=row.get("levelTo", 0),
                settlement_date=row.get("settlementDate"),
                settlement_period=row.get("settlementPeriod"),
            )
            records.append(record)
        
        # Check for existing data and only insert new records
        inserted_count = 0
        for record in records:
            # Check if this data point already exists (unique on timestamp, bm_unit, settlement_period)
            existing_stmt = select(ElexonGenerationData).where(
                and_(
                    ElexonGenerationData.timestamp == record.timestamp,
                    ElexonGenerationData.bm_unit == record.bm_unit,
                    ElexonGenerationData.settlement_period == record.settlement_period
                )
            )
            existing = await db.execute(existing_stmt)
            existing_record = existing.scalar_one_or_none()
            
            if not existing_record:
                db.add(record)
                inserted_count += 1
            else:
                # Update existing record if values changed
                if existing_record.level_from != record.level_from or existing_record.level_to != record.level_to:
                    existing_record.level_from = record.level_from
                    existing_record.level_to = record.level_to
                    logger.debug(f"Updated existing Elexon record: {record.timestamp} - {record.bm_unit}")
        
        await db.commit()
        logger.info(f"Inserted {inserted_count} new Elexon records out of {len(records)} total")
        return inserted_count

    async def _fetch_eia_data_with_db(self, generation_unit: GenerationUnit, task: BackfillTask, db: AsyncSession) -> int:
        """Fetch EIA data for a generation unit."""
        # TODO: Implement EIA data fetching
        logger.warning(f"EIA data fetching not yet implemented for unit {generation_unit.name}")
        return 0

    async def _fetch_taipower_data_with_db(self, generation_unit: GenerationUnit, task: BackfillTask, db: AsyncSession) -> int:
        """Fetch Taipower data for a generation unit."""
        # TODO: Implement Taipower data fetching
        logger.warning(f"Taipower data fetching not yet implemented for unit {generation_unit.name}")
        return 0
    
    # Legacy methods for compatibility
    async def _fetch_entsoe_data(self, generation_unit: GenerationUnit, task: BackfillTask) -> int:
        return await self._fetch_entsoe_data_with_db(generation_unit, task, self.db)
    
    async def _fetch_elexon_data(self, generation_unit: GenerationUnit, task: BackfillTask) -> int:
        return await self._fetch_elexon_data_with_db(generation_unit, task, self.db)
    
    async def _fetch_eia_data(self, generation_unit: GenerationUnit, task: BackfillTask) -> int:
        return await self._fetch_eia_data_with_db(generation_unit, task, self.db)
    
    async def _fetch_taipower_data(self, generation_unit: GenerationUnit, task: BackfillTask) -> int:
        return await self._fetch_taipower_data_with_db(generation_unit, task, self.db)

    async def get_backfill_preview(
        self,
        request: BackfillJobCreate,
    ) -> BackfillPreview:
        """Get a preview of what will be backfilled."""
        # Get windfarm with generation units
        stmt = (
            select(Windfarm)
            .options(selectinload(Windfarm.generation_units))
            .where(Windfarm.id == request.windfarm_id)
        )
        result = await self.db.execute(stmt)
        windfarm = result.scalar_one_or_none()
        
        if not windfarm:
            raise ValueError(f"Windfarm {request.windfarm_id} not found")
        
        # Filter generation units by source
        generation_units = windfarm.generation_units
        if request.sources:
            generation_units = [
                gu for gu in generation_units 
                if gu.source.lower() in [s.lower() for s in request.sources]
            ]
        
        # Calculate date ranges (monthly chunks)
        date_ranges = []
        start_date = datetime(request.start_year, 1, 1)
        end_date = datetime(request.end_year, 12, 31)
        
        current_date = start_date
        while current_date <= end_date:
            last_day = monthrange(current_date.year, current_date.month)[1]
            month_end = current_date.replace(day=last_day)
            if month_end > end_date:
                month_end = end_date
            
            date_ranges.append({
                "start": current_date.isoformat(),
                "end": month_end.isoformat(),
                "year": current_date.year,
                "month": current_date.month,
            })
            
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)
        
        # Calculate total tasks
        total_tasks = len(generation_units) * len(date_ranges)
        
        # Estimate time (2 seconds per task)
        estimated_time_minutes = (total_tasks * 2) / 60
        
        return BackfillPreview(
            windfarm_id=windfarm.id,
            windfarm_name=windfarm.name,
            windfarm_code=windfarm.code,
            generation_units=[
                {
                    "id": gu.id,
                    "code": gu.code,
                    "name": gu.name,
                    "source": gu.source,
                }
                for gu in generation_units
            ],
            date_ranges=date_ranges,
            total_tasks=total_tasks,
            estimated_time_minutes=estimated_time_minutes,
        )

    async def get_data_availability(
        self,
        windfarm_id: int,
        year: Optional[int] = None,
    ) -> DataAvailabilityResponse:
        """Check data availability for a windfarm."""
        # Get windfarm with generation units
        stmt = (
            select(Windfarm)
            .options(selectinload(Windfarm.generation_units))
            .where(Windfarm.id == windfarm_id)
        )
        result = await self.db.execute(stmt)
        windfarm = result.scalar_one_or_none()
        
        if not windfarm:
            raise ValueError(f"Windfarm {windfarm_id} not found")
        
        # Get unique sources
        sources = list(set(gu.source for gu in windfarm.generation_units))
        
        # Check availability for each source and month
        availability = []
        
        # Determine year range
        if year:
            years = [year]
        else:
            current_year = datetime.now().year
            years = list(range(current_year - 2, current_year + 1))
        
        for year in years:
            for month in range(1, 13):
                start_date = datetime(year, month, 1)
                last_day = monthrange(year, month)[1]
                end_date = datetime(year, month, last_day, 23, 59, 59)
                
                for source in sources:
                    # Check data based on source
                    has_data = False
                    record_count = 0
                    first_record = None
                    last_record = None
                    
                    if source.lower() == "entsoe":
                        # Check ENTSOE data
                        stmt = (
                            select(
                                func.count(ENTSOEGenerationData.id),
                                func.min(ENTSOEGenerationData.timestamp),
                                func.max(ENTSOEGenerationData.timestamp),
                            )
                            .where(
                                and_(
                                    ENTSOEGenerationData.timestamp >= start_date,
                                    ENTSOEGenerationData.timestamp <= end_date,
                                )
                            )
                        )
                        result = await self.db.execute(stmt)
                        count, first, last = result.one()
                        
                        has_data = count > 0
                        record_count = count or 0
                        first_record = first
                        last_record = last
                    
                    elif source.lower() == "elexon":
                        # Get generation unit codes for this windfarm
                        unit_codes = [
                            gu.code for gu in windfarm.generation_units 
                            if gu.source.lower() == "elexon"
                        ]
                        
                        if unit_codes:
                            stmt = (
                                select(
                                    func.count(ElexonGenerationData.id),
                                    func.min(ElexonGenerationData.timestamp),
                                    func.max(ElexonGenerationData.timestamp),
                                )
                                .where(
                                    and_(
                                        ElexonGenerationData.bm_unit.in_(unit_codes),
                                        ElexonGenerationData.timestamp >= start_date,
                                        ElexonGenerationData.timestamp <= end_date,
                                    )
                                )
                            )
                            result = await self.db.execute(stmt)
                            count, first, last = result.one()
                            
                            has_data = count > 0
                            record_count = count or 0
                            first_record = first
                            last_record = last
                    
                    # Calculate coverage percentage
                    # Assuming hourly data, calculate expected records
                    days_in_month = last_day
                    expected_records = days_in_month * 24  # Hourly data
                    coverage_percentage = (record_count / expected_records * 100) if expected_records > 0 else 0
                    
                    availability.append(DataAvailability(
                        windfarm_id=windfarm_id,
                        source=source,
                        year=year,
                        month=month,
                        has_data=has_data,
                        record_count=record_count,
                        first_record=first_record,
                        last_record=last_record,
                        coverage_percentage=min(coverage_percentage, 100),
                    ))
        
        return DataAvailabilityResponse(
            windfarm_id=windfarm.id,
            windfarm_name=windfarm.name,
            windfarm_code=windfarm.code,
            sources=sources,
            availability=availability,
        )

    async def retry_failed_tasks(
        self,
        job_id: int,
        task_ids: Optional[List[int]] = None,
    ) -> BackfillJob:
        """Retry failed tasks in a backfill job."""
        # Get job with tasks
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job_id)
        )
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Filter tasks to retry
        tasks_to_retry = []
        for task in job.tasks:
            if task.status == BackfillTaskStatus.FAILED:
                if task_ids is None or task.id in task_ids:
                    task.status = BackfillTaskStatus.PENDING
                    task.attempt_count = 0
                    task.error_message = None
                    tasks_to_retry.append(task)
        
        if not tasks_to_retry:
            raise ValueError("No failed tasks to retry")
        
        # Update job status to pending so it can be processed again
        job.status = BackfillJobStatus.PENDING
        job.failed_tasks = max(0, job.failed_tasks - len(tasks_to_retry))
        
        await self.db.commit()
        
        # Don't start processing automatically - let it be triggered separately
        logger.info(f"Reset {len(tasks_to_retry)} failed tasks in job {job_id}")
        
        return job

    async def get_backfill_jobs(
        self,
        windfarm_id: Optional[int] = None,
        status: Optional[BackfillJobStatus] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[BackfillJob]:
        """Get list of backfill jobs."""
        stmt = select(BackfillJob).options(selectinload(BackfillJob.tasks))
        
        if windfarm_id:
            stmt = stmt.where(BackfillJob.windfarm_id == windfarm_id)
        
        if status:
            stmt = stmt.where(BackfillJob.status == status)
        
        stmt = stmt.order_by(BackfillJob.created_at.desc()).limit(limit).offset(offset)
        
        result = await self.db.execute(stmt)
        return result.scalars().all()

    async def get_backfill_job(self, job_id: int) -> Optional[BackfillJob]:
        """Get a specific backfill job."""
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job_id)
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def cancel_backfill_job(self, job_id: int) -> BackfillJob:
        """Cancel a pending or in-progress backfill job."""
        # Get job with tasks
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job_id)
        )
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Check if job can be cancelled
        if job.status in [BackfillJobStatus.COMPLETED, BackfillJobStatus.FAILED]:
            raise ValueError(f"Cannot cancel job with status {job.status}")
        
        # Update job status
        job.status = BackfillJobStatus.FAILED
        job.error_message = "Job cancelled by user"
        job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        
        # Cancel all pending tasks
        for task in job.tasks:
            if task.status in [BackfillTaskStatus.PENDING, BackfillTaskStatus.IN_PROGRESS]:
                task.status = BackfillTaskStatus.SKIPPED
                task.error_message = "Job cancelled by user"
                if not task.completed_at:
                    task.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        
        await self.db.commit()
        await self.db.refresh(job)
        
        logger.info(f"Cancelled backfill job {job_id}")
        return job

    async def reset_stuck_tasks(self, job_id: int) -> BackfillJob:
        """Reset stuck tasks (in_progress or pending) to failed status so they can be retried."""
        # Get job with tasks
        stmt = (
            select(BackfillJob)
            .options(selectinload(BackfillJob.tasks))
            .where(BackfillJob.id == job_id)
        )
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Find stuck tasks (in_progress or pending with job not in_progress)
        stuck_tasks = []
        for task in job.tasks:
            # A task is stuck if:
            # 1. It's in_progress but the job is not in_progress
            # 2. It's been in_progress for too long (more than 5 minutes)
            if task.status == BackfillTaskStatus.IN_PROGRESS:
                if job.status != BackfillJobStatus.IN_PROGRESS:
                    stuck_tasks.append(task)
                elif task.started_at:
                    # Check if task has been running for more than 5 minutes
                    # Convert to timezone-aware for comparison if needed
                    task_started = task.started_at
                    if task_started.tzinfo is None:
                        task_started = task_started.replace(tzinfo=timezone.utc)
                    time_running = datetime.now(timezone.utc) - task_started
                    if time_running.total_seconds() > 300:  # 5 minutes
                        stuck_tasks.append(task)
        
        # Reset stuck tasks to failed
        reset_count = 0
        for task in stuck_tasks:
            task.status = BackfillTaskStatus.FAILED
            task.error_message = "Task was stuck and has been reset. You can retry it."
            if not task.completed_at:
                task.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            reset_count += 1
            job.failed_tasks += 1
            
        # Also handle pending tasks if the job is not in progress or pending
        if job.status not in [BackfillJobStatus.IN_PROGRESS, BackfillJobStatus.PENDING]:
            for task in job.tasks:
                if task.status == BackfillTaskStatus.PENDING:
                    task.status = BackfillTaskStatus.FAILED
                    task.error_message = "Job was terminated while task was pending. You can retry it."
                    task.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    reset_count += 1
                    job.failed_tasks += 1
        
        # Update job status if needed
        if reset_count > 0:
            if job.status == BackfillJobStatus.IN_PROGRESS:
                # If job was stuck in progress, mark it as partially completed
                job.status = BackfillJobStatus.PARTIALLY_COMPLETED
                job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            
            await self.db.commit()
            await self.db.refresh(job)
            logger.info(f"Reset {reset_count} stuck tasks in job {job_id}")
        
        return job

    async def delete_backfill_job(self, job_id: int) -> bool:
        """Delete a backfill job and all its tasks."""
        # Get job
        stmt = select(BackfillJob).where(BackfillJob.id == job_id)
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            raise ValueError(f"Backfill job {job_id} not found")
        
        # Check if job can be deleted (only allow deletion of completed, failed, or cancelled jobs)
        if job.status == BackfillJobStatus.IN_PROGRESS:
            raise ValueError("Cannot delete a job that is currently in progress. Please cancel it first.")
        
        # Delete the job (tasks will be cascade deleted)
        await self.db.delete(job)
        await self.db.commit()
        
        logger.info(f"Deleted backfill job {job_id}")
        return True