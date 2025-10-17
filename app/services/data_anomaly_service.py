"""Service for detecting and managing data anomalies."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from decimal import Decimal

from sqlalchemy import select, and_, or_, func, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.models.data_anomaly import DataAnomaly, AnomalyType, AnomalyStatus, AnomalySeverity
from app.models.generation_data import GenerationData
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm
from app.schemas.data_anomaly import (
    DataAnomalyCreate,
    DataAnomalyUpdate,
    DataAnomalyResponse,
    AnomalyDetectionRequest,
    AnomalyListFilters,
)

logger = logging.getLogger(__name__)


class DataAnomalyService:
    """Service for data anomaly detection and management."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def detect_anomalies(
        self,
        request: AnomalyDetectionRequest
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Detect anomalies based on request parameters.

        This method does NOT save anomalies to the database - it only detects and returns them.
        Use save_anomalies() to persist detected anomalies if needed.

        Args:
            request: Detection request with filters and parameters

        Returns:
            Tuple of (list of anomaly dicts, detection summary dict)
        """
        all_anomalies = []
        summary = {
            "windfarms_checked": 0,
            "periods_checked": 0,
            "anomalies_by_type": {},
            "anomalies_by_severity": {},
        }

        # Get windfarm IDs to check
        windfarm_ids = request.windfarm_ids
        if not windfarm_ids:
            # Get all windfarm IDs
            result = await self.db.execute(select(Windfarm.id))
            windfarm_ids = [row[0] for row in result.all()]

        summary["windfarms_checked"] = len(windfarm_ids)

        # Detect capacity factor anomalies if requested
        anomaly_types = request.anomaly_types or [AnomalyType.CAPACITY_FACTOR_OVER_LIMIT]

        if AnomalyType.CAPACITY_FACTOR_OVER_LIMIT in anomaly_types:
            cf_anomalies = await self._detect_capacity_factor_anomalies(
                windfarm_ids=windfarm_ids,
                start_date=request.start_date,
                end_date=request.end_date,
                threshold=request.capacity_factor_threshold
            )
            all_anomalies.extend(cf_anomalies)
            summary["anomalies_by_type"][AnomalyType.CAPACITY_FACTOR_OVER_LIMIT] = len(cf_anomalies)

        # Count by severity
        for anomaly in all_anomalies:
            severity = anomaly.severity
            summary["anomalies_by_severity"][severity] = summary["anomalies_by_severity"].get(severity, 0) + 1

        # Convert to dicts for response (don't save to database)
        anomaly_dicts = []
        for anomaly in all_anomalies:
            # Get windfarm and unit names
            windfarm_name = None
            if anomaly.windfarm_id:
                wf_result = await self.db.execute(
                    select(Windfarm.name).where(Windfarm.id == anomaly.windfarm_id)
                )
                wf_row = wf_result.first()
                if wf_row:
                    windfarm_name = wf_row[0]

            generation_unit_name = anomaly.anomaly_metadata.get('generation_unit_name') if anomaly.anomaly_metadata else None

            anomaly_dicts.append({
                "anomaly_type": anomaly.anomaly_type,
                "severity": anomaly.severity,
                "status": anomaly.status,
                "windfarm_id": anomaly.windfarm_id,
                "generation_unit_id": anomaly.generation_unit_id,
                "period_start": anomaly.period_start.isoformat(),
                "period_end": anomaly.period_end.isoformat(),
                "description": anomaly.description,
                "anomaly_metadata": anomaly.anomaly_metadata,
                "detected_at": anomaly.detected_at.isoformat(),
                "windfarm_name": windfarm_name,
                "generation_unit_name": generation_unit_name,
                # Fields that don't exist yet since not saved
                "id": None,
                "resolved_at": None,
                "resolved_by": None,
                "resolution_notes": None,
                "is_active": True,
                "created_at": None,
                "updated_at": None,
            })

        return anomaly_dicts, summary

    async def _detect_capacity_factor_anomalies(
        self,
        windfarm_ids: List[int],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        threshold: float = 1.2
    ) -> List[DataAnomaly]:
        """
        Detect capacity factor > threshold anomalies.

        Groups consecutive hours with the same issue into single anomaly entries.

        Args:
            windfarm_ids: List of windfarm IDs to check
            start_date: Start date for detection range
            end_date: End date for detection range
            threshold: Capacity factor threshold (default 1.2 = 120%)

        Returns:
            List of DataAnomaly objects (not yet committed)
        """
        anomalies = []

        for windfarm_id in windfarm_ids:
            # Build query for problematic data points
            query = select(GenerationData).where(
                and_(
                    GenerationData.windfarm_id == windfarm_id,
                    GenerationData.capacity_factor > Decimal(str(threshold)),
                    GenerationData.capacity_factor.isnot(None)
                )
            )

            if start_date:
                query = query.where(GenerationData.hour >= start_date)
            if end_date:
                query = query.where(GenerationData.hour <= end_date)

            query = query.order_by(GenerationData.hour, GenerationData.generation_unit_id)

            result = await self.db.execute(query)
            problematic_records = result.scalars().all()

            if not problematic_records:
                continue

            # Group consecutive periods by generation unit
            unit_periods = self._group_consecutive_periods(problematic_records)

            # Create anomaly for each grouped period
            for (gen_unit_id, periods) in unit_periods:
                for period_group in periods:
                    # Calculate max capacity factor in this period
                    max_cf = max(float(r.capacity_factor) for r in period_group)
                    avg_cf = sum(float(r.capacity_factor) for r in period_group) / len(period_group)

                    # Determine severity based on how far over threshold
                    if max_cf >= 2.0:
                        severity = AnomalySeverity.CRITICAL
                    elif max_cf >= 1.5:
                        severity = AnomalySeverity.HIGH
                    elif max_cf >= 1.3:
                        severity = AnomalySeverity.MEDIUM
                    else:
                        severity = AnomalySeverity.LOW

                    # Get generation unit name
                    gen_unit_name = None
                    if gen_unit_id:
                        gen_unit_result = await self.db.execute(
                            select(GenerationUnit.name).where(GenerationUnit.id == gen_unit_id)
                        )
                        gen_unit_row = gen_unit_result.first()
                        if gen_unit_row:
                            gen_unit_name = gen_unit_row[0]

                    anomaly = DataAnomaly(
                        anomaly_type=AnomalyType.CAPACITY_FACTOR_OVER_LIMIT,
                        severity=severity,
                        status=AnomalyStatus.PENDING,
                        windfarm_id=windfarm_id,
                        generation_unit_id=gen_unit_id,
                        period_start=period_group[0].hour,
                        period_end=period_group[-1].hour + timedelta(hours=1),  # End of last hour
                        description=f"Capacity factor exceeded {threshold*100:.0f}% threshold",
                        anomaly_metadata={
                            "threshold": threshold,
                            "max_capacity_factor": round(max_cf, 4),
                            "avg_capacity_factor": round(avg_cf, 4),
                            "hours_affected": len(period_group),
                            "generation_unit_name": gen_unit_name,
                        },
                        detected_at=datetime.utcnow(),
                    )
                    anomalies.append(anomaly)

        return anomalies

    def _group_consecutive_periods(
        self,
        records: List[GenerationData]
    ) -> List[Tuple[Optional[int], List[List[GenerationData]]]]:
        """
        Group consecutive hourly records by generation unit.

        Args:
            records: List of generation data records sorted by hour and unit

        Returns:
            List of tuples (generation_unit_id, [list of period groups])
            where each period group is a list of consecutive hourly records
        """
        if not records:
            return []

        # Group by generation unit first
        by_unit: Dict[Optional[int], List[GenerationData]] = {}
        for record in records:
            unit_id = record.generation_unit_id
            if unit_id not in by_unit:
                by_unit[unit_id] = []
            by_unit[unit_id].append(record)

        # For each unit, group consecutive hours
        result = []
        for unit_id, unit_records in by_unit.items():
            # Sort by hour
            unit_records.sort(key=lambda r: r.hour)

            period_groups = []
            current_group = [unit_records[0]]

            for i in range(1, len(unit_records)):
                prev_hour = unit_records[i-1].hour
                curr_hour = unit_records[i].hour

                # Check if consecutive (allowing for 1 hour gap)
                if curr_hour - prev_hour <= timedelta(hours=1, minutes=30):
                    current_group.append(unit_records[i])
                else:
                    # Start new group
                    period_groups.append(current_group)
                    current_group = [unit_records[i]]

            # Add last group
            if current_group:
                period_groups.append(current_group)

            result.append((unit_id, period_groups))

        return result

    async def get_anomalies(
        self,
        filters: AnomalyListFilters
    ) -> Tuple[List[DataAnomalyResponse], int]:
        """
        Get list of anomalies with pagination and filtering.

        Args:
            filters: Filter parameters

        Returns:
            Tuple of (list of anomalies, total count)
        """
        # Build base query
        query = select(DataAnomaly)

        # Apply filters
        conditions = []

        if filters.windfarm_id is not None:
            conditions.append(DataAnomaly.windfarm_id == filters.windfarm_id)

        if filters.generation_unit_id is not None:
            conditions.append(DataAnomaly.generation_unit_id == filters.generation_unit_id)

        if filters.anomaly_type:
            conditions.append(DataAnomaly.anomaly_type == filters.anomaly_type)

        if filters.status:
            conditions.append(DataAnomaly.status == filters.status)

        if filters.severity:
            conditions.append(DataAnomaly.severity == filters.severity)

        if filters.start_date:
            conditions.append(DataAnomaly.period_start >= filters.start_date)

        if filters.end_date:
            conditions.append(DataAnomaly.period_end <= filters.end_date)

        if filters.is_active is not None:
            conditions.append(DataAnomaly.is_active == filters.is_active)

        if conditions:
            query = query.where(and_(*conditions))

        # Add eager loading for related entities
        query = query.options(
            joinedload(DataAnomaly.windfarm),
            joinedload(DataAnomaly.generation_unit)
        )

        # Get total count
        count_query = select(func.count()).select_from(DataAnomaly)
        if conditions:
            count_query = count_query.where(and_(*conditions))
        total_result = await self.db.execute(count_query)
        total = total_result.scalar() or 0

        # Add pagination
        query = query.order_by(DataAnomaly.detected_at.desc())
        query = query.offset((filters.page - 1) * filters.page_size)
        query = query.limit(filters.page_size)

        # Execute query
        result = await self.db.execute(query)
        anomalies = result.scalars().all()

        # Convert to response objects
        response_list = []
        for anomaly in anomalies:
            response = DataAnomalyResponse.model_validate(anomaly)
            # Add related entity names
            if anomaly.windfarm:
                response.windfarm_name = anomaly.windfarm.name
            if anomaly.generation_unit:
                response.generation_unit_name = anomaly.generation_unit.name
            response_list.append(response)

        return response_list, total

    async def get_anomaly_by_id(self, anomaly_id: int) -> Optional[DataAnomalyResponse]:
        """
        Get a single anomaly by ID.

        Args:
            anomaly_id: Anomaly ID

        Returns:
            DataAnomalyResponse or None
        """
        query = select(DataAnomaly).where(DataAnomaly.id == anomaly_id)
        query = query.options(
            joinedload(DataAnomaly.windfarm),
            joinedload(DataAnomaly.generation_unit)
        )

        result = await self.db.execute(query)
        anomaly = result.scalar_one_or_none()

        if not anomaly:
            return None

        response = DataAnomalyResponse.model_validate(anomaly)
        if anomaly.windfarm:
            response.windfarm_name = anomaly.windfarm.name
        if anomaly.generation_unit:
            response.generation_unit_name = anomaly.generation_unit.name

        return response

    async def update_anomaly_status(
        self,
        anomaly_id: int,
        status: str,
        resolution_notes: Optional[str] = None,
        user_id: Optional[int] = None
    ) -> Optional[DataAnomalyResponse]:
        """
        Update the status of an anomaly.

        Args:
            anomaly_id: Anomaly ID
            status: New status
            resolution_notes: Optional resolution notes
            user_id: ID of user updating the status

        Returns:
            Updated anomaly or None if not found
        """
        result = await self.db.execute(
            select(DataAnomaly).where(DataAnomaly.id == anomaly_id)
        )
        anomaly = result.scalar_one_or_none()

        if not anomaly:
            return None

        anomaly.status = status
        if resolution_notes:
            anomaly.resolution_notes = resolution_notes

        # Set resolved timestamp if marking as resolved
        if status in [AnomalyStatus.RESOLVED, AnomalyStatus.FALSE_POSITIVE, AnomalyStatus.IGNORED]:
            anomaly.resolved_at = datetime.utcnow()
            if user_id:
                anomaly.resolved_by = user_id

        await self.db.commit()
        await self.db.refresh(anomaly)

        return await self.get_anomaly_by_id(anomaly_id)

    async def update_anomaly(
        self,
        anomaly_id: int,
        update_data: DataAnomalyUpdate
    ) -> Optional[DataAnomalyResponse]:
        """
        Update an anomaly with partial data.

        Args:
            anomaly_id: Anomaly ID
            update_data: Update data

        Returns:
            Updated anomaly or None if not found
        """
        result = await self.db.execute(
            select(DataAnomaly).where(DataAnomaly.id == anomaly_id)
        )
        anomaly = result.scalar_one_or_none()

        if not anomaly:
            return None

        # Update fields
        update_dict = update_data.model_dump(exclude_unset=True)
        for field, value in update_dict.items():
            setattr(anomaly, field, value)

        await self.db.commit()
        await self.db.refresh(anomaly)

        return await self.get_anomaly_by_id(anomaly_id)

    async def delete_anomaly(self, anomaly_id: int) -> bool:
        """
        Delete an anomaly (soft delete by setting is_active=False).

        Args:
            anomaly_id: Anomaly ID

        Returns:
            True if deleted, False if not found
        """
        result = await self.db.execute(
            select(DataAnomaly).where(DataAnomaly.id == anomaly_id)
        )
        anomaly = result.scalar_one_or_none()

        if not anomaly:
            return False

        anomaly.is_active = False
        await self.db.commit()

        return True

    async def hard_delete_anomaly(self, anomaly_id: int) -> bool:
        """
        Permanently delete an anomaly.

        Args:
            anomaly_id: Anomaly ID

        Returns:
            True if deleted, False if not found
        """
        result = await self.db.execute(
            delete(DataAnomaly).where(DataAnomaly.id == anomaly_id).returning(DataAnomaly.id)
        )
        deleted_id = result.scalar_one_or_none()

        if deleted_id:
            await self.db.commit()
            return True

        return False

    async def reaggregate_period(
        self,
        start_date: datetime,
        end_date: datetime,
        sources: Optional[List[str]] = None,
        windfarm_id: Optional[int] = None,
        generation_unit_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Re-aggregate generation data for a specific period.

        This method uses the existing daily aggregation processor to re-process data.
        It's idempotent - will delete existing data for the period before re-processing.

        Args:
            start_date: Start date for re-aggregation
            end_date: End date for re-aggregation
            sources: List of sources to re-aggregate (e.g., ['ENTSOE', 'ELEXON'])
            windfarm_id: Optional windfarm ID to limit re-aggregation
            generation_unit_id: Optional generation unit ID to limit re-aggregation

        Returns:
            Dict with result information
        """
        from pathlib import Path
        import sys

        # Add the scripts path so we can import the processor
        scripts_path = Path(__file__).parent.parent.parent / "scripts" / "seeds" / "aggregate_generation_data"
        sys.path.insert(0, str(scripts_path))

        try:
            # Import the daily processor
            from process_generation_data_daily import DailyGenerationProcessor

            # Use the existing database session
            processor = DailyGenerationProcessor(self.db, dry_run=False)

            # Load generation units once (for batch processing efficiency)
            await processor.load_generation_units()

            # Process each day in the range
            current_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date_normalized = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

            total_records_processed = 0
            total_records_created = 0
            errors = []
            sources_processed_set = set()

            while current_date <= end_date_normalized:
                try:
                    result = await processor.process_day(
                        date=current_date,
                        sources=sources,
                        skip_load_units=True,  # Already loaded
                        skip_commit=False  # Commit after each day to avoid conflicts
                    )

                    # Track stats
                    for source, source_result in result.get('sources', {}).items():
                        if 'error' not in source_result:
                            total_records_created += source_result.get('saved', 0)
                            total_records_processed += source_result.get('raw_records', 0)
                            sources_processed_set.add(source)
                        else:
                            errors.append(f"{source} on {current_date.date()}: {source_result['error']}")

                except Exception as e:
                    logger.error(f"Error processing {current_date.date()}: {e}")
                    errors.append(f"{current_date.date()}: {str(e)}")
                    # Rollback this day and continue to next
                    await self.db.rollback()

                # Move to next day
                current_date += timedelta(days=1)

            return {
                "success": len(errors) == 0,
                "message": "Re-aggregation completed successfully" if not errors else "Re-aggregation completed with errors",
                "records_processed": total_records_processed,
                "records_created": total_records_created,
                "period_start": start_date,
                "period_end": end_date,
                "sources_processed": list(sources_processed_set),
                "errors": errors if errors else None
            }

        except Exception as e:
            logger.error(f"Fatal error during re-aggregation: {e}")
            await self.db.rollback()
            return {
                "success": False,
                "message": f"Re-aggregation failed: {str(e)}",
                "records_processed": 0,
                "records_created": 0,
                "period_start": start_date,
                "period_end": end_date,
                "sources_processed": [],
                "errors": [str(e)]
            }
        finally:
            # Remove scripts path from sys.path
            if str(scripts_path) in sys.path:
                sys.path.remove(str(scripts_path))
