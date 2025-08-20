"""Service for storing and retrieving Taipower generation data."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
from uuid import UUID

from sqlalchemy import select, and_, func, desc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.taipower_generation_data import TaipowerGenerationData
from app.models.generation_unit import GenerationUnit
from app.models.user import User
from app.services.taipower_client import TaipowerClient


class TaipowerStorageService:
    """Service for managing Taipower generation data storage."""
    
    def __init__(self, db: AsyncSession):
        """Initialize the service with database session."""
        self.db = db
        self.client = TaipowerClient()
    
    async def fetch_and_store_live_data(
        self,
        current_user: User,
        store_data: bool = True
    ) -> Dict:
        """
        Fetch live generation data from Taipower API and optionally store it.
        
        Args:
            current_user: Current authenticated user
            store_data: Whether to store the fetched data
            
        Returns:
            Dictionary containing fetched data and metadata
        """
        try:
            # Fetch from Taipower API
            data, metadata = await self.client.fetch_live_data()
            
            if not data:
                return {
                    "success": False,
                    "error": "Failed to fetch data from Taipower",
                    "metadata": metadata,
                    "data": [],
                    "stored": False,
                    "records_stored": 0
                }
            
            # Get all Taipower generation units
            result = await self.db.execute(
                select(GenerationUnit).where(
                    GenerationUnit.source == "TAIPOWER",
                    GenerationUnit.is_active == True
                )
            )
            generation_units = result.scalars().all()
            
            # Create mapping
            gen_units_map = {unit.name: unit for unit in generation_units}
            
            # Transform to data points
            data_points = self.client.transform_to_data_points(data, gen_units_map)
            
            # Prepare records for storage with deduplication
            records_dict = {}  # Use dict to deduplicate by unique key
            
            for dp in data_points:
                # Create unique key for deduplication
                unique_key = (dp.timestamp, dp.unit_name)
                
                # Skip if we already have this record
                if unique_key in records_dict:
                    continue
                    
                from uuid import uuid4
                    
                record = {
                    "id": uuid4(),
                    "timestamp": dp.timestamp,
                    "generation_type": dp.generation_type,
                    "unit_name": dp.unit_name,
                    "installed_capacity_mw": Decimal(str(dp.installed_capacity_mw)) if dp.installed_capacity_mw else None,
                    "net_generation_mw": Decimal(str(dp.net_generation_mw)) if dp.net_generation_mw else None,
                    "capacity_utilization_percent": Decimal(str(dp.capacity_utilization_percent)) if dp.capacity_utilization_percent else None,
                    "notes": dp.notes,
                    "generation_unit_id": dp.generation_unit_id,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "created_by_id": current_user.id,
                    "updated_by_id": current_user.id
                }
                records_dict[unique_key] = record
            
            # Convert dict back to list
            records_to_store = list(records_dict.values())
            
            # Store data if requested
            stored_count = 0
            if store_data and records_to_store:
                stored_count = await self.store_snapshot_data_batch(
                    records_to_store, current_user
                )
            
            # Calculate summary statistics
            stats = self.client.calculate_summary_statistics(data)
            
            return {
                "success": True,
                "data": records_to_store,
                "metadata": {
                    **metadata,
                    **stats,
                    "stored": store_data,
                    "records_stored": stored_count,
                    "records_fetched": len(data_points)
                }
            }
            
        except Exception as e:
            # Error fetching and storing Taipower data
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "metadata": {},
                "stored": False,
                "records_stored": 0
            }
    
    async def store_snapshot_data_batch(
        self, data: List[Dict], user: User
    ) -> int:
        """
        Store snapshot data using batch operations with upsert.
        
        Args:
            data: List of data records to store
            user: User performing the operation
            
        Returns:
            Number of records stored/updated
        """
        if not data:
            return 0
        
        # Process in batches
        batch_size = 100
        total_stored = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            try:
                # Use PostgreSQL's ON CONFLICT for efficient upsert
                stmt = insert(TaipowerGenerationData).values(batch)
                
                # On conflict, update the values
                stmt = stmt.on_conflict_do_update(
                    index_elements=['timestamp', 'unit_name'],
                    set_={
                        'net_generation_mw': stmt.excluded.net_generation_mw,
                        'capacity_utilization_percent': stmt.excluded.capacity_utilization_percent,
                        'notes': stmt.excluded.notes,
                        'updated_at': datetime.utcnow(),
                        'updated_by_id': user.id
                    }
                )
                
                await self.db.execute(stmt)
                await self.db.commit()
                total_stored += len(batch)
                
            except Exception as e:
                await self.db.rollback()
                # Log error but continue with other batches
                import structlog
                logger = structlog.get_logger()
                logger.error(
                    "Error storing Taipower batch",
                    error=str(e),
                    error_type=type(e).__name__,
                    batch_size=len(batch)
                )
                # Don't raise - continue with next batch
                continue
        
        return total_stored
    
    async def get_stored_snapshots(
        self,
        unit_names: Optional[List[str]] = None,
        generation_types: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000
    ) -> Dict:
        """
        Retrieve stored snapshot data.
        
        Args:
            unit_names: Optional list of unit names to filter
            generation_types: Optional list of generation types to filter
            start_date: Optional start date
            end_date: Optional end date
            limit: Maximum number of records to return
            
        Returns:
            Dictionary containing data and statistics
        """
        # Build query
        query = select(TaipowerGenerationData)
        
        conditions = []
        if unit_names:
            conditions.append(TaipowerGenerationData.unit_name.in_(unit_names))
        if generation_types:
            conditions.append(TaipowerGenerationData.generation_type.in_(generation_types))
        if start_date:
            conditions.append(TaipowerGenerationData.timestamp >= start_date)
        if end_date:
            conditions.append(TaipowerGenerationData.timestamp <= end_date)
        
        if conditions:
            query = query.where(and_(*conditions))
        
        query = query.order_by(desc(TaipowerGenerationData.timestamp)).limit(limit)
        
        result = await self.db.execute(query)
        data = result.scalars().all()
        
        # Calculate statistics
        statistics = await self.calculate_snapshot_statistics(
            unit_names, generation_types, start_date, end_date
        )
        
        # Detect gaps if date range provided
        gaps = []
        if start_date and end_date:
            gaps = await self.detect_snapshot_gaps(
                unit_names, generation_types, start_date, end_date
            )
        
        return {
            "data": data,
            "gaps": gaps,
            "statistics": statistics
        }
    
    async def detect_snapshot_gaps(
        self,
        unit_names: Optional[List[str]] = None,
        generation_types: Optional[List[str]] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        expected_interval_minutes: int = 15
    ) -> List[Dict]:
        """
        Detect gaps in snapshot data based on expected intervals.
        
        Args:
            unit_names: Optional list of unit names
            generation_types: Optional list of generation types
            start_date: Start date for gap detection
            end_date: End date for gap detection
            expected_interval_minutes: Expected interval between snapshots
            
        Returns:
            List of gap information
        """
        if not start_date or not end_date:
            return []
        
        # Query existing timestamps
        query = select(TaipowerGenerationData.timestamp).distinct()
        
        conditions = []
        if unit_names:
            conditions.append(TaipowerGenerationData.unit_name.in_(unit_names))
        if generation_types:
            conditions.append(TaipowerGenerationData.generation_type.in_(generation_types))
        conditions.append(TaipowerGenerationData.timestamp >= start_date)
        conditions.append(TaipowerGenerationData.timestamp <= end_date)
        
        query = query.where(and_(*conditions)).order_by(TaipowerGenerationData.timestamp)
        
        result = await self.db.execute(query)
        existing_timestamps = [row[0] for row in result]
        
        if not existing_timestamps:
            return [{
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "duration_hours": (end_date - start_date).total_seconds() / 3600,
                "type": "no_data"
            }]
        
        # Detect gaps
        gaps = []
        for i in range(1, len(existing_timestamps)):
            prev_time = existing_timestamps[i-1]
            curr_time = existing_timestamps[i]
            time_diff = (curr_time - prev_time).total_seconds() / 60
            
            # If gap is larger than expected interval + tolerance
            if time_diff > expected_interval_minutes * 1.5:
                gaps.append({
                    "start": prev_time.isoformat(),
                    "end": curr_time.isoformat(),
                    "duration_minutes": round(time_diff, 2),
                    "expected_snapshots": int(time_diff / expected_interval_minutes),
                    "type": "missing_snapshots"
                })
        
        # Check for gap at beginning
        if existing_timestamps[0] > start_date + timedelta(minutes=expected_interval_minutes):
            gaps.insert(0, {
                "start": start_date.isoformat(),
                "end": existing_timestamps[0].isoformat(),
                "duration_minutes": round((existing_timestamps[0] - start_date).total_seconds() / 60, 2),
                "type": "missing_at_start"
            })
        
        # Check for gap at end
        if existing_timestamps[-1] < end_date - timedelta(minutes=expected_interval_minutes):
            gaps.append({
                "start": existing_timestamps[-1].isoformat(),
                "end": end_date.isoformat(),
                "duration_minutes": round((end_date - existing_timestamps[-1]).total_seconds() / 60, 2),
                "type": "missing_at_end"
            })
        
        return gaps
    
    async def calculate_snapshot_statistics(
        self,
        unit_names: Optional[List[str]] = None,
        generation_types: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict:
        """
        Calculate statistics for stored snapshots.
        
        Args:
            unit_names: Optional list of unit names
            generation_types: Optional list of generation types
            start_date: Optional start date
            end_date: Optional end date
            
        Returns:
            Dictionary containing statistics
        """
        # Build base query
        conditions = []
        if unit_names:
            conditions.append(TaipowerGenerationData.unit_name.in_(unit_names))
        if generation_types:
            conditions.append(TaipowerGenerationData.generation_type.in_(generation_types))
        if start_date:
            conditions.append(TaipowerGenerationData.timestamp >= start_date)
        if end_date:
            conditions.append(TaipowerGenerationData.timestamp <= end_date)
        
        # Count total records
        count_query = select(func.count(TaipowerGenerationData.id))
        if conditions:
            count_query = count_query.where(and_(*conditions))
        
        result = await self.db.execute(count_query)
        total_records = result.scalar()
        
        # Get unique timestamps
        timestamp_query = select(
            func.count(func.distinct(TaipowerGenerationData.timestamp))
        )
        if conditions:
            timestamp_query = timestamp_query.where(and_(*conditions))
        
        result = await self.db.execute(timestamp_query)
        unique_timestamps = result.scalar()
        
        # Get date range
        range_query = select(
            func.min(TaipowerGenerationData.timestamp),
            func.max(TaipowerGenerationData.timestamp)
        )
        if conditions:
            range_query = range_query.where(and_(*conditions))
        
        result = await self.db.execute(range_query)
        min_date, max_date = result.one()
        
        # Calculate average generation
        avg_query = select(
            func.avg(TaipowerGenerationData.net_generation_mw),
            func.sum(TaipowerGenerationData.net_generation_mw),
            func.max(TaipowerGenerationData.net_generation_mw),
            func.min(TaipowerGenerationData.net_generation_mw)
        )
        if conditions:
            avg_query = avg_query.where(and_(*conditions))
        
        result = await self.db.execute(avg_query)
        avg_gen, total_gen, max_gen, min_gen = result.one()
        
        return {
            "total_records": total_records or 0,
            "unique_snapshots": unique_timestamps or 0,
            "date_range": {
                "start": min_date.isoformat() if min_date else None,
                "end": max_date.isoformat() if max_date else None
            },
            "generation_statistics": {
                "average_mw": float(avg_gen) if avg_gen else 0,
                "total_mwh": float(total_gen) if total_gen else 0,
                "max_mw": float(max_gen) if max_gen else 0,
                "min_mw": float(min_gen) if min_gen else 0
            },
            "filters_applied": {
                "unit_names": unit_names,
                "generation_types": generation_types,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            }
        }
    
    async def get_latest_snapshot(
        self,
        unit_names: Optional[List[str]] = None,
        generation_types: Optional[List[str]] = None
    ) -> Dict:
        """
        Get the most recent snapshot data.
        
        Args:
            unit_names: Optional list of unit names to filter
            generation_types: Optional list of generation types to filter
            
        Returns:
            Dictionary containing latest snapshot data
        """
        # Get latest timestamp
        latest_query = select(func.max(TaipowerGenerationData.timestamp))
        
        conditions = []
        if unit_names:
            conditions.append(TaipowerGenerationData.unit_name.in_(unit_names))
        if generation_types:
            conditions.append(TaipowerGenerationData.generation_type.in_(generation_types))
        
        if conditions:
            latest_query = latest_query.where(and_(*conditions))
        
        result = await self.db.execute(latest_query)
        latest_timestamp = result.scalar()
        
        if not latest_timestamp:
            return {
                "data": [],
                "timestamp": None,
                "total_generation_mw": 0,
                "unit_count": 0
            }
        
        # Get all data for latest timestamp
        data_query = select(TaipowerGenerationData).where(
            TaipowerGenerationData.timestamp == latest_timestamp
        )
        
        if conditions:
            data_query = data_query.where(and_(*conditions))
        
        result = await self.db.execute(data_query)
        data = result.scalars().all()
        
        # Calculate total generation
        total_generation = sum(
            float(d.net_generation_mw) for d in data if d.net_generation_mw
        )
        
        # Group by generation type
        generation_by_type = {}
        for record in data:
            gen_type = record.generation_type or "Unknown"
            if gen_type not in generation_by_type:
                generation_by_type[gen_type] = 0
            if record.net_generation_mw:
                generation_by_type[gen_type] += float(record.net_generation_mw)
        
        return {
            "data": data,
            "timestamp": latest_timestamp.isoformat(),
            "total_generation_mw": round(total_generation, 2),
            "generation_by_type": generation_by_type,
            "unit_count": len(data)
        }
    
    async def get_data_availability(
        self,
        year: int,
        month: Optional[int] = None,
        unit_names: Optional[List[str]] = None,
        generation_types: Optional[List[str]] = None
    ) -> Dict:
        """
        Get data availability for a specific period.
        
        Args:
            year: Year to check
            month: Optional month to check (if not provided, check whole year)
            unit_names: Optional list of unit names
            generation_types: Optional list of generation types
            
        Returns:
            Dictionary containing availability information
        """
        # Build date range
        if month:
            from calendar import monthrange
            start_date = datetime(year, month, 1)
            last_day = monthrange(year, month)[1]
            end_date = datetime(year, month, last_day, 23, 59, 59)
        else:
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31, 23, 59, 59)
        
        # Build base conditions
        conditions = [
            TaipowerGenerationData.timestamp >= start_date,
            TaipowerGenerationData.timestamp <= end_date
        ]
        
        if unit_names:
            conditions.append(TaipowerGenerationData.unit_name.in_(unit_names))
        if generation_types:
            conditions.append(TaipowerGenerationData.generation_type.in_(generation_types))
        
        # Query all timestamps with their hour
        timestamp_query = select(
            TaipowerGenerationData.timestamp,
            func.date(TaipowerGenerationData.timestamp).label('date'),
            func.extract('hour', TaipowerGenerationData.timestamp).label('hour')
        ).distinct()
        
        timestamp_query = timestamp_query.where(and_(*conditions))
        
        result = await self.db.execute(timestamp_query)
        all_snapshots = result.all()
        
        # Process daily and hourly availability
        daily_availability = {}
        hourly_distribution = {h: 0 for h in range(24)}
        
        for snapshot in all_snapshots:
            date_str = snapshot.date.isoformat()
            hour = int(snapshot.hour)
            
            if date_str not in daily_availability:
                daily_availability[date_str] = {
                    'snapshot_count': 0,
                    'hourly_coverage': {h: 0 for h in range(24)},
                    'coverage_percent': 0
                }
            
            daily_availability[date_str]['snapshot_count'] += 1
            daily_availability[date_str]['hourly_coverage'][hour] += 1
            hourly_distribution[hour] += 1
        
        # Calculate coverage percentage for each day
        # Assuming 4 snapshots per hour (every 15 minutes) for 24 hours = 96 snapshots/day
        expected_snapshots_per_day = 96
        for date_str in daily_availability:
            day_data = daily_availability[date_str]
            day_data['coverage_percent'] = min(
                100,
                round((day_data['snapshot_count'] / expected_snapshots_per_day) * 100, 1)
            )
        
        # Monthly summary if showing yearly view
        monthly_summary = {}
        if not month:
            for date_str in daily_availability:
                date_obj = datetime.fromisoformat(date_str)
                month_num = date_obj.month
                
                if month_num not in monthly_summary:
                    monthly_summary[month_num] = {
                        'snapshot_count': 0,
                        'days_with_data': 0,
                        'coverage_percent': 0
                    }
                
                monthly_summary[month_num]['snapshot_count'] += daily_availability[date_str]['snapshot_count']
                monthly_summary[month_num]['days_with_data'] += 1
            
            # Calculate coverage for each month
            for month_num in monthly_summary:
                from calendar import monthrange
                days_in_month = monthrange(year, month_num)[1]
                expected_snapshots = days_in_month * expected_snapshots_per_day
                monthly_summary[month_num]['coverage_percent'] = min(
                    100,
                    round((monthly_summary[month_num]['snapshot_count'] / expected_snapshots) * 100, 1)
                )
        
        # Calculate overall statistics
        total_snapshots = sum(d['snapshot_count'] for d in daily_availability.values())
        days_with_data = len(daily_availability)
        total_days = (end_date.date() - start_date.date()).days + 1
        
        # Calculate average snapshots per day (only for days with data)
        avg_snapshots_per_day = (total_snapshots / days_with_data) if days_with_data > 0 else 0
        
        # Overall coverage based on expected snapshots
        expected_total_snapshots = total_days * expected_snapshots_per_day
        overall_coverage_percent = min(
            100,
            round((total_snapshots / expected_total_snapshots) * 100, 1)
        ) if expected_total_snapshots > 0 else 0
        
        # Get time range
        time_range_query = select(
            func.min(TaipowerGenerationData.timestamp),
            func.max(TaipowerGenerationData.timestamp)
        ).where(and_(*conditions))
        
        result = await self.db.execute(time_range_query)
        first_snapshot, last_snapshot = result.one()
        
        return {
            "year": year,
            "month": month,
            "daily_availability": daily_availability,
            "monthly_summary": monthly_summary,
            "hourly_distribution": hourly_distribution,
            "statistics": {
                "total_snapshots": total_snapshots,
                "days_with_data": days_with_data,
                "total_days": total_days,
                "overall_coverage_percent": overall_coverage_percent,
                "average_snapshots_per_day": round(avg_snapshots_per_day, 1),
                "unit_count": len(unit_names) if unit_names else None,
            },
            "time_range": {
                "first_snapshot": first_snapshot.isoformat() if first_snapshot else None,
                "last_snapshot": last_snapshot.isoformat() if last_snapshot else None
            },
            "filters": {
                "unit_names": unit_names,
                "generation_types": generation_types
            }
        }