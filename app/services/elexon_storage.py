"""Service for storing and retrieving Elexon generation data."""

from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple, Any
from uuid import UUID
import calendar
import pandas as pd

from sqlalchemy import select, update, cast, Date, extract, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from app.models.elexon_generation_data import ElexonGenerationData
from app.models.generation_unit import GenerationUnit
from app.models.user import User
from app.services.elexon_client import ElexonClient


class ElexonStorageService:
    """Service for managing Elexon generation data storage."""

    def __init__(self, db: AsyncSession):
        """Initialize the service with database session."""
        self.db = db
        self.client = ElexonClient()

    async def fetch_and_store_generation(
        self,
        start_date: datetime,
        end_date: datetime,
        bm_units: List[str],
        generation_units: List[GenerationUnit],
        current_user: User,
        store_data: bool = True
    ) -> Dict:
        """
        Fetch generation data from Elexon API and optionally store it.
        
        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch
            bm_units: List of BM Unit IDs
            generation_units: List of GenerationUnit objects
            current_user: Current authenticated user
            store_data: Whether to store the fetched data
            
        Returns:
            Dictionary containing fetched data and metadata
        """
        try:
            # Fetch data from Elexon API
            df, metadata = await self.client.fetch_physical_data(
                start=start_date,
                end=end_date,
                bm_units=bm_units
            )
            
            if df.empty:
                return {
                    "data": [],
                    "metadata": metadata,
                    "stored": False,
                    "records_stored": 0
                }
            
            # Create generation unit map for quick lookup
            gen_unit_map = {unit.code: unit for unit in generation_units}
            
            # Prepare data for storage
            records_to_store = []
            for _, row in df.iterrows():
                # Ensure timestamp is a Python datetime object
                timestamp = row["timestamp"]
                if isinstance(timestamp, str):
                    timestamp = pd.to_datetime(timestamp).to_pydatetime()
                elif isinstance(timestamp, pd.Timestamp):
                    timestamp = timestamp.to_pydatetime()
                    
                record = {
                    "timestamp": timestamp,
                    "bm_unit": row["bm_unit"],
                    "settlement_period": row.get("settlement_period"),
                    "value": row["value"],
                    "unit": row["unit"],
                    "generation_unit_id": gen_unit_map.get(row["bm_unit"]).id if row["bm_unit"] in gen_unit_map else None
                }
                records_to_store.append(record)
            
            # Store data if requested
            stored_count = 0
            if store_data:
                stored_count = await self.store_generation_data_batch(records_to_store, current_user)
            
            return {
                "data": records_to_store,
                "metadata": metadata,
                "stored": store_data,
                "records_stored": stored_count,
                "records_fetched": len(records_to_store)
            }
            
        except Exception as e:
            # Error fetching and storing Elexon data
            raise

    async def store_generation_data_batch(self, data: List[Dict], user: User) -> int:
        """
        Store generation data using batch operations with upsert.
        
        Args:
            data: List of data records to store
            user: User performing the operation
            
        Returns:
            Number of records stored/updated
        """
        if not data:
            return 0
            
        # Prepare records for batch insert
        records_to_insert = []
        
        for record in data:
            # Ensure timestamp is a datetime object
            timestamp = record['timestamp']
            if isinstance(timestamp, str):
                timestamp = pd.to_datetime(timestamp).to_pydatetime()
            elif isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.to_pydatetime()
            
            records_to_insert.append({
                "timestamp": timestamp,
                "bm_unit": record['bm_unit'],
                "settlement_period": record.get('settlement_period'),
                "value": record['value'],
                "unit": record['unit'],
                "generation_unit_id": record.get('generation_unit_id'),
                "created_by_id": user.id,
                "updated_by_id": user.id,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            })
        
        # Process in batches to avoid connection timeout
        batch_size = 100
        total_stored = 0
        
        for i in range(0, len(records_to_insert), batch_size):
            batch = records_to_insert[i:i + batch_size]
            
            try:
                # Use PostgreSQL's ON CONFLICT for efficient upsert
                stmt = insert(ElexonGenerationData).values(batch)
                
                # On conflict, update the value and timestamps
                stmt = stmt.on_conflict_do_update(
                    index_elements=['timestamp', 'bm_unit', 'settlement_period'],
                    set_={
                        'value': stmt.excluded.value,
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
                print(f"Error storing batch: {str(e)}")
                continue
        
        return total_stored

    async def store_generation_data(self, data: List[Dict], user: User) -> int:
        """
        Store generation data with collision handling (legacy method for compatibility).
        
        Args:
            data: List of data records to store
            user: User performing the operation
            
        Returns:
            Number of records stored/updated
        """
        # Use the batch method instead
        return await self.store_generation_data_batch(data, user)

    async def get_stored_data(
        self,
        bm_units: List[str],
        start_date: datetime,
        end_date: datetime,
        include_gaps: bool = True
    ) -> Dict:
        """
        Retrieve stored data with optional gap detection.
        
        Args:
            bm_units: List of BM Unit IDs
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            include_gaps: Whether to include gap detection
            
        Returns:
            Dictionary containing data, gaps, and statistics
        """
        # Query stored data
        result = await self.db.execute(
            select(ElexonGenerationData)
            .where(
                and_(
                    ElexonGenerationData.bm_unit.in_(bm_units),
                    ElexonGenerationData.timestamp >= start_date,
                    ElexonGenerationData.timestamp <= end_date
                )
            )
            .order_by(ElexonGenerationData.timestamp, ElexonGenerationData.settlement_period)
        )
        data = result.scalars().all()
        
        # Detect gaps if requested
        gaps = []
        if include_gaps:
            gaps = self.detect_settlement_period_gaps(data, bm_units, start_date, end_date)
        
        # Calculate statistics
        statistics = self.calculate_coverage_statistics(data, bm_units, start_date, end_date)
        
        return {
            "data": data,
            "gaps": gaps,
            "statistics": statistics
        }

    def detect_settlement_period_gaps(
        self,
        data: List[ElexonGenerationData],
        bm_units: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict]:
        """
        Detect gaps in settlement period data.
        
        Args:
            data: List of stored data records
            bm_units: List of BM Unit IDs
            start_date: Start date for gap detection
            end_date: End date for gap detection
            
        Returns:
            List of gap information dictionaries
        """
        gaps = []
        
        # Build set of existing (date, bm_unit, settlement_period) tuples
        existing_periods = set()
        for record in data:
            date_key = record.timestamp.date()
            period_key = (date_key, record.bm_unit, record.settlement_period)
            existing_periods.add(period_key)
        
        # Check each BM unit separately
        for bm_unit in bm_units:
            unit_gaps = []
            current_date = start_date.date()
            end_date_normalized = end_date.date()
            
            while current_date <= end_date_normalized:
                # Get expected periods for this date (48 or 46/50 on clock change days)
                expected_periods = self.get_expected_periods_for_date(current_date)
                
                # Check for missing periods
                missing_periods = []
                for period in range(1, expected_periods + 1):
                    if (current_date, bm_unit, period) not in existing_periods:
                        missing_periods.append(period)
                
                # Record gaps for this date if any
                if missing_periods:
                    unit_gaps.append({
                        "date": current_date.isoformat(),
                        "bm_unit": bm_unit,
                        "missing_periods": missing_periods,
                        "missing_count": len(missing_periods),
                        "total_expected": expected_periods,
                        "coverage_percent": ((expected_periods - len(missing_periods)) / expected_periods) * 100
                    })
                
                current_date += timedelta(days=1)
            
            if unit_gaps:
                gaps.extend(unit_gaps)
        
        return gaps

    def get_expected_periods_for_date(self, check_date: date) -> int:
        """
        Get expected number of settlement periods for a given date.
        
        Args:
            check_date: Date to check
            
        Returns:
            Number of expected settlement periods (46, 48, or 50)
        """
        # UK clock changes typically occur on last Sunday of March and October
        # Spring forward (March): 46 periods
        # Fall back (October): 50 periods
        # Normal days: 48 periods
        
        # Simplified logic - in production, use proper timezone library
        if check_date.month == 3 and check_date.weekday() == 6:  # Last Sunday of March
            # Check if it's the last Sunday
            next_week = check_date + timedelta(days=7)
            if next_week.month != 3:
                return 46  # Spring forward
        elif check_date.month == 10 and check_date.weekday() == 6:  # Last Sunday of October
            # Check if it's the last Sunday
            next_week = check_date + timedelta(days=7)
            if next_week.month != 10:
                return 50  # Fall back
        
        return 48  # Normal day

    def calculate_coverage_statistics(
        self,
        data: List[ElexonGenerationData],
        bm_units: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> Dict:
        """
        Calculate data coverage statistics.
        
        Args:
            data: List of stored data records
            bm_units: List of BM Unit IDs
            start_date: Start date for statistics
            end_date: End date for statistics
            
        Returns:
            Dictionary containing coverage statistics
        """
        # Calculate expected total records
        days = (end_date.date() - start_date.date()).days + 1
        expected_periods_per_day = 48  # Simplified - should account for clock changes
        expected_total = days * expected_periods_per_day * len(bm_units)
        
        # Count actual records
        actual_records = len(data)
        
        # Calculate coverage by BM unit
        bm_unit_counts = {}
        for record in data:
            if record.bm_unit not in bm_unit_counts:
                bm_unit_counts[record.bm_unit] = 0
            bm_unit_counts[record.bm_unit] += 1
        
        # Calculate statistics
        coverage_percent = (actual_records / expected_total * 100) if expected_total > 0 else 0
        
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "days": days,
            "bm_units": bm_units,
            "expected_records": expected_total,
            "actual_records": actual_records,
            "missing_records": expected_total - actual_records,
            "coverage_percent": round(coverage_percent, 2),
            "bm_unit_coverage": {
                bm_unit: {
                    "records": bm_unit_counts.get(bm_unit, 0),
                    "expected": days * expected_periods_per_day,
                    "coverage_percent": round((bm_unit_counts.get(bm_unit, 0) / (days * expected_periods_per_day) * 100), 2)
                }
                for bm_unit in bm_units
            }
        }

    async def get_data_availability(
        self,
        bm_units: List[str],
        year: int,
        month: int
    ) -> Dict:
        """
        Get data availability for a specific month.
        
        Args:
            bm_units: List of BM Unit IDs
            year: Year to check
            month: Month to check
            
        Returns:
            Dictionary containing availability information
        """
        # Query distinct dates with data
        result = await self.db.execute(
            select(cast(ElexonGenerationData.timestamp, Date))
            .distinct()
            .where(
                and_(
                    ElexonGenerationData.bm_unit.in_(bm_units),
                    extract('year', ElexonGenerationData.timestamp) == year,
                    extract('month', ElexonGenerationData.timestamp) == month
                )
            )
        )
        dates_with_data = [row[0] for row in result]
        
        # Get total days in month
        days_in_month = calendar.monthrange(year, month)[1]
        
        # Calculate availability by date
        availability_by_date = {}
        for day in range(1, days_in_month + 1):
            check_date = date(year, month, day)
            availability_by_date[check_date.isoformat()] = check_date in dates_with_data
        
        return {
            "year": year,
            "month": month,
            "dates_with_data": [d.isoformat() for d in dates_with_data],
            "availability_by_date": availability_by_date,
            "statistics": {
                "days_with_data": len(dates_with_data),
                "days_in_month": days_in_month,
                "coverage_percent": round((len(dates_with_data) / days_in_month * 100), 2),
                "expected_periods_per_day": 48  # UK settlement periods
            }
        }