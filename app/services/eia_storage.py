"""Service for storing and retrieving EIA generation data."""

from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional
from uuid import UUID

from sqlalchemy import select, and_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.eia_generation_data import EIAGenerationData
from app.models.generation_unit import GenerationUnit
from app.models.user import User
from app.services.eia_client import EIAClient


class EIAStorageService:
    """Service for managing EIA generation data storage."""
    
    def __init__(self, db: AsyncSession):
        """Initialize the service with database session."""
        self.db = db
        self.client = EIAClient()
    
    async def fetch_and_store_generation(
        self,
        windfarm_id: int,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        current_user: User,
        store_data: bool = True
    ) -> Dict:
        """
        Fetch monthly generation data from EIA API and optionally store it.
        
        Args:
            windfarm_id: ID of the windfarm
            start_year: Start year for data fetch
            start_month: Start month (1-12)
            end_year: End year for data fetch
            end_month: End month (1-12)
            current_user: Current authenticated user
            store_data: Whether to store the fetched data
            
        Returns:
            Dictionary containing fetched data and metadata
        """
        # Get generation units for windfarm
        result = await self.db.execute(
            select(GenerationUnit).where(
                GenerationUnit.windfarm_id == windfarm_id,
                GenerationUnit.is_active == True,
                GenerationUnit.source.in_(["EIA", None])
            )
        )
        generation_units = result.scalars().all()
        
        if not generation_units:
            return {
                "success": False,
                "error": "No generation units found for windfarm",
                "data": [],
                "metadata": {},
                "stored": False,
                "records_stored": 0
            }
        
        # Extract plant codes
        plant_codes = [unit.code for unit in generation_units if unit.code]
        
        if not plant_codes:
            return {
                "success": False,
                "error": "Generation units do not have codes configured",
                "data": [],
                "metadata": {},
                "stored": False,
                "records_stored": 0
            }
        
        try:
            # Fetch from EIA API
            df, metadata = await self.client.fetch_monthly_generation_data(
                plant_codes=plant_codes,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month
            )
            
            if df.empty:
                return {
                    "data": [],
                    "metadata": metadata,
                    "stored": False,
                    "records_stored": 0,
                    "records_fetched": 0
                }
            
            # Prepare records for storage
            gen_unit_map = {unit.code: unit for unit in generation_units}
            records_dict = {}  # Use dict to deduplicate by unique key
            
            for _, row in df.iterrows():
                period = row.get("period", "")  # YYYY-MM format
                if period and "-" in period:
                    year, month = map(int, period.split("-"))
                else:
                    continue
                
                plant_code = str(row.get("plantCode", ""))
                fuel_type = row.get("fuel2002", "WND")
                
                # Create unique key for deduplication
                unique_key = (period, plant_code, fuel_type)
                
                # If we already have this record, sum the generation values
                if unique_key in records_dict:
                    existing_gen = records_dict[unique_key].get("generation", 0)
                    new_gen = Decimal(str(row.get("generation", 0)))
                    records_dict[unique_key]["generation"] = existing_gen + new_gen
                else:
                    gen_unit = gen_unit_map.get(plant_code)
                    
                    from datetime import datetime
                    from uuid import uuid4
                    
                    record = {
                        "id": uuid4(),
                        "period": period,
                        "year": year,
                        "month": month,
                        "plant_code": plant_code,
                        "plant_name": row.get("plantName"),
                        "state": row.get("state"),
                        "generation": Decimal(str(row.get("generation", 0))),
                        "fuel_type": fuel_type,
                        "unit": "MWh",
                        "generation_unit_id": gen_unit.id if gen_unit else None,
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
                stored_count = await self.store_generation_data_batch(
                    records_to_store, current_user
                )
            
            return {
                "data": records_to_store,
                "metadata": metadata,
                "stored": store_data,
                "records_stored": stored_count,
                "records_fetched": len(records_to_store)
            }
            
        except Exception as e:
            # Error fetching and storing EIA data
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "metadata": {},
                "stored": False,
                "records_stored": 0
            }
    
    async def store_generation_data_batch(
        self, data: List[Dict], user: User
    ) -> int:
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
        
        # Process in batches
        batch_size = 100
        total_stored = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            try:
                # Use PostgreSQL's ON CONFLICT for efficient upsert
                stmt = insert(EIAGenerationData).values(batch)
                
                # On conflict, update the value
                stmt = stmt.on_conflict_do_update(
                    index_elements=['period', 'plant_code', 'fuel_type'],
                    set_={
                        'generation': stmt.excluded.generation,
                        'plant_name': stmt.excluded.plant_name,
                        'state': stmt.excluded.state,
                        'updated_at': datetime.utcnow(),
                        'updated_by_id': user.id
                    }
                )
                
                await self.db.execute(stmt)
                await self.db.commit()
                total_stored += len(batch)
                
            except Exception as e:
                await self.db.rollback()
                # Log error with full details
                import structlog
                logger = structlog.get_logger()
                logger.error(
                    "Error storing EIA batch",
                    error=str(e),
                    error_type=type(e).__name__,
                    batch_size=len(batch)
                )
                # Don't raise - continue with next batch
                continue
        
        return total_stored
    
    async def get_stored_data(
        self,
        plant_codes: List[str],
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        include_gaps: bool = True
    ) -> Dict:
        """
        Retrieve stored data with optional gap detection.
        
        Args:
            plant_codes: List of plant codes
            start_year: Start year for data retrieval
            start_month: Start month (1-12)
            end_year: End year for data retrieval
            end_month: End month (1-12)
            include_gaps: Whether to include gap detection
            
        Returns:
            Dictionary containing data, gaps, and statistics
        """
        # Build period range
        start_period = f"{start_year:04d}-{start_month:02d}"
        end_period = f"{end_year:04d}-{end_month:02d}"
        
        # Query stored data
        result = await self.db.execute(
            select(EIAGenerationData)
            .where(
                and_(
                    EIAGenerationData.plant_code.in_(plant_codes),
                    EIAGenerationData.period >= start_period,
                    EIAGenerationData.period <= end_period
                )
            )
            .order_by(EIAGenerationData.period, EIAGenerationData.plant_code)
        )
        data = result.scalars().all()
        
        # Detect gaps if requested
        gaps = []
        if include_gaps:
            gaps = await self.detect_monthly_gaps(
                plant_codes, start_year, start_month, end_year, end_month
            )
        
        # Calculate statistics
        statistics = self.calculate_coverage_statistics(
            data, plant_codes, start_year, start_month, end_year, end_month
        )
        
        return {
            "data": data,
            "gaps": gaps,
            "statistics": statistics
        }
    
    async def detect_monthly_gaps(
        self,
        plant_codes: List[str],
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int
    ) -> List[Dict]:
        """
        Detect gaps in monthly data.
        
        Args:
            plant_codes: List of plant codes
            start_year: Start year
            start_month: Start month
            end_year: End year
            end_month: End month
            
        Returns:
            List of gap information dictionaries
        """
        # Generate expected periods
        expected_periods = []
        current = date(start_year, start_month, 1)
        end = date(end_year, end_month, 1)
        
        while current <= end:
            expected_periods.append(current.strftime("%Y-%m"))
            # Move to next month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
        
        # Query existing periods
        result = await self.db.execute(
            select(
                EIAGenerationData.period,
                EIAGenerationData.plant_code
            ).where(
                and_(
                    EIAGenerationData.plant_code.in_(plant_codes),
                    EIAGenerationData.period >= f"{start_year:04d}-{start_month:02d}",
                    EIAGenerationData.period <= f"{end_year:04d}-{end_month:02d}"
                )
            )
        )
        existing = {(row.period, row.plant_code) for row in result}
        
        # Find gaps
        gaps = []
        for period in expected_periods:
            missing_plants = []
            for plant_code in plant_codes:
                if (period, plant_code) not in existing:
                    missing_plants.append(plant_code)
            
            if missing_plants:
                gaps.append({
                    "period": period,
                    "missing_plants": missing_plants,
                    "missing_count": len(missing_plants),
                    "total_expected": len(plant_codes),
                    "coverage_percent": ((len(plant_codes) - len(missing_plants)) / len(plant_codes)) * 100
                })
        
        return gaps
    
    def calculate_coverage_statistics(
        self,
        data: List[EIAGenerationData],
        plant_codes: List[str],
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int
    ) -> Dict:
        """
        Calculate data coverage statistics.
        
        Args:
            data: List of stored data records
            plant_codes: List of plant codes
            start_year: Start year
            start_month: Start month
            end_year: End year
            end_month: End month
            
        Returns:
            Dictionary containing coverage statistics
        """
        # Calculate expected total records
        current = date(start_year, start_month, 1)
        end = date(end_year, end_month, 1)
        months = 0
        
        while current <= end:
            months += 1
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
        
        expected_total = months * len(plant_codes)
        actual_records = len(data)
        
        # Calculate coverage by plant
        plant_counts = {}
        total_generation = Decimal('0')
        for record in data:
            if record.plant_code not in plant_counts:
                plant_counts[record.plant_code] = 0
            plant_counts[record.plant_code] += 1
            total_generation += record.generation
        
        coverage_percent = (actual_records / expected_total * 100) if expected_total > 0 else 0
        
        return {
            "start_period": f"{start_year:04d}-{start_month:02d}",
            "end_period": f"{end_year:04d}-{end_month:02d}",
            "months": months,
            "plant_codes": plant_codes,
            "expected_records": expected_total,
            "actual_records": actual_records,
            "missing_records": expected_total - actual_records,
            "coverage_percent": round(coverage_percent, 2),
            "total_generation_mwh": float(total_generation),
            "plant_coverage": {
                plant_code: {
                    "records": plant_counts.get(plant_code, 0),
                    "expected": months,
                    "coverage_percent": round((plant_counts.get(plant_code, 0) / months * 100), 2) if months > 0 else 0
                }
                for plant_code in plant_codes
            }
        }
    
    async def get_data_availability(
        self,
        plant_codes: List[str],
        year: int
    ) -> Dict:
        """
        Get data availability for a specific year.
        
        Args:
            plant_codes: List of plant codes
            year: Year to check
            
        Returns:
            Dictionary containing availability information
        """
        # Query distinct periods with data
        result = await self.db.execute(
            select(EIAGenerationData.period)
            .distinct()
            .where(
                and_(
                    EIAGenerationData.plant_code.in_(plant_codes),
                    EIAGenerationData.year == year
                )
            )
        )
        periods_with_data = [row[0] for row in result]
        
        # Calculate availability by month
        availability_by_month = {}
        for month in range(1, 13):
            period = f"{year:04d}-{month:02d}"
            availability_by_month[period] = period in periods_with_data
        
        return {
            "year": year,
            "periods_with_data": periods_with_data,
            "availability_by_month": availability_by_month,
            "statistics": {
                "months_with_data": len(periods_with_data),
                "months_in_year": 12,
                "coverage_percent": round((len(periods_with_data) / 12 * 100), 2)
            }
        }