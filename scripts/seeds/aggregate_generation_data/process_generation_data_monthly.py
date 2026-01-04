"""
Monthly Generation Data Processing Script

Processes monthly raw generation data (EIA, ENERGISTYRELSEN) into monthly aggregated records.
Unlike daily processing, this creates one record per unit per month.

Usage:
    # Process all months in a date range
    poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2020-01 --end 2024-12

    # Process specific source only
    poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2020-01 --end 2024-12 --source EIA

    # Dry run (no database changes)
    poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2020-01 --end 2024-12 --dry-run
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
from decimal import Decimal
import argparse
import sys
from dataclasses import dataclass
from collections import defaultdict
from uuid import uuid4
from pathlib import Path
from dateutil.relativedelta import relativedelta

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, delete, func

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw, GenerationData
from app.models.generation_unit import GenerationUnit
from app.models.turbine_unit import TurbineUnit

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class MonthlyRecord:
    """Intermediate representation of monthly aggregated data."""
    month: datetime  # First day of month at 00:00:00 UTC
    identifier: str
    generation_mwh: float
    capacity_mw: Optional[float]
    raw_data_ids: List[int]
    metadata: Dict[str, Any]


class MonthlyGenerationProcessor:
    """Process monthly generation data."""

    MONTHLY_SOURCES = ['EIA', 'ENERGISTYRELSEN']

    def __init__(self, db_session: AsyncSession, dry_run: bool = False):
        self.db = db_session
        self.dry_run = dry_run
        self.generation_units_cache = {}
        self.turbine_units_cache = {}  # For ENERGISTYRELSEN GSRN codes
        self.stats = {
            'raw_records_processed': 0,
            'monthly_records_created': 0,
            'errors': 0
        }

    async def process_month(
        self,
        year: int,
        month: int,
        sources: Optional[List[str]] = None,
        skip_unit_load: bool = False
    ) -> Dict[str, Any]:
        """Process all data for a specific month."""

        # Create month start/end timestamps
        month_start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)

        # Calculate next month
        if month == 12:
            month_end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        else:
            month_end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        sources = sources or self.MONTHLY_SOURCES

        logger.info(f"Processing data for {year}-{month:02d}")
        logger.info(f"Sources: {', '.join(sources)}")

        # Load generation units and turbine units only if not already cached
        if not skip_unit_load:
            if not self.generation_units_cache:
                await self.load_generation_units()
            if not self.turbine_units_cache:
                await self.load_turbine_units()

        results = {}

        for source in sources:
            try:
                source_result = await self.process_source_for_month(
                    source, month_start, month_end
                )
                results[source] = source_result

            except Exception as e:
                logger.error(f"Error processing {source}: {e}", exc_info=True)
                self.stats['errors'] += 1
                results[source] = {'error': str(e)}
                # Rollback the failed transaction
                await self.db.rollback()

        # Note: Commit happens at the session level in process_month_range
        # Don't commit here since we're processing multiple months in one session

        return {
            'year': year,
            'month': month,
            'sources': results,
            'stats': self.stats
        }

    async def load_generation_units(self):
        """Load generation units into memory for faster lookups."""

        result = await self.db.execute(select(GenerationUnit))
        units = result.scalars().all()

        for unit in units:
            key = f"{unit.source}:{unit.code}"
            self.generation_units_cache[key] = {
                'id': unit.id,
                'windfarm_id': unit.windfarm_id,
                'capacity_mw': float(unit.capacity_mw) if unit.capacity_mw else None,
                'name': unit.name,
                'start_date': unit.start_date,
                'end_date': unit.end_date
            }

        logger.info(f"Loaded {len(self.generation_units_cache)} generation units")

    async def load_turbine_units(self):
        """Load turbine units with turbine model capacity for ENERGISTYRELSEN GSRN code lookups."""

        from app.models.turbine_model import TurbineModel

        # Join with turbine_model to get capacity information
        result = await self.db.execute(
            select(TurbineUnit, TurbineModel)
            .join(TurbineModel, TurbineUnit.turbine_model_id == TurbineModel.id)
        )
        units_with_models = result.all()

        for turbine_unit, turbine_model in units_with_models:
            # Calculate capacity in MW from turbine model's rated_power_kw
            capacity_mw = float(turbine_model.rated_power_kw / 1000.0) if turbine_model.rated_power_kw else None

            # Key is just the GSRN code (no source prefix needed as it's unique)
            self.turbine_units_cache[turbine_unit.code] = {
                'id': turbine_unit.id,
                'windfarm_id': turbine_unit.windfarm_id,
                'turbine_model_id': turbine_unit.turbine_model_id,
                'capacity_mw': capacity_mw,
                'start_date': turbine_unit.start_date,
                'end_date': turbine_unit.end_date
            }

        logger.info(f"Loaded {len(self.turbine_units_cache)} turbine units with capacity data")

    async def process_source_for_month(
        self,
        source: str,
        month_start: datetime,
        month_end: datetime
    ) -> Dict[str, Any]:
        """Process a single source for one month."""

        logger.info(f"Processing {source} for {month_start.strftime('%Y-%m')}")

        # Fetch raw data for this month
        result = await self.db.execute(
            select(GenerationDataRaw)
            .where(
                and_(
                    GenerationDataRaw.source == source,
                    GenerationDataRaw.period_start >= month_start,
                    GenerationDataRaw.period_start < month_end,
                    GenerationDataRaw.period_type == 'month'
                )
            )
        )

        raw_data = result.scalars().all()
        raw_count = len(raw_data)

        logger.info(f"Found {raw_count} raw records for {source}")

        if raw_count == 0:
            return {
                'raw_records': 0,
                'monthly_records': 0,
                'saved': 0
            }

        self.stats['raw_records_processed'] += raw_count

        # Transform based on source
        if source == 'EIA':
            monthly_records = self.transform_eia(raw_data)
        elif source == 'ENERGISTYRELSEN':
            monthly_records = self.transform_energistyrelsen(raw_data)
        else:
            logger.warning(f"Unknown monthly source: {source}")
            return {'error': f'Unknown source: {source}'}

        logger.info(f"Created {len(monthly_records)} monthly records")

        # Clear existing data for this month/source
        await self.clear_existing_data(source, month_start, month_end)

        # Save records
        saved_count = await self.save_monthly_records(monthly_records, source)
        self.stats['monthly_records_created'] += saved_count

        return {
            'raw_records': raw_count,
            'monthly_records': len(monthly_records),
            'saved': saved_count
        }

    def transform_eia(self, raw_data: List[GenerationDataRaw]) -> List[MonthlyRecord]:
        """Transform EIA monthly data."""

        monthly_records = []

        for record in raw_data:
            # Get unit info
            unit_key = f"EIA:{record.identifier}"
            unit_info = self.generation_units_cache.get(unit_key)

            if not unit_info:
                logger.debug(f"Unit not found: EIA:{record.identifier}")
                continue

            # Parse data JSON (already a dict from JSONB field)
            data_json = record.data if isinstance(record.data, dict) else {}

            # Create monthly record
            monthly_record = MonthlyRecord(
                month=record.period_start,
                identifier=record.identifier,
                generation_mwh=float(record.value_extracted),
                capacity_mw=unit_info['capacity_mw'],
                raw_data_ids=[record.id],
                metadata={
                    'plant_id': data_json.get('plant_id'),
                    'plant_name': data_json.get('plant_name'),
                    'fuel_type': data_json.get('fuel_type', 'WND'),
                    'month_name': data_json.get('month'),
                    'year': data_json.get('year'),
                    'source': 'EIA'
                }
            )

            monthly_records.append(monthly_record)

        return monthly_records

    def transform_energistyrelsen(
        self,
        raw_data: List[GenerationDataRaw]
    ) -> List[MonthlyRecord]:
        """Transform ENERGISTYRELSEN monthly data.

        ENERGISTYRELSEN data uses GSRN codes which are stored in turbine_units.code,
        not generation_units.code (which has placeholder 'nan' values).
        """

        monthly_records = []
        skipped_units = set()
        matched_units = 0

        for record in raw_data:
            # ENERGISTYRELSEN identifiers are GSRN codes - look them up in turbine_units
            turbine_unit = self.turbine_units_cache.get(record.identifier)

            if not turbine_unit:
                skipped_units.add(record.identifier)
                continue

            matched_units += 1

            # Parse data JSON (already a dict from JSONB field)
            data_json = record.data if isinstance(record.data, dict) else {}

            # Get capacity from turbine unit (from turbine model's rated_power_kw)
            capacity_mw = turbine_unit.get('capacity_mw')

            # Create monthly record with turbine_unit info
            monthly_record = MonthlyRecord(
                month=record.period_start,
                identifier=record.identifier,
                generation_mwh=float(record.value_extracted),
                capacity_mw=capacity_mw,
                raw_data_ids=[record.id],
                metadata={
                    'unit_code': data_json.get('unit_code'),
                    'unit_name': data_json.get('unit_name'),
                    'gsrn': data_json.get('gsrn'),
                    'generation_kwh': data_json.get('generation_kwh'),
                    'month': data_json.get('month'),
                    'source': 'ENERGISTYRELSEN',
                    'turbine_unit_id': turbine_unit['id'],
                    'windfarm_id': turbine_unit['windfarm_id']
                }
            )

            monthly_records.append(monthly_record)

        # Log summary
        logger.info(f"Matched {matched_units} turbine units")
        if skipped_units:
            logger.warning(f"Skipped {len(skipped_units)} GSRN codes not found in turbine_units")
            logger.warning(f"First 10 skipped GSRN codes: {list(skipped_units)[:10]}")
            logger.info(f"Total turbine units in cache: {len(self.turbine_units_cache)}")

        return monthly_records

    async def clear_existing_data(
        self,
        source: str,
        month_start: datetime,
        month_end: datetime
    ):
        """Clear existing data for re-processing (idempotent)."""

        result = await self.db.execute(
            delete(GenerationData)
            .where(
                and_(
                    GenerationData.source == source,
                    GenerationData.hour >= month_start,
                    GenerationData.hour < month_end,
                    GenerationData.source_resolution == 'monthly'
                )
            )
            .returning(GenerationData.id)
        )

        deleted_count = len(result.all())
        if deleted_count > 0:
            logger.info(f"Cleared {deleted_count} existing monthly records for {source}")

    async def save_monthly_records(
        self,
        monthly_records: List[MonthlyRecord],
        source: str
    ) -> int:
        """Save monthly records to database."""

        generation_data_objects = []

        for record in monthly_records:
            # Calculate capacity factor
            capacity_factor = None
            if record.capacity_mw and record.capacity_mw > 0:
                # For monthly data, capacity factor is:
                # monthly_generation_mwh / (capacity_mw * hours_in_month)
                # Calculate actual hours in the specific month
                import calendar
                year = record.month.year
                month = record.month.month
                days_in_month = calendar.monthrange(year, month)[1]
                hours_in_month = days_in_month * 24

                monthly_capacity = record.capacity_mw * hours_in_month
                calculated_cf = record.generation_mwh / monthly_capacity
                capacity_factor = min(calculated_cf, 9.9999)

            # Get unit info - for ENERGISTYRELSEN, this comes from metadata (turbine_units)
            # For other sources, it comes from generation_units_cache
            turbine_unit_id = record.metadata.get('turbine_unit_id') if record.metadata else None
            windfarm_id = record.metadata.get('windfarm_id') if record.metadata else None
            generation_unit_id = None

            # If not in metadata (e.g., EIA), try generation_units_cache
            if not turbine_unit_id and not windfarm_id:
                unit_key = f"{source}:{record.identifier}"
                unit_info = self.generation_units_cache.get(unit_key)
                if unit_info:
                    generation_unit_id = unit_info['id']
                    windfarm_id = unit_info['windfarm_id']

            # Create GenerationData object
            obj = GenerationData(
                id=str(uuid4()),
                hour=record.month,  # Store as first of month
                generation_unit_id=generation_unit_id,
                windfarm_id=windfarm_id,
                turbine_unit_id=turbine_unit_id,
                generation_mwh=Decimal(str(record.generation_mwh)),
                capacity_mw=Decimal(str(record.capacity_mw)) if record.capacity_mw else None,
                capacity_factor=Decimal(str(capacity_factor)) if capacity_factor else None,
                raw_capacity_mw=None,
                raw_capacity_factor=None,
                source=source,
                source_resolution='monthly',  # Important: mark as monthly
                raw_data_ids=record.raw_data_ids,
                quality_flag='HIGH',  # Monthly data is typically complete
                quality_score=Decimal('1.0'),
                completeness=Decimal('1.0'),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )

            generation_data_objects.append(obj)

        # Bulk insert
        if generation_data_objects:
            self.db.add_all(generation_data_objects)

        logger.info(f"Saved {len(generation_data_objects)} monthly records for {source}")

        return len(generation_data_objects)


async def process_month_range(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    sources: Optional[List[str]] = None,
    dry_run: bool = False
):
    """Process a range of months."""

    # Create session factory
    session_factory = get_session_factory()

    # Calculate months to process
    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)

    months_to_process = []
    while current_date <= end_date:
        months_to_process.append((current_date.year, current_date.month))
        current_date += relativedelta(months=1)

    logger.info(f"Processing {len(months_to_process)} months")

    results = []

    # OPTIMIZATION: Use one database session and processor for all months
    # This way we only load generation units and turbine units once
    async with session_factory() as db:
        processor = MonthlyGenerationProcessor(db, dry_run=dry_run)

        # Load generation units once at the start
        logger.info("Loading generation units (one-time operation)...")
        await processor.load_generation_units()
        logger.info(f"✓ Loaded {len(processor.generation_units_cache)} generation units")

        # Only load turbine units if ENERGISTYRELSEN is being processed (not needed for EIA)
        if sources is None or 'ENERGISTYRELSEN' in sources:
            logger.info("Loading turbine units for ENERGISTYRELSEN (one-time operation)...")
            await processor.load_turbine_units()
            logger.info(f"✓ Loaded {len(processor.turbine_units_cache)} turbine units")

        for year, month in months_to_process:
            try:
                # Reset stats for each month
                processor.stats = {
                    'raw_records_processed': 0,
                    'monthly_records_created': 0,
                    'errors': 0
                }

                result = await processor.process_month(year, month, sources, skip_unit_load=True)
                results.append(result)

                logger.info(
                    f"✓ {year}-{month:02d}: "
                    f"{processor.stats['raw_records_processed']} raw → "
                    f"{processor.stats['monthly_records_created']} monthly"
                )

            except Exception as e:
                logger.error(f"✗ {year}-{month:02d}: {e}", exc_info=True)
                results.append({
                    'year': year,
                    'month': month,
                    'error': str(e)
                })
                # Don't break - continue with next month

        # Commit all changes at once (or rollback if dry run)
        if not dry_run:
            await db.commit()
            logger.info("✓ All changes committed to database")
        else:
            await db.rollback()
            logger.info("✓ Dry run - all changes rolled back")

    # Print summary
    print("\n" + "="*60)
    print("MONTHLY PROCESSING SUMMARY")
    print("="*60)
    print(f"Months processed: {len(months_to_process)}")
    print(f"Sources: {', '.join(sources) if sources else 'EIA, ENERGISTYRELSEN'}")

    total_raw = sum(
        r.get('stats', {}).get('raw_records_processed', 0)
        for r in results if 'error' not in r
    )
    total_monthly = sum(
        r.get('stats', {}).get('monthly_records_created', 0)
        for r in results if 'error' not in r
    )

    print(f"Total raw records: {total_raw:,}")
    print(f"Total monthly records: {total_monthly:,}")

    if dry_run:
        print("\nDRY RUN - No changes made to database")

    print("="*60)

    return results


async def main():
    """Main entry point."""

    parser = argparse.ArgumentParser(
        description='Process monthly generation data (EIA, ENERGISTYRELSEN)'
    )

    parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='Start month (YYYY-MM)'
    )
    parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='End month (YYYY-MM)'
    )
    parser.add_argument(
        '--source',
        type=str,
        choices=['EIA', 'ENERGISTYRELSEN'],
        help='Process only specific source'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without making database changes'
    )

    args = parser.parse_args()

    # Parse dates
    try:
        start_year, start_month = map(int, args.start.split('-'))
        end_year, end_month = map(int, args.end.split('-'))
    except ValueError:
        print("Error: Dates must be in YYYY-MM format")
        sys.exit(1)

    # Validate months
    if not (1 <= start_month <= 12 and 1 <= end_month <= 12):
        print("Error: Month must be between 01 and 12")
        sys.exit(1)

    # Check date range
    start_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)
    if start_date > end_date:
        print("Error: Start month must be before or equal to end month")
        sys.exit(1)

    # Determine sources
    sources = [args.source] if args.source else None

    # Process months
    try:
        await process_month_range(
            start_year, start_month,
            end_year, end_month,
            sources=sources,
            dry_run=args.dry_run
        )
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
