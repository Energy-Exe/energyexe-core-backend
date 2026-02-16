"""
Daily Generation Data Processing Script

Processes raw generation data into hourly harmonized records for a specific day.
Designed for daily cron job execution with proper error handling and logging.

Usage:
    # Process yesterday's data (default)
    poetry run python scripts/process_generation_data_daily.py

    # Process specific date
    poetry run python scripts/process_generation_data_daily.py --date 2024-09-15

    # Process specific source only
    poetry run python scripts/process_generation_data_daily.py --date 2024-09-15 --source ENTSOE

    # Dry run (no database changes)
    poetry run python scripts/process_generation_data_daily.py --dry-run
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any
from decimal import Decimal, ROUND_HALF_UP
import argparse
import sys
from dataclasses import dataclass
from collections import defaultdict
import numpy as np
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, delete, func

# Add parent directories to path for imports
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw, GenerationData
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class HourlyRecord:
    """Intermediate representation of hourly aggregated data."""
    hour: datetime
    identifier: str
    generation_mwh: float
    capacity_mw: Optional[float]
    raw_data_ids: List[int]
    data_points: int
    expected_points: int
    metadata: Dict[str, Any]
    # Consumption tracking (ENTSOE reports both gen and consumption)
    consumption_mwh: Optional[float] = None
    # Curtailment tracking (ELEXON BOAV integration)
    metered_mwh: Optional[float] = None  # What was delivered to grid (B1610)
    curtailed_mwh: Optional[float] = None  # What was curtailed (BOAV bids)


class DailyGenerationProcessor:
    """Process generation data for a single day."""

    SOURCES = ['ENTSOE', 'ELEXON', 'TAIPOWER', 'NVE', 'ENERGISTYRELSEN']

    def __init__(self, db_session: AsyncSession, dry_run: bool = False):
        self.db = db_session
        self.dry_run = dry_run
        self.generation_units_cache = {}
        self.stats = {
            'raw_records_processed': 0,
            'hourly_records_created': 0,
            'errors': 0
        }

    async def process_day(
        self,
        date: datetime,
        sources: Optional[List[str]] = None,
        skip_load_units: bool = False,
        skip_commit: bool = False,
        windfarm_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Process all data for a specific day.

        Args:
            date: Date to process
            sources: List of sources to process
            skip_load_units: Skip loading generation units (for batch processing)
            skip_commit: Skip committing (for batch processing - commit will be done externally)
            windfarm_id: Optional windfarm ID to filter data (only process this windfarm's data)
        """

        # Ensure date is at start of day in UTC
        day_start = date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)

        sources = sources or self.SOURCES

        # Reduce logging in batch mode
        if not skip_commit:
            logger.info(f"Processing data for {day_start.date()}")
            logger.info(f"Sources: {', '.join(sources)}")

        # Load generation units (skip if already loaded for batch processing)
        if not skip_load_units:
            await self.load_generation_units()

        results = {}

        for source in sources:
            try:
                source_result = await self.process_source_for_day(
                    source, day_start, day_end, windfarm_id=windfarm_id
                )
                results[source] = source_result

            except Exception as e:
                logger.error(f"Error processing {source}: {e}")
                self.stats['errors'] += 1
                results[source] = {'error': str(e)}
                # Rollback the failed transaction to reset session state
                await self.db.rollback()

        # Commit if not dry run and not in batch mode
        if not skip_commit:
            if not self.dry_run:
                await self.db.commit()
                logger.info("Changes committed to database")
            else:
                await self.db.rollback()
                logger.info("Dry run - changes rolled back")

        return {
            'date': day_start.isoformat(),
            'sources': results,
            'stats': self.stats
        }

    async def load_generation_units(self):
        """Load generation units into memory for faster lookups.

        For sources like NVE with multiple phases per code, stores lists of units.
        Also loads commercial_operational_date from windfarms for capacity factor calculation.
        """

        # Join with windfarms to get commercial_operational_date
        result = await self.db.execute(
            select(GenerationUnit, Windfarm)
            .outerjoin(Windfarm, GenerationUnit.windfarm_id == Windfarm.id)
            .order_by(GenerationUnit.code, GenerationUnit.start_date)
        )
        rows = result.all()

        # Also build a cache of commercial_operational_date by windfarm_id
        self.windfarm_commercial_dates = {}

        # Group by source:code (multiple phases can have same code)
        for unit, windfarm in rows:
            # Use uppercase for case-insensitive matching (TAIPOWER vs Taipower)
            key = f"{unit.source.upper()}:{unit.code}"

            # Get commercial_operational_date from windfarm if available
            commercial_date = None
            if windfarm and windfarm.commercial_operational_date:
                commercial_date = windfarm.commercial_operational_date
                # Cache by windfarm_id for units without direct windfarm link
                self.windfarm_commercial_dates[windfarm.id] = commercial_date

            unit_info = {
                'id': unit.id,
                'windfarm_id': unit.windfarm_id,
                'capacity_mw': float(unit.capacity_mw) if unit.capacity_mw else None,
                'name': unit.name,
                'start_date': unit.start_date,
                'end_date': unit.end_date,
                'first_power_date': unit.first_power_date,  # For data filtering (takes precedence over start_date)
                'commercial_operational_date': commercial_date  # For capacity factor calculation
            }

            # If key exists, convert to list or append
            if key in self.generation_units_cache:
                existing = self.generation_units_cache[key]
                if isinstance(existing, dict):
                    # Convert single unit to list
                    self.generation_units_cache[key] = [existing, unit_info]
                else:
                    # Append to existing list
                    self.generation_units_cache[key].append(unit_info)
            else:
                # First unit for this code
                self.generation_units_cache[key] = unit_info

        logger.info(f"Loaded generation units from {len(self.generation_units_cache)} unique codes")

    def is_unit_operational(self, unit_info: Dict, check_date: datetime) -> bool:
        """Check if a generation unit is operational on a given date.

        Args:
            unit_info: Unit information from cache
            check_date: Date to check

        Returns:
            True if unit is operational on the date
        """
        if not unit_info:
            return False

        # Remove timezone info from check_date for comparison
        # We compare dates only, not times or timezones
        if hasattr(check_date, 'date'):
            check_date_naive = check_date.replace(tzinfo=None) if check_date.tzinfo else check_date
        else:
            check_date_naive = check_date

        # Check start date - use first_power_date if set, otherwise fallback to start_date
        # first_power_date represents when the unit first generated power (earliest data date)
        # start_date represents when the unit officially started operations
        effective_start = unit_info.get('first_power_date') or unit_info.get('start_date')
        if effective_start:
            # Convert to datetime if it's a date object
            if not isinstance(effective_start, datetime):
                effective_start = datetime.combine(effective_start, datetime.min.time())
            # Remove timezone info if present
            if hasattr(effective_start, 'tzinfo') and effective_start.tzinfo:
                effective_start = effective_start.replace(tzinfo=None)
            if check_date_naive < effective_start:
                return False

        # Check end date
        end_date = unit_info.get('end_date')
        if end_date:
            # Convert to datetime if it's a date object
            if not isinstance(end_date, datetime):
                end_date = datetime.combine(end_date, datetime.max.time())
            # Remove timezone info if present
            if hasattr(end_date, 'tzinfo') and end_date.tzinfo:
                end_date = end_date.replace(tzinfo=None)
            if check_date_naive > end_date:
                return False

        return True

    def get_operational_unit(self, cache_entry, check_date: datetime):
        """Get the operational unit from cache entry.

        Cache entry can be:
        - A single unit dict (for codes with only one unit)
        - A list of unit dicts (for codes with multiple phases)

        Args:
            cache_entry: Entry from generation_units_cache
            check_date: Date to check

        Returns:
            The operational unit info dict, or None
        """
        if not cache_entry:
            return None

        # Handle single unit
        if isinstance(cache_entry, dict):
            if self.is_unit_operational(cache_entry, check_date):
                return cache_entry
            return None

        # Handle list of units (multiple phases)
        if isinstance(cache_entry, list):
            for unit_info in cache_entry:
                if self.is_unit_operational(unit_info, check_date):
                    return unit_info
            return None

        return None

    async def process_source_for_day(
        self,
        source: str,
        day_start: datetime,
        day_end: datetime,
        windfarm_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Process a single source for a specific day.

        Args:
            source: Data source (e.g., 'ELEXON', 'ENTSOE')
            day_start: Start of day (UTC)
            day_end: End of day (UTC)
            windfarm_id: Optional windfarm ID to filter data
        """

        logger.info(f"Processing {source} for {day_start.date()}" + (f" (windfarm_id={windfarm_id})" if windfarm_id else ""))

        # Fetch raw data for the day (filtered by windfarm if specified)
        # For ELEXON, exclude BOAV data from main fetch (we'll fetch it separately)
        # For ENTSOE, exclude consumption source_types from generation fetch
        if source == 'ELEXON':
            raw_data = await self.fetch_raw_data(
                source, day_start, day_end,
                windfarm_id=windfarm_id,
                exclude_source_types=['boav_bid', 'boav_offer']  # All B1610 data (api + csv)
            )
            # Deduplicate: if both 'api' and 'csv' records exist for the same
            # (identifier, period_start), prefer 'api' to avoid double-counting
            if raw_data:
                seen = {}
                for r in raw_data:
                    key = (r.identifier, r.period_start)
                    if key not in seen:
                        seen[key] = r
                    elif r.source_type == 'api':
                        seen[key] = r  # api takes priority over csv
                before_count = len(raw_data)
                raw_data = list(seen.values())
                if len(raw_data) < before_count:
                    logger.info(f"Deduplicated {before_count} → {len(raw_data)} B1610 records (api preferred over csv)")
        elif source == 'ENTSOE':
            raw_data = await self.fetch_raw_data(
                source, day_start, day_end,
                windfarm_id=windfarm_id,
                exclude_source_types=['api_consumption', 'excel_consumption']
            )
            # Deduplicate: if both 'api' and 'excel' records exist for the same
            # (identifier, period_start), prefer most recently updated record
            if raw_data:
                seen = {}
                for r in raw_data:
                    key = (r.identifier, r.period_start)
                    if key not in seen:
                        seen[key] = r
                    elif r.updated_at and seen[key].updated_at and r.updated_at > seen[key].updated_at:
                        seen[key] = r  # More recent update wins
                before_count = len(raw_data)
                raw_data = list(seen.values())
                if len(raw_data) < before_count:
                    logger.info(f"Deduplicated {before_count} → {len(raw_data)} ENTSOE records (latest update preferred)")
        else:
            raw_data = await self.fetch_raw_data(source, day_start, day_end, windfarm_id=windfarm_id)

        # For ELEXON, also fetch BOAV data for curtailment calculations
        boav_data = []
        if source == 'ELEXON':
            boav_data = await self.fetch_boav_data(day_start, day_end, windfarm_id=windfarm_id)
            if boav_data:
                logger.info(f"Found {len(boav_data)} BOAV bid records for curtailment calculation")
                self.stats['raw_records_processed'] += len(boav_data)

        # For ENTSOE, fetch consumption data separately
        consumption_data = []
        if source == 'ENTSOE':
            consumption_data = await self.fetch_raw_data(
                source, day_start, day_end,
                windfarm_id=windfarm_id,
                source_type=None,
                exclude_source_types=None,
            )
            # Filter to only consumption source_types
            consumption_data = [
                r for r in consumption_data
                if r.source_type in ('api_consumption', 'excel_consumption')
            ]
            if consumption_data:
                logger.info(f"Found {len(consumption_data)} ENTSOE consumption records")
                self.stats['raw_records_processed'] += len(consumption_data)

        if not raw_data and not boav_data:
            logger.info(f"No data found for {source} on {day_start.date()}")
            return {'processed': 0, 'raw_records': 0}

        if raw_data:
            logger.info(f"Found {len(raw_data)} raw records for {source}")
            self.stats['raw_records_processed'] += len(raw_data)

        # Transform to hourly records
        if source == 'ELEXON':
            hourly_records = self.transform_elexon(raw_data, boav_data, day_start, day_end)
        elif source == 'ENTSOE':
            hourly_records = self.transform_entsoe(raw_data, consumption_data)
        else:
            hourly_records = self.transform_source_data(source, raw_data)

        if not hourly_records:
            return {'processed': 0, 'raw_records': len(raw_data)}

        # Clear existing data for this day/source (idempotent)
        # Only clear data for the specific windfarm if filtering
        if not self.dry_run:
            await self.clear_existing_data(source, day_start, day_end, windfarm_id=windfarm_id)

        # Save hourly records
        saved_count = 0
        if not self.dry_run:
            saved_count = await self.save_hourly_records(hourly_records, source)

        self.stats['hourly_records_created'] += saved_count

        return {
            'raw_records': len(raw_data),
            'boav_records': len(boav_data) if source == 'ELEXON' else 0,
            'hourly_records': len(hourly_records),
            'saved': saved_count
        }

    async def fetch_raw_data(
        self,
        source: str,
        day_start: datetime,
        day_end: datetime,
        windfarm_id: Optional[int] = None,
        source_type: Optional[str] = None,
        exclude_source_types: Optional[List[str]] = None
    ) -> List[GenerationDataRaw]:
        """Fetch raw data for a specific source and day.

        Args:
            source: Data source (e.g., 'ELEXON', 'ENTSOE')
            day_start: Start of day (UTC)
            day_end: End of day (UTC)
            windfarm_id: Optional windfarm ID to filter data by its generation unit identifiers
            source_type: Optional source_type filter (e.g., 'api', 'boav_bid', 'boav_offer')
            exclude_source_types: Optional list of source_types to exclude
        """

        # Get identifiers for windfarm filtering if specified
        identifiers = None
        if windfarm_id:
            identifiers = await self._get_windfarm_identifiers(windfarm_id, source)
            if not identifiers:
                logger.warning(f"No generation units found for windfarm {windfarm_id} with source {source}")
                return []
            logger.debug(f"Filtering by identifiers: {identifiers}")

        # For monthly sources, we need to check if the month contains this day
        if source == 'ENERGISTYRELSEN':
            # Fetch monthly data that covers this day
            month_start = day_start.replace(day=1)
            month_end = (month_start + timedelta(days=32)).replace(day=1)

            logger.debug(f"Fetching {source} data for month {month_start.date()} to {month_end.date()}")
            query = select(GenerationDataRaw).where(
                and_(
                    GenerationDataRaw.source == source,
                    GenerationDataRaw.period_start >= month_start,
                    GenerationDataRaw.period_start < month_end
                )
            )
            if identifiers:
                query = query.where(GenerationDataRaw.identifier.in_(identifiers))
            if source_type:
                query = query.where(GenerationDataRaw.source_type == source_type)
            if exclude_source_types:
                query = query.where(GenerationDataRaw.source_type.notin_(exclude_source_types))
            result = await self.db.execute(query)
        else:
            # Fetch data for the specific day
            # For ELEXON, extend query window by 1 hour to capture BST offset records.
            # During BST, UK settlement periods 1-2 are at 23:00-00:00 UTC of the previous day,
            # but raw data has incorrect period_start timestamps (1 hour ahead).
            # This means settlement periods 1-2 for next UK day are stored at day_end 00:00-01:00 UTC.
            # The day boundary filter in transform_elexon will correctly include only records
            # that map to hours within the current UTC day.
            query_end = day_end
            if source == 'ELEXON':
                query_end = day_end + timedelta(hours=1)

            logger.debug(f"Fetching {source} data for {day_start} to {query_end}" +
                        (f" (source_type={source_type})" if source_type else ""))
            query = select(GenerationDataRaw).where(
                and_(
                    GenerationDataRaw.source == source,
                    GenerationDataRaw.period_start >= day_start,
                    GenerationDataRaw.period_start < query_end
                )
            )
            if identifiers:
                query = query.where(GenerationDataRaw.identifier.in_(identifiers))
            if source_type:
                query = query.where(GenerationDataRaw.source_type == source_type)
            if exclude_source_types:
                query = query.where(GenerationDataRaw.source_type.notin_(exclude_source_types))
            query = query.order_by(GenerationDataRaw.period_start, GenerationDataRaw.identifier)
            result = await self.db.execute(query)

        logger.debug(f"Query executed, fetching all records...")
        records = result.scalars().all()
        logger.debug(f"Fetched {len(records)} records")
        return records

    async def fetch_boav_data(
        self,
        day_start: datetime,
        day_end: datetime,
        windfarm_id: Optional[int] = None
    ) -> List[GenerationDataRaw]:
        """Fetch BOAV bid data (curtailment) for ELEXON.

        Args:
            day_start: Start of day (UTC)
            day_end: End of day (UTC)
            windfarm_id: Optional windfarm ID to filter data

        Returns:
            List of BOAV bid records
        """
        return await self.fetch_raw_data(
            source='ELEXON',
            day_start=day_start,
            day_end=day_end,
            windfarm_id=windfarm_id,
            source_type='boav_bid'
        )

    async def _get_windfarm_identifiers(self, windfarm_id: int, source: str) -> List[str]:
        """Get generation unit identifiers (codes) for a specific windfarm and source."""
        result = await self.db.execute(
            select(GenerationUnit.code)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationUnit.source == source
                )
            )
        )
        codes = result.scalars().all()
        return [code for code in codes if code]

    def transform_source_data(
        self,
        source: str,
        raw_data: List[GenerationDataRaw]
    ) -> List[HourlyRecord]:
        """Transform raw data based on source-specific rules.

        Note: ELEXON is handled separately in process_source_for_day
        to incorporate BOAV data for curtailment calculations.
        """

        if source == 'ENTSOE':
            return self.transform_entsoe(raw_data)
        elif source == 'ELEXON':
            # Should not reach here - ELEXON is handled separately
            return self.transform_elexon(raw_data, [])
        elif source == 'TAIPOWER':
            return self.transform_taipower(raw_data)
        elif source == 'NVE':
            return self.transform_nve(raw_data)
        elif source == 'ENERGISTYRELSEN':
            # For monthly data, we need the day boundaries
            return self.transform_energistyrelsen(raw_data)
        else:
            logger.warning(f"Unknown source: {source}")
            return []

    def transform_entsoe(
        self,
        raw_data: List[GenerationDataRaw],
        consumption_data: Optional[List[GenerationDataRaw]] = None
    ) -> List[HourlyRecord]:
        """Transform ENTSOE data (15-min or hourly) with optional consumption.

        Args:
            raw_data: Generation records (source_type='api' or 'excel', excluding consumption)
            consumption_data: Consumption records (source_type='api_consumption' or 'excel_consumption')
        """

        hourly_groups = defaultdict(list)

        for record in raw_data:
            # Ensure UTC timezone for PostgreSQL TIMESTAMP WITH TIME ZONE
            hour = record.period_start.replace(minute=0, second=0, microsecond=0)
            if hour.tzinfo is None:
                hour = hour.replace(tzinfo=ZoneInfo('UTC'))
            key = (hour, record.identifier)
            hourly_groups[key].append(record)

        # Group consumption data by (hour, identifier)
        consumption_groups = defaultdict(list)
        if consumption_data:
            for record in consumption_data:
                hour = record.period_start.replace(minute=0, second=0, microsecond=0)
                if hour.tzinfo is None:
                    hour = hour.replace(tzinfo=ZoneInfo('UTC'))
                key = (hour, record.identifier)
                consumption_groups[key].append(record)

        hourly_records = []

        for (hour, identifier), records in hourly_groups.items():
            # Filter out records with null or NaN values
            # (PostgreSQL NaN loads as Decimal('NaN') which is not None)
            valid_records = [
                r for r in records
                if r.value_extracted is not None and not Decimal(str(r.value_extracted)).is_nan()
            ]
            if not valid_records:
                continue  # Skip this hour if no valid data

            jsonb_resolution = valid_records[0].data.get('resolution_code', 'PT60M')

            # Heuristic: use record count to detect sub-hourly data regardless of metadata
            # This handles cases where entsoe-py didn't provide resolution_code and
            # existing data has incorrect 'PT60M' stored in JSONB
            if len(valid_records) >= 3:
                resolution = 'PT15M'
            elif len(valid_records) == 2:
                resolution = 'PT30M'
            else:
                resolution = jsonb_resolution

            if resolution == 'PT15M':
                # Average of sub-hourly values per hour (harmonization rule)
                # Use Decimal arithmetic to preserve precision (avoids float rounding)
                values = [Decimal(str(r.value_extracted)) for r in valid_records]
                generation_mw = float(sum(values) / len(values))
                expected_points = 4
            elif resolution == 'PT30M':
                # Average of 2 half-hourly values
                values = [Decimal(str(r.value_extracted)) for r in valid_records]
                generation_mw = float(sum(values) / len(values))
                expected_points = 2
            else:
                # Hourly data - use directly
                generation_mw = float(valid_records[0].value_extracted)
                expected_points = 1

            # Calculate consumption for this hour/identifier
            consumption_mw = None
            consumption_records = consumption_groups.get((hour, identifier), [])
            valid_consumption = [
                r for r in consumption_records
                if r.value_extracted is not None and not Decimal(str(r.value_extracted)).is_nan()
            ]
            if valid_consumption:
                if len(valid_consumption) >= 3:
                    consumption_mw = np.mean([float(r.value_extracted) for r in valid_consumption])
                elif len(valid_consumption) == 2:
                    consumption_mw = np.mean([float(r.value_extracted) for r in valid_consumption])
                else:
                    consumption_mw = float(valid_consumption[0].value_extracted)

            # Get raw capacity from ENTSOE data (store separately)
            raw_capacity_mw = None
            if valid_records[0].data and 'installed_capacity_mw' in valid_records[0].data:
                try:
                    raw_capacity_mw = float(valid_records[0].data['installed_capacity_mw'])
                except (TypeError, ValueError):
                    raw_capacity_mw = None

            # Always use generation units cache for capacity_mw field
            unit_key = f"ENTSOE:{identifier}"
            cache_entry = self.generation_units_cache.get(unit_key)
            unit_info = self.get_operational_unit(cache_entry, hour)

            # Get capacity if unit is operational
            capacity_mw = unit_info.get('capacity_mw') if unit_info else None

            all_raw_ids = [r.id for r in valid_records] + [r.id for r in valid_consumption]

            hourly_records.append(HourlyRecord(
                hour=hour,
                identifier=identifier,
                generation_mwh=generation_mw,  # MW for 1 hour = MWh
                capacity_mw=capacity_mw,  # From generation_units table (only if operational)
                raw_data_ids=all_raw_ids,
                data_points=len(valid_records),
                expected_points=expected_points,
                consumption_mwh=consumption_mw,
                metadata={
                    'resolution_code': resolution,
                    'raw_capacity_mw': raw_capacity_mw,  # Store raw value separately
                    'has_consumption': consumption_mw is not None,
                }
            ))

        # Handle consumption-only hours (no generation data for this hour/identifier)
        # These occur when a unit is only consuming (e.g., auxiliary load during zero wind)
        processed_keys = set(hourly_groups.keys())
        consumption_only_count = 0
        for (hour, identifier), consumption_records in consumption_groups.items():
            if (hour, identifier) in processed_keys:
                continue  # Already handled above with generation data

            valid_consumption = [
                r for r in consumption_records
                if r.value_extracted is not None and not Decimal(str(r.value_extracted)).is_nan()
            ]
            if not valid_consumption:
                continue

            # Average consumption values
            if len(valid_consumption) >= 2:
                consumption_mw = np.mean([float(r.value_extracted) for r in valid_consumption])
            else:
                consumption_mw = float(valid_consumption[0].value_extracted)

            unit_key = f"ENTSOE:{identifier}"
            cache_entry = self.generation_units_cache.get(unit_key)
            unit_info = self.get_operational_unit(cache_entry, hour)
            capacity_mw = unit_info.get('capacity_mw') if unit_info else None

            consumption_only_count += 1
            hourly_records.append(HourlyRecord(
                hour=hour,
                identifier=identifier,
                generation_mwh=0.0,  # No generation during consumption-only hours
                capacity_mw=capacity_mw,
                raw_data_ids=[r.id for r in valid_consumption],
                data_points=0,
                expected_points=0,
                consumption_mwh=consumption_mw,
                metadata={
                    'resolution_code': 'PT15M' if len(valid_consumption) >= 3 else 'PT60M',
                    'consumption_only': True,
                    'has_consumption': True,
                }
            ))

        if consumption_only_count > 0:
            logger.info(f"Created {consumption_only_count} consumption-only hourly records")

        return hourly_records

    def _calculate_correct_elexon_hour(self, record: GenerationDataRaw) -> datetime:
        """Calculate correct UTC hour from settlement_date + settlement_period.

        Handles UK DST correctly:
        - During BST (summer): UK midnight = 23:00 UTC previous day
        - During GMT (winter): UK midnight = 00:00 UTC same day

        Falls back to period_start if JSONB fields are missing or invalid.
        """
        UK_TZ = ZoneInfo('Europe/London')
        UTC_TZ = ZoneInfo('UTC')

        if record.data and 'settlement_date' in record.data and 'settlement_period' in record.data:
            settlement_date_raw = record.data['settlement_date']
            settlement_period_raw = record.data['settlement_period']

            # Handle None or 'None' string values - fall back to period_start
            if settlement_date_raw is None or settlement_date_raw == 'None' or \
               settlement_period_raw is None or settlement_period_raw == 'None':
                # Fall back to period_start - ensure UTC timezone
                period_start = record.period_start
                if period_start.tzinfo is None:
                    period_start = period_start.replace(tzinfo=ZoneInfo('UTC'))
                return period_start.replace(minute=0, second=0, microsecond=0)

            settlement_date = str(settlement_date_raw)
            try:
                settlement_period = int(settlement_period_raw)
            except (ValueError, TypeError):
                # Invalid settlement_period - fall back to period_start
                period_start = record.period_start
                if period_start.tzinfo is None:
                    period_start = period_start.replace(tzinfo=ZoneInfo('UTC'))
                return period_start.replace(minute=0, second=0, microsecond=0)

            # Parse settlement date (supports YYYYMMDD and ISO formats)
            if len(settlement_date) == 8:  # YYYYMMDD format
                year = int(settlement_date[:4])
                month = int(settlement_date[4:6])
                day = int(settlement_date[6:8])
            else:  # ISO format (YYYY-MM-DD...)
                parts = settlement_date[:10].split('-')
                year, month, day = int(parts[0]), int(parts[1]), int(parts[2])

            # Create UK midnight and convert to UTC
            uk_midnight = datetime(year, month, day, 0, 0, 0, tzinfo=UK_TZ)
            utc_midnight = uk_midnight.astimezone(UTC_TZ)

            # Add settlement period offset (SP 1 = 00:00-00:30, each SP is 30 min)
            utc_timestamp = utc_midnight + timedelta(minutes=(settlement_period - 1) * 30)

            # Floor to hour boundary - keep UTC tzinfo for PostgreSQL TIMESTAMP WITH TIME ZONE
            return utc_timestamp.replace(minute=0, second=0, microsecond=0)

        # Fallback to period_start (for data without JSONB fields)
        period_start = record.period_start
        if period_start.tzinfo is None:
            period_start = period_start.replace(tzinfo=ZoneInfo('UTC'))
        return period_start.replace(minute=0, second=0, microsecond=0)

    def _get_elexon_value_with_sign(self, record: GenerationDataRaw) -> float:
        """Get the value from ELEXON record with correct sign based on import_export_ind.

        Elexon uses import_export_ind to indicate direction:
        - 'I' = Import (consuming from grid) = negative generation
        - 'E' = Export (generating to grid) = positive generation

        Reads from JSONB metered_volume if available, falls back to value_extracted.
        """
        # Try to get metered_volume from JSONB (more reliable)
        if record.data and 'metered_volume' in record.data:
            value = float(record.data['metered_volume'])
            import_export_ind = record.data.get('import_export_ind', '')
        else:
            # Fallback to value_extracted (may already have sign applied or not)
            value = float(record.value_extracted) if record.value_extracted is not None else 0.0
            import_export_ind = record.data.get('import_export_ind', '') if record.data else ''

        # Apply sign based on import_export_ind
        if import_export_ind == 'I':
            value = -abs(value)  # Imports are negative
        elif import_export_ind == 'E':
            value = abs(value)  # Exports are positive

        return value

    def transform_elexon(
        self,
        raw_data: List[GenerationDataRaw],
        boav_data: List[GenerationDataRaw],
        day_start: Optional[datetime] = None,
        day_end: Optional[datetime] = None
    ) -> List[HourlyRecord]:
        """Transform ELEXON data (30-min periods) with BOAV curtailment integration.

        ELEXON provides data in 30-minute settlement periods.
        Each value represents MWh generated in that 30-min period.
        To get hourly MWh, we sum the two 30-min periods.

        BOAV Integration:
        - Accepted bids represent curtailment (generator paid to reduce output)
        - Actual Production = Metered Generation (B1610) + abs(Curtailed Volume)
        - generation_mwh = metered_mwh + curtailed_mwh

        Uses settlement_date and settlement_period from JSONB to calculate
        correct UTC timestamps, handling UK DST properly.

        Applies sign based on import_export_ind (I=Import=negative, E=Export=positive).

        Args:
            raw_data: B1610 metered volume records
            boav_data: BOAV bid records for curtailment
            day_start: Optional UTC day start - hours outside [day_start, day_end) are filtered
            day_end: Optional UTC day end
        """

        # Group B1610 data by hour and identifier
        hourly_groups = defaultdict(list)

        for record in raw_data:
            # Calculate correct UTC hour from settlement_date + settlement_period
            hour = self._calculate_correct_elexon_hour(record)
            key = (hour, record.identifier)
            hourly_groups[key].append(record)

        # Group BOAV bid data by hour and identifier
        boav_hourly_groups = defaultdict(list)
        for record in boav_data:
            hour = self._calculate_correct_elexon_hour(record)
            key = (hour, record.identifier)
            boav_hourly_groups[key].append(record)

        hourly_records = []

        for (hour, identifier), records in hourly_groups.items():
            # Filter out records with null values (check JSONB metered_volume first)
            valid_records = []
            for r in records:
                has_value = (r.data and 'metered_volume' in r.data and r.data['metered_volume'] is not None) or r.value_extracted is not None
                if has_value:
                    valid_records.append(r)
            if not valid_records:
                continue  # Skip this hour if no valid data

            # Sum the MWh values from the 30-min periods to get hourly total (metered generation)
            # Apply correct sign based on import_export_ind
            metered_mwh = sum([self._get_elexon_value_with_sign(r) for r in valid_records])

            # Get BOAV bid data for this hour/identifier (curtailment)
            boav_records = boav_hourly_groups.get((hour, identifier), [])
            curtailed_mwh = 0.0
            boav_raw_ids = []

            if boav_records:
                # Sum of absolute bid volumes (bids are stored as negative values)
                # Curtailment = abs(sum of bid volumes)
                for boav_record in boav_records:
                    # value_extracted contains the bid volume (negative for curtailment)
                    if boav_record.value_extracted is not None:
                        # Take absolute value since bids are negative
                        curtailed_mwh += abs(float(boav_record.value_extracted))
                        boav_raw_ids.append(boav_record.id)

            # Actual production = metered + curtailed
            generation_mwh = metered_mwh + curtailed_mwh

            # Get capacity from generation units cache
            unit_key = f"ELEXON:{identifier}"
            cache_entry = self.generation_units_cache.get(unit_key)
            unit_info = self.get_operational_unit(cache_entry, hour)

            # Get capacity if unit is operational
            capacity_mw = unit_info.get('capacity_mw') if unit_info else None

            # Combine raw data IDs from both B1610 and BOAV
            all_raw_ids = [r.id for r in valid_records] + boav_raw_ids

            hourly_records.append(HourlyRecord(
                hour=hour,
                identifier=identifier,
                generation_mwh=generation_mwh,  # Actual production (metered + curtailed)
                capacity_mw=capacity_mw,
                raw_data_ids=all_raw_ids,
                data_points=len(valid_records),
                expected_points=2,
                metadata={
                    'has_curtailment': curtailed_mwh > 0,
                    'boav_records_count': len(boav_records)
                },
                metered_mwh=metered_mwh,  # What was delivered to grid
                curtailed_mwh=curtailed_mwh if curtailed_mwh > 0 else None  # What was curtailed
            ))

        # Process BOAV-only hours (curtailment without corresponding B1610 metered data)
        # This captures periods when windfarms were fully curtailed (no metered output)
        b1610_hours = set(hourly_groups.keys())
        boav_only_hours = set(boav_hourly_groups.keys()) - b1610_hours

        for (hour, identifier) in boav_only_hours:
            boav_records = boav_hourly_groups[(hour, identifier)]

            # Sum of absolute bid volumes (bids are stored as negative values)
            curtailed_mwh = 0.0
            boav_raw_ids = []

            for boav_record in boav_records:
                if boav_record.value_extracted is not None:
                    curtailed_mwh += abs(float(boav_record.value_extracted))
                    boav_raw_ids.append(boav_record.id)

            # Skip if no valid curtailment data
            if curtailed_mwh == 0:
                continue

            # Get capacity from generation units cache
            unit_key = f"ELEXON:{identifier}"
            cache_entry = self.generation_units_cache.get(unit_key)
            unit_info = self.get_operational_unit(cache_entry, hour)
            capacity_mw = unit_info.get('capacity_mw') if unit_info else None

            # For BOAV-only hours: metered=0, generation=curtailed
            hourly_records.append(HourlyRecord(
                hour=hour,
                identifier=identifier,
                generation_mwh=curtailed_mwh,  # Actual production = curtailed (metered is 0)
                capacity_mw=capacity_mw,
                raw_data_ids=boav_raw_ids,
                data_points=0,  # No B1610 data points
                expected_points=2,
                metadata={
                    'has_curtailment': True,
                    'boav_records_count': len(boav_records),
                    'boav_only': True  # Flag to indicate this is a BOAV-only record
                },
                metered_mwh=0.0,  # No metered output (fully curtailed)
                curtailed_mwh=curtailed_mwh
            ))

        # Filter hourly records to only include those within the day boundaries.
        # This is needed because raw data may have incorrect period_start timestamps
        # (1-hour BST offset bug), causing records to appear in the wrong day's batch.
        # The _calculate_correct_elexon_hour function corrects the hour using
        # settlement_date + settlement_period from JSONB, but the raw data query
        # still uses period_start, so some records may produce hours outside the
        # expected day range. These will be processed when the correct day is processed.
        if day_start is not None and day_end is not None:
            original_count = len(hourly_records)
            hourly_records = [
                r for r in hourly_records
                if r.hour >= day_start and r.hour < day_end
            ]
            filtered_count = original_count - len(hourly_records)
            if filtered_count > 0:
                logger.debug(
                    f"Filtered {filtered_count} hourly records outside day boundaries "
                    f"[{day_start}, {day_end})"
                )

        return hourly_records

    def transform_taipower(self, raw_data: List[GenerationDataRaw]) -> List[HourlyRecord]:
        """Transform TAIPOWER data (hourly, UTC+8 timezone)."""

        hourly_records = []

        for record in raw_data:
            # Skip records with no generation value
            if record.value_extracted is None:
                logger.warning(f"Skipping TAIPOWER record {record.id} with null generation value")
                continue

            # Note: Check if timezone conversion is needed
            # If data was imported in UTC+8, convert to UTC
            hour = record.period_start.replace(minute=0, second=0, microsecond=0)
            if hour.tzinfo is None:
                hour = hour.replace(tzinfo=ZoneInfo('UTC'))

            # Get raw capacity from TAIPOWER data (store separately)
            raw_capacity_mw = None
            if record.data and 'installed_capacity_mw' in record.data:
                try:
                    raw_capacity_mw = float(record.data['installed_capacity_mw'])
                except (TypeError, ValueError):
                    raw_capacity_mw = None

            # Always use generation units cache for capacity_mw field
            unit_key = f"TAIPOWER:{record.identifier}"
            cache_entry = self.generation_units_cache.get(unit_key)
            unit_info = self.get_operational_unit(cache_entry, hour)

            # Get capacity if unit is operational
            capacity_mw = unit_info.get('capacity_mw') if unit_info else None

            hourly_records.append(HourlyRecord(
                hour=hour,
                identifier=record.identifier,
                generation_mwh=float(record.value_extracted),
                capacity_mw=capacity_mw,  # From generation_units table (only if operational)
                raw_data_ids=[record.id],
                data_points=1,
                expected_points=1,
                metadata={
                    'capacity_factor': record.data.get('capacity_factor') if record.data else None,  # Raw CF
                    'raw_capacity_mw': raw_capacity_mw  # Raw capacity
                }
            ))

        return hourly_records

    def transform_nve(self, raw_data: List[GenerationDataRaw]) -> List[HourlyRecord]:
        """Transform NVE data (hourly, already in MWh)."""

        # Group records by unit and hour (aggregate multiple records for same hour)
        hourly_data = {}

        for record in raw_data:
            # Skip records with null values
            if record.value_extracted is None:
                continue

            hour = record.period_start.replace(minute=0, second=0, microsecond=0)
            if hour.tzinfo is None:
                hour = hour.replace(tzinfo=ZoneInfo('UTC'))
            key = (record.identifier, hour)

            if key not in hourly_data:
                hourly_data[key] = {
                    'generation_mwh': 0,
                    'raw_data_ids': [],
                    'data_points': 0
                }

            # Sum generation values for the same unit/hour
            hourly_data[key]['generation_mwh'] += float(record.value_extracted)
            hourly_data[key]['raw_data_ids'].append(record.id)
            hourly_data[key]['data_points'] += 1

        # Create hourly records from aggregated data
        hourly_records = []
        for (identifier, hour), data in hourly_data.items():
            # Get capacity from generation units cache
            # identifier is the code (e.g., "20" for Bessakerfjellet)
            unit_key = f"NVE:{identifier}"
            cache_entry = self.generation_units_cache.get(unit_key)

            # Get the operational unit (handles both single units and multiple phases)
            unit_info = self.get_operational_unit(cache_entry, hour)

            if unit_info:
                capacity_mw = unit_info.get('capacity_mw')
            else:
                capacity_mw = None  # No operational unit found for this timestamp

            hourly_records.append(HourlyRecord(
                hour=hour,
                identifier=identifier,
                generation_mwh=data['generation_mwh'],
                capacity_mw=capacity_mw,
                raw_data_ids=data['raw_data_ids'],
                data_points=data['data_points'],
                expected_points=1,  # NVE should have 1 aggregated value per hour
                metadata={}
            ))

        return hourly_records

    def transform_energistyrelsen(
        self,
        raw_data: List[GenerationDataRaw]
    ) -> List[HourlyRecord]:
        """Transform ENERGISTYRELSEN data (monthly totals)."""

        # This is complex for daily processing - skip for now
        # Monthly data should be processed separately
        logger.warning("ENERGISTYRELSEN monthly data skipped in daily processing")
        return []

    async def clear_existing_data(
        self,
        source: str,
        day_start: datetime,
        day_end: datetime,
        windfarm_id: Optional[int] = None
    ):
        """Clear existing data for re-processing (idempotent).

        Args:
            source: Data source
            day_start: Start of day (UTC)
            day_end: End of day (UTC)
            windfarm_id: Optional windfarm ID to limit deletion to specific windfarm
        """

        # Note: We do NOT extend the clear window for ELEXON BST handling.
        # During BST, UK settlement dates start at 23:00 UTC (previous day),
        # but those records are created when processing the PREVIOUS UTC day.
        # Extending the window would delete records from the previous day's processing.
        clear_start = day_start

        # Build delete query
        conditions = [
            GenerationData.source == source,
            GenerationData.hour >= clear_start,
            GenerationData.hour < day_end
        ]

        # Add windfarm filter if specified
        if windfarm_id:
            conditions.append(GenerationData.windfarm_id == windfarm_id)

        result = await self.db.execute(
            delete(GenerationData)
            .where(and_(*conditions))
            .returning(GenerationData.id)
        )

        deleted_count = len(result.all())
        if deleted_count > 0:
            logger.info(f"Cleared {deleted_count} existing records for {source}" + (f" (windfarm_id={windfarm_id})" if windfarm_id else ""))

    async def save_hourly_records(
        self,
        hourly_records: List[HourlyRecord],
        source: str
    ) -> int:
        """Save hourly records to database."""

        generation_data_objects = []

        for record in hourly_records:
            # Calculate quality metrics
            completeness = min(1.0, record.data_points / record.expected_points) if record.expected_points > 0 else 0.0
            quality_score = self.calculate_quality_score(
                record.data_points,
                record.expected_points
            )

            # Store raw capacity from source data
            raw_capacity_mw = record.metadata.get('raw_capacity_mw') if record.metadata else None
            raw_capacity_factor = None

            # Check for raw capacity factor from source (e.g., TAIPOWER)
            if record.metadata and record.metadata.get('capacity_factor'):
                raw_cf_value = float(record.metadata['capacity_factor'])
                raw_capacity_factor = min(raw_cf_value, 9.9999)
            # Calculate raw capacity factor for ENTSOE if we have raw capacity
            elif raw_capacity_mw and raw_capacity_mw > 0:
                # Calculate raw CF using raw capacity from ENTSOE data
                raw_cf_value = record.generation_mwh / raw_capacity_mw
                raw_capacity_factor = min(raw_cf_value, 9.9999)

            # Get unit info (handles both single units and multiple phases)
            # Moved earlier so we can check commercial_operational_date for capacity factor
            unit_key = f"{source}:{record.identifier}"
            cache_entry = self.generation_units_cache.get(unit_key)
            unit_info = self.get_operational_unit(cache_entry, record.hour)

            if not unit_info:
                logger.warning(
                    f"Unit lookup failed for {unit_key} at {record.hour} "
                    f"— record will have NULL generation_unit_id and windfarm_id"
                )

            # Check if we're before commercial operational date
            # If so, don't calculate capacity factor (pre-commercial/commissioning data)
            is_pre_commercial = False
            commercial_date = unit_info.get('commercial_operational_date') if unit_info else None
            if commercial_date:
                # Convert record.hour to date for comparison
                record_date = record.hour.date() if hasattr(record.hour, 'date') else record.hour
                if isinstance(commercial_date, datetime):
                    commercial_date = commercial_date.date()
                if record_date < commercial_date:
                    is_pre_commercial = True

            # Calculate capacity factor from generation_units capacity
            # Only calculate if we're at or after commercial operational date
            capacity_factor = None
            effective_capacity_mw = record.capacity_mw
            if is_pre_commercial:
                # Pre-commercial period: don't show capacity factor or capacity
                effective_capacity_mw = None
            elif record.capacity_mw and record.capacity_mw > 0:
                # Calculate capacity factor using generation_units capacity
                calculated_cf = record.generation_mwh / record.capacity_mw
                # Cap at 9.9999 to fit in NUMERIC(5,4) - values > 1.0 can occur
                # when actual generation exceeds nameplate capacity
                capacity_factor = min(calculated_cf, 9.9999)

            # Create GenerationData object
            obj = GenerationData(
                id=str(uuid4()),
                hour=record.hour,
                generation_unit_id=unit_info['id'] if unit_info else None,
                windfarm_id=unit_info['windfarm_id'] if unit_info else None,
                turbine_unit_id=None,  # Will be set when we have turbine-level data
                generation_mwh=Decimal(str(record.generation_mwh)).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP),
                capacity_mw=Decimal(str(effective_capacity_mw)) if effective_capacity_mw else None,
                capacity_factor=Decimal(str(capacity_factor)) if capacity_factor else None,
                raw_capacity_mw=Decimal(str(raw_capacity_mw)) if raw_capacity_mw else None,
                raw_capacity_factor=Decimal(str(raw_capacity_factor)) if raw_capacity_factor else None,
                # Consumption tracking (ENTSOE)
                consumption_mwh=Decimal(str(record.consumption_mwh)).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP) if record.consumption_mwh is not None else None,
                # Curtailment tracking (ELEXON BOAV integration)
                metered_mwh=Decimal(str(record.metered_mwh)) if record.metered_mwh is not None else None,
                curtailed_mwh=Decimal(str(record.curtailed_mwh)) if record.curtailed_mwh else None,
                source=source,
                source_resolution=self.get_source_resolution(source),
                raw_data_ids=record.raw_data_ids,
                quality_flag=self.get_quality_flag(quality_score),
                quality_score=Decimal(str(quality_score)),
                completeness=Decimal(str(completeness)),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )

            generation_data_objects.append(obj)

        # Bulk insert
        if generation_data_objects:
            self.db.add_all(generation_data_objects)
            await self.db.flush()
            logger.info(f"Saved {len(generation_data_objects)} hourly records for {source}")

        return len(generation_data_objects)

    @staticmethod
    def calculate_quality_score(data_points: int, expected_points: int) -> float:
        """Calculate quality score based on completeness."""

        if expected_points == 0:
            return 0.0

        ratio = data_points / expected_points

        if ratio >= 1.0:
            return 1.0
        elif ratio >= 0.8:
            return 0.8
        elif ratio >= 0.5:
            return 0.5
        else:
            return ratio

    @staticmethod
    def get_quality_flag(quality_score: float) -> str:
        """Get quality flag based on score."""

        if quality_score >= 0.9:
            return 'HIGH'
        elif quality_score >= 0.7:
            return 'MEDIUM'
        elif quality_score >= 0.5:
            return 'LOW'
        else:
            return 'POOR'

    @staticmethod
    def get_source_resolution(source: str) -> str:
        """Get standard resolution for each source."""

        return {
            'ENTSOE': 'PT60M',
            'ELEXON': 'PT30M',
            'TAIPOWER': 'PT60M',
            'NVE': 'PT60M',
            'ENERGISTYRELSEN': 'P1M'
        }.get(source, 'PT60M')


async def check_data_availability(date: datetime):
    """Check what raw data is available for a specific date."""

    session_factory = get_session_factory()

    day_start = date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    async with session_factory() as db:
        result = await db.execute(
            select(
                GenerationDataRaw.source,
                func.count(GenerationDataRaw.id).label('count'),
                func.count(func.distinct(GenerationDataRaw.identifier)).label('units'),
                func.min(GenerationDataRaw.period_start).label('min_time'),
                func.max(GenerationDataRaw.period_end).label('max_time')
            )
            .where(
                and_(
                    GenerationDataRaw.period_start >= day_start,
                    GenerationDataRaw.period_start < day_end
                )
            )
            .group_by(GenerationDataRaw.source)
        )

        data = result.all()

        print(f"\nData availability for {date.date()}:")
        print("-" * 60)

        if not data:
            print("No data available")
            return

        for row in data:
            print(f"{row.source:20} {row.count:8,} records ({row.units:3} units)")
            print(f"{'':20} {row.min_time.strftime('%H:%M')} - {row.max_time.strftime('%H:%M')}")


async def main():
    """Main entry point."""

    parser = argparse.ArgumentParser(
        description='Process generation data for a specific day'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Date to process (YYYY-MM-DD). Default: yesterday'
    )
    parser.add_argument(
        '--source',
        type=str,
        choices=['ENTSOE', 'ELEXON', 'TAIPOWER', 'NVE', 'ENERGISTYRELSEN'],
        help='Process only specific source'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check data availability only'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without making database changes'
    )

    args = parser.parse_args()

    # Determine date to process
    if args.date:
        try:
            process_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print(f"Invalid date format: {args.date}")
            sys.exit(1)
    else:
        # Default to yesterday (typical for daily cron jobs)
        process_date = datetime.now() - timedelta(days=1)

    # Ensure UTC timezone
    process_date = process_date.replace(tzinfo=timezone.utc)

    if args.check:
        # Check data availability only
        await check_data_availability(process_date)
        return

    # Process data
    session_factory = get_session_factory()

    async with session_factory() as db:
        processor = DailyGenerationProcessor(db, dry_run=args.dry_run)

        sources = [args.source] if args.source else None

        try:
            result = await processor.process_day(process_date, sources)

            # Print summary
            print(f"\nProcessing Summary for {process_date.date()}")
            print("=" * 60)

            for source, source_result in result['sources'].items():
                if 'error' in source_result:
                    print(f"{source:20} ERROR: {source_result['error']}")
                else:
                    print(f"{source:20} {source_result.get('raw_records', 0):8,} raw → "
                          f"{source_result.get('hourly_records', 0):4} hourly")

            print("-" * 60)
            stats = result['stats']
            print(f"Total raw records:    {stats['raw_records_processed']:8,}")
            print(f"Hourly records saved: {stats['hourly_records_created']:8,}")
            print(f"Errors:               {stats['errors']:8,}")

            if args.dry_run:
                print("\nDRY RUN - No changes made to database")

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())