"""
Unified ELEXON Data Processor

Handles:
1. Raw B1610 data from CSV files and API
2. BOAV bid/offer data for curtailment
3. Aggregation into hourly generation_data records

Formula: generation_mwh = metered_mwh + curtailed_mwh

Usage:
    # Process specific windfarms for a date range
    poetry run python scripts/seeds/elexon_processor.py --start 2021-01-01 --end 2021-12-31 --windfarm-ids 7248,7251,7412

    # Process all ELEXON windfarms
    poetry run python scripts/seeds/elexon_processor.py --start 2021-01-01 --end 2021-12-31

    # Verify data for specific windfarms
    poetry run python scripts/seeds/elexon_processor.py --verify --windfarm-ids 7248,7251,7412

    # Debug specific date/windfarm
    poetry run python scripts/seeds/elexon_processor.py --debug --date 2021-06-10 --windfarm-id 7251
"""

import asyncio
import argparse
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Dict, Optional, Any, Tuple
from collections import defaultdict
from dataclasses import dataclass
from uuid import uuid4
from zoneinfo import ZoneInfo
import logging

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, and_, delete, text, func

from app.core.config import get_settings
from app.models.generation_data import GenerationDataRaw, GenerationData
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

UK_TZ = ZoneInfo('Europe/London')
UTC_TZ = ZoneInfo('UTC')


@dataclass
class HourlyRecord:
    """Aggregated hourly record."""
    hour: datetime  # UTC
    identifier: str
    generation_unit_id: Optional[int]
    windfarm_id: Optional[int]
    metered_mwh: float
    curtailed_mwh: float
    generation_mwh: float  # metered + curtailed
    capacity_mw: Optional[float]
    raw_data_ids: List[int]
    data_points: int


class ElexonProcessor:
    """Process ELEXON generation and curtailment data."""

    def __init__(self, session: AsyncSession):
        self.db = session
        self.generation_units_cache: Dict[str, Dict] = {}
        self.stats = {
            'days_processed': 0,
            'hours_created': 0,
            'hours_with_curtailment': 0,
            'errors': 0
        }

    async def load_generation_units(self, windfarm_ids: Optional[List[int]] = None):
        """Load ELEXON generation units into cache."""
        query = (
            select(GenerationUnit, Windfarm)
            .outerjoin(Windfarm, GenerationUnit.windfarm_id == Windfarm.id)
            .where(GenerationUnit.source == 'ELEXON')
        )

        if windfarm_ids:
            query = query.where(GenerationUnit.windfarm_id.in_(windfarm_ids))

        result = await self.db.execute(query)
        rows = result.all()

        for unit, windfarm in rows:
            self.generation_units_cache[unit.code] = {
                'id': unit.id,
                'windfarm_id': unit.windfarm_id,
                'capacity_mw': float(unit.capacity_mw) if unit.capacity_mw else None,
                'name': unit.name,
                'start_date': unit.start_date,
                'end_date': unit.end_date,
                'commercial_date': windfarm.commercial_operational_date if windfarm else None
            }

        logger.info(f"Loaded {len(self.generation_units_cache)} ELEXON generation units")

    def calculate_utc_hour(self, settlement_date_str: str, settlement_period: int) -> datetime:
        """
        Calculate UTC hour from UK settlement date and period.

        Settlement periods are in UK local time:
        - SP 1 = 00:00-00:30 UK
        - SP 2 = 00:30-01:00 UK
        - etc.

        During BST (summer), UK midnight = 23:00 UTC previous day.
        During GMT (winter), UK midnight = 00:00 UTC same day.
        """
        # Parse settlement date
        if len(settlement_date_str) == 8:  # YYYYMMDD
            year = int(settlement_date_str[:4])
            month = int(settlement_date_str[4:6])
            day = int(settlement_date_str[6:8])
        else:  # ISO format
            parts = settlement_date_str[:10].split('-')
            year, month, day = int(parts[0]), int(parts[1]), int(parts[2])

        # UK midnight for this settlement date
        uk_midnight = datetime(year, month, day, 0, 0, 0, tzinfo=UK_TZ)

        # Convert to UTC
        utc_midnight = uk_midnight.astimezone(UTC_TZ)

        # Add settlement period offset (SP 1 starts at 00:00)
        utc_timestamp = utc_midnight + timedelta(minutes=(settlement_period - 1) * 30)

        # Floor to hour
        return utc_timestamp.replace(minute=0, second=0, microsecond=0)

    async def get_windfarm_codes(self, windfarm_ids: List[int]) -> List[str]:
        """Get generation unit codes for specified windfarms."""
        result = await self.db.execute(
            select(GenerationUnit.code)
            .where(
                and_(
                    GenerationUnit.windfarm_id.in_(windfarm_ids),
                    GenerationUnit.source == 'ELEXON'
                )
            )
        )
        return [row[0] for row in result.all() if row[0]]

    async def fetch_raw_b1610_data(
        self,
        day_start: datetime,
        day_end: datetime,
        identifiers: Optional[List[str]] = None
    ) -> List[GenerationDataRaw]:
        """
        Fetch B1610 raw data (both API and CSV sources).
        Deduplicates: prefers API over CSV for same timestamp.

        IMPORTANT: Extends fetch window 1 hour earlier to capture BST boundary.
        During BST, 23:00 UTC = 00:00 UK = SP 1-2 of NEXT UK settlement day.
        So raw data with period_start 23:00 UTC on day X has settlement_date of day X+1.
        We need to fetch this data to properly aggregate day X+1.
        """
        # Extend 1 hour earlier to capture BST boundary records
        fetch_start = day_start - timedelta(hours=1)

        query = (
            select(GenerationDataRaw)
            .where(
                and_(
                    GenerationDataRaw.source == 'ELEXON',
                    GenerationDataRaw.period_start >= fetch_start,
                    GenerationDataRaw.period_start < day_end,
                    GenerationDataRaw.source_type.notin_(['boav_bid', 'boav_offer'])
                )
            )
        )

        if identifiers:
            query = query.where(GenerationDataRaw.identifier.in_(identifiers))

        result = await self.db.execute(query)
        records = result.scalars().all()

        # Deduplicate: prefer API over CSV
        seen = {}
        for r in records:
            key = (r.identifier, r.period_start)
            if key not in seen:
                seen[key] = r
            elif r.source_type == 'api':
                seen[key] = r

        return list(seen.values())

    async def fetch_boav_data(
        self,
        day_start: datetime,
        day_end: datetime,
        identifiers: Optional[List[str]] = None
    ) -> List[GenerationDataRaw]:
        """Fetch BOAV bid data for curtailment.

        Extends fetch window 1 hour earlier for BST boundary (same as B1610).
        """
        # Extend 1 hour earlier to capture BST boundary records
        fetch_start = day_start - timedelta(hours=1)

        query = (
            select(GenerationDataRaw)
            .where(
                and_(
                    GenerationDataRaw.source == 'ELEXON',
                    GenerationDataRaw.period_start >= fetch_start,
                    GenerationDataRaw.period_start < day_end,
                    GenerationDataRaw.source_type == 'boav_bid'
                )
            )
        )

        if identifiers:
            query = query.where(GenerationDataRaw.identifier.in_(identifiers))

        result = await self.db.execute(query)
        return result.scalars().all()

    def get_record_value(self, record: GenerationDataRaw) -> float:
        """Get value from record with correct sign based on import_export_ind."""
        # Try JSONB metered_volume first
        if record.data and 'metered_volume' in record.data:
            value = float(record.data['metered_volume'])
            import_export_ind = record.data.get('import_export_ind', '')
        else:
            value = float(record.value_extracted) if record.value_extracted is not None else 0.0
            import_export_ind = record.data.get('import_export_ind', '') if record.data else ''

        # Apply sign
        if import_export_ind == 'I':
            value = -abs(value)
        elif import_export_ind == 'E':
            value = abs(value)

        return value

    def get_record_hour(self, record: GenerationDataRaw) -> datetime:
        """
        Get UTC hour for a record.

        IMPORTANT: Use period_start directly, NOT settlement_date+SP calculation.

        The CSV import stored timestamps as UK local times in a UTC column
        (without converting from UK to UTC). This means during BST, all
        period_start values are 1 hour ahead of the correct UTC time.

        However, the aggregated data should match what's in the raw data
        (period_start), not the recalculated time. This ensures consistency
        when querying by hour.

        If we need to fix this properly, we should fix the raw data import
        to correctly convert UK local times to UTC.
        """
        hour = record.period_start.replace(minute=0, second=0, microsecond=0)
        if hour.tzinfo is None:
            hour = hour.replace(tzinfo=UTC_TZ)
        return hour

    def aggregate_to_hourly(
        self,
        b1610_records: List[GenerationDataRaw],
        boav_records: List[GenerationDataRaw]
    ) -> List[HourlyRecord]:
        """Aggregate raw records into hourly records."""

        # Group B1610 by (hour, identifier)
        b1610_groups = defaultdict(list)
        for r in b1610_records:
            hour = self.get_record_hour(r)
            key = (hour, r.identifier)
            b1610_groups[key].append(r)

        # Group BOAV by (hour, identifier)
        boav_groups = defaultdict(list)
        for r in boav_records:
            hour = self.get_record_hour(r)
            key = (hour, r.identifier)
            boav_groups[key].append(r)

        hourly_records = []

        # Process B1610 hours
        for (hour, identifier), records in b1610_groups.items():
            # Filter valid records
            valid = [r for r in records if self.get_record_value(r) is not None or r.value_extracted is not None]
            if not valid:
                continue

            # Sum metered values
            metered_mwh = sum(self.get_record_value(r) for r in valid)

            # Get curtailment for this hour
            boav = boav_groups.get((hour, identifier), [])
            curtailed_mwh = sum(abs(float(r.value_extracted)) for r in boav if r.value_extracted is not None)

            # Get unit info
            unit_info = self.generation_units_cache.get(identifier, {})

            hourly_records.append(HourlyRecord(
                hour=hour,
                identifier=identifier,
                generation_unit_id=unit_info.get('id'),
                windfarm_id=unit_info.get('windfarm_id'),
                metered_mwh=metered_mwh,
                curtailed_mwh=curtailed_mwh,
                generation_mwh=metered_mwh + curtailed_mwh,
                capacity_mw=unit_info.get('capacity_mw'),
                raw_data_ids=[r.id for r in valid] + [r.id for r in boav],
                data_points=len(valid)
            ))

        # Process BOAV-only hours (fully curtailed)
        b1610_keys = set(b1610_groups.keys())
        for (hour, identifier), boav in boav_groups.items():
            if (hour, identifier) in b1610_keys:
                continue  # Already processed

            curtailed_mwh = sum(abs(float(r.value_extracted)) for r in boav if r.value_extracted is not None)
            if curtailed_mwh == 0:
                continue

            unit_info = self.generation_units_cache.get(identifier, {})

            hourly_records.append(HourlyRecord(
                hour=hour,
                identifier=identifier,
                generation_unit_id=unit_info.get('id'),
                windfarm_id=unit_info.get('windfarm_id'),
                metered_mwh=0.0,
                curtailed_mwh=curtailed_mwh,
                generation_mwh=curtailed_mwh,
                capacity_mw=unit_info.get('capacity_mw'),
                raw_data_ids=[r.id for r in boav],
                data_points=0
            ))

        return hourly_records

    async def clear_existing_data(
        self,
        day_start: datetime,
        day_end: datetime,
        generation_unit_ids: Optional[List[int]] = None
    ):
        """
        Clear existing aggregated data.

        IMPORTANT: Clear by generation_unit_id, NOT windfarm_id!
        This ensures we delete even records with NULL windfarm_id.
        """
        # Extend window 1 hour earlier for BST boundary
        clear_start = day_start - timedelta(hours=1)

        if generation_unit_ids:
            # Delete by generation_unit_id (more precise)
            result = await self.db.execute(
                delete(GenerationData)
                .where(
                    and_(
                        GenerationData.source == 'ELEXON',
                        GenerationData.hour >= clear_start,
                        GenerationData.hour < day_end,
                        GenerationData.generation_unit_id.in_(generation_unit_ids)
                    )
                )
                .returning(GenerationData.id)
            )
        else:
            # Delete all ELEXON data for the day
            result = await self.db.execute(
                delete(GenerationData)
                .where(
                    and_(
                        GenerationData.source == 'ELEXON',
                        GenerationData.hour >= clear_start,
                        GenerationData.hour < day_end
                    )
                )
                .returning(GenerationData.id)
            )

        deleted = len(result.all())
        if deleted > 0:
            logger.debug(f"Cleared {deleted} existing records")

    async def save_hourly_records(self, records: List[HourlyRecord]) -> int:
        """Save hourly records to database in batches for better performance."""
        if not records:
            return 0

        objects = []
        curtailment_count = 0

        for record in records:
            # Calculate capacity factor
            capacity_factor = None
            if record.capacity_mw and record.capacity_mw > 0:
                cf = record.generation_mwh / record.capacity_mw
                capacity_factor = min(cf, 9.9999)

            # Calculate quality
            completeness = min(1.0, record.data_points / 2)  # Expected 2 half-hours
            quality_score = 1.0 if record.data_points >= 2 else (0.5 if record.data_points == 1 else 0.0)

            obj = GenerationData(
                id=str(uuid4()),
                hour=record.hour,
                generation_unit_id=record.generation_unit_id,
                windfarm_id=record.windfarm_id,
                turbine_unit_id=None,
                generation_mwh=Decimal(str(record.generation_mwh)),
                capacity_mw=Decimal(str(record.capacity_mw)) if record.capacity_mw else None,
                capacity_factor=Decimal(str(capacity_factor)) if capacity_factor else None,
                metered_mwh=Decimal(str(record.metered_mwh)),
                curtailed_mwh=Decimal(str(record.curtailed_mwh)) if record.curtailed_mwh > 0 else None,
                source='ELEXON',
                source_resolution='PT30M',
                raw_data_ids=record.raw_data_ids,
                quality_flag='HIGH' if quality_score >= 0.9 else ('MEDIUM' if quality_score >= 0.5 else 'LOW'),
                quality_score=Decimal(str(quality_score)),
                completeness=Decimal(str(completeness)),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            objects.append(obj)
            if record.curtailed_mwh > 0:
                curtailment_count += 1

        # Batch insert all objects
        try:
            self.db.add_all(objects)
            await self.db.flush()
            self.stats['hours_with_curtailment'] += curtailment_count
            return len(objects)
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            await self.db.rollback()
            self.stats['errors'] += len(objects)
            return 0

    async def process_day(
        self,
        date: datetime,
        windfarm_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """Process a single day."""
        day_start = date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC_TZ)
        day_end = day_start + timedelta(days=1)

        # Get identifiers for filtering
        identifiers = None
        generation_unit_ids = None
        if windfarm_ids:
            identifiers = await self.get_windfarm_codes(windfarm_ids)
            if not identifiers:
                return {'error': 'No generation units found for windfarms'}
            # Get generation_unit_ids for clearing
            generation_unit_ids = [
                self.generation_units_cache[code]['id']
                for code in identifiers
                if code in self.generation_units_cache
            ]

        # Fetch raw data
        b1610_data = await self.fetch_raw_b1610_data(day_start, day_end, identifiers)
        boav_data = await self.fetch_boav_data(day_start, day_end, identifiers)

        if not b1610_data and not boav_data:
            return {'b1610': 0, 'boav': 0, 'hourly': 0}

        # Aggregate to hourly
        hourly_records = self.aggregate_to_hourly(b1610_data, boav_data)

        # Clear existing and save new
        await self.clear_existing_data(day_start, day_end, generation_unit_ids)
        saved = await self.save_hourly_records(hourly_records)

        self.stats['hours_created'] += saved
        self.stats['days_processed'] += 1

        return {
            'b1610': len(b1610_data),
            'boav': len(boav_data),
            'hourly': len(hourly_records),
            'saved': saved
        }

    async def process_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        windfarm_ids: Optional[List[int]] = None
    ):
        """Process a date range."""
        await self.load_generation_units(windfarm_ids)

        total_days = (end_date - start_date).days + 1
        current = start_date

        logger.info(f"Processing {total_days} days from {start_date.date()} to {end_date.date()}")
        if windfarm_ids:
            logger.info(f"Filtering by windfarm IDs: {windfarm_ids}")

        while current <= end_date:
            try:
                result = await self.process_day(current, windfarm_ids)
                await self.db.commit()

                # Log progress every day
                logger.info(f"Processed {current.date()} - {result.get('saved', 0)} hours saved ({self.stats['days_processed']}/{total_days} days)")

            except Exception as e:
                logger.error(f"Error processing {current.date()}: {e}")
                await self.db.rollback()
                self.stats['errors'] += 1

            current += timedelta(days=1)

        logger.info(f"Completed: {self.stats['days_processed']} days, {self.stats['hours_created']} hours, {self.stats['errors']} errors")


async def verify_windfarm(
    session: AsyncSession,
    windfarm_id: int,
    start_date: datetime,
    end_date: datetime
):
    """Verify data completeness for a windfarm."""

    # Get windfarm info
    result = await session.execute(
        select(Windfarm).where(Windfarm.id == windfarm_id)
    )
    windfarm = result.scalar_one_or_none()
    if not windfarm:
        print(f"Windfarm {windfarm_id} not found")
        return

    # Get generation unit codes
    result = await session.execute(
        select(GenerationUnit.code)
        .where(
            and_(
                GenerationUnit.windfarm_id == windfarm_id,
                GenerationUnit.source == 'ELEXON'
            )
        )
    )
    codes = [r[0] for r in result.all()]

    print(f"\n{'='*70}")
    print(f"VERIFYING: {windfarm.name} (id={windfarm_id})")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Generation units: {', '.join(codes)}")
    print(f"{'='*70}")

    total_days = (end_date - start_date).days + 1
    expected_hours = total_days * 24 * len(codes)

    # Count actual records
    result = await session.execute(
        text("""
            SELECT COUNT(*) as cnt
            FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gu.code = ANY(:codes)
            AND gd.hour >= :start AND gd.hour < :end
        """),
        {'codes': codes, 'start': start_date, 'end': end_date + timedelta(days=1)}
    )
    total_records = result.scalar()

    print(f"\n1. RECORD COUNT:")
    print(f"   Total records: {total_records:,}")
    print(f"   Expected (24h x {total_days} days x {len(codes)} units): {expected_hours:,}")
    print(f"   Missing: {expected_hours - total_records:,}")

    # Check days with missing hours
    result = await session.execute(
        text("""
            SELECT gd.hour::date as day, gu.code, COUNT(DISTINCT EXTRACT(HOUR FROM gd.hour)) as hours
            FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gu.code = ANY(:codes)
            AND gd.hour >= :start AND gd.hour < :end
            GROUP BY day, gu.code
            HAVING COUNT(DISTINCT EXTRACT(HOUR FROM gd.hour)) < 24
            ORDER BY day, gu.code
            LIMIT 30
        """),
        {'codes': codes, 'start': start_date, 'end': end_date + timedelta(days=1)}
    )
    missing_days = result.all()

    print(f"\n2. MISSING HOURS CHECK:")
    if missing_days:
        print(f"   Found {len(missing_days)} day/unit combinations with < 24 hours:")
        for row in missing_days[:15]:
            print(f"     {row[0]} | {row[1]}: {row[2]} hours (missing {24 - row[2]})")
    else:
        print(f"   All days have 24 hours")

    # Check formula consistency
    result = await session.execute(
        text("""
            SELECT COUNT(*) as cnt
            FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gu.code = ANY(:codes)
            AND gd.hour >= :start AND gd.hour < :end
            AND gd.metered_mwh IS NOT NULL
            AND ABS(gd.generation_mwh - (COALESCE(gd.metered_mwh, 0) + COALESCE(gd.curtailed_mwh, 0))) > 0.01
        """),
        {'codes': codes, 'start': start_date, 'end': end_date + timedelta(days=1)}
    )
    formula_errors = result.scalar()

    print(f"\n3. FORMULA CHECK (generation = metered + curtailed):")
    if formula_errors > 0:
        print(f"   ISSUES: {formula_errors} records fail formula check")
    else:
        print(f"   All records pass")

    # Curtailment stats
    result = await session.execute(
        text("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN curtailed_mwh > 0 THEN 1 ELSE 0 END) as with_curtailment,
                SUM(curtailed_mwh) as total_curtailed
            FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gu.code = ANY(:codes)
            AND gd.hour >= :start AND gd.hour < :end
        """),
        {'codes': codes, 'start': start_date, 'end': end_date + timedelta(days=1)}
    )
    row = result.one()

    print(f"\n4. CURTAILMENT STATS:")
    print(f"   Hours with curtailment > 0: {row[1] or 0:,}")
    print(f"   Total curtailed: {float(row[2] or 0):,.2f} MWh")


async def debug_hour(
    session: AsyncSession,
    windfarm_id: int,
    target_date: datetime,
    target_hour: int
):
    """Debug a specific hour."""

    # Get generation unit codes
    result = await session.execute(
        select(GenerationUnit.code, GenerationUnit.id)
        .where(
            and_(
                GenerationUnit.windfarm_id == windfarm_id,
                GenerationUnit.source == 'ELEXON'
            )
        )
    )
    units = result.all()
    codes = [u[0] for u in units]

    hour_start = target_date.replace(hour=target_hour, minute=0, second=0, microsecond=0, tzinfo=UTC_TZ)
    hour_end = hour_start + timedelta(hours=1)

    print(f"\n{'='*70}")
    print(f"DEBUG: {hour_start}")
    print(f"Units: {codes}")
    print(f"{'='*70}")

    # Check raw B1610 data
    print(f"\n1. RAW B1610 DATA:")
    result = await session.execute(
        select(GenerationDataRaw)
        .where(
            and_(
                GenerationDataRaw.source == 'ELEXON',
                GenerationDataRaw.identifier.in_(codes),
                GenerationDataRaw.period_start >= hour_start,
                GenerationDataRaw.period_start < hour_end,
                GenerationDataRaw.source_type.notin_(['boav_bid', 'boav_offer'])
            )
        )
    )
    raw_records = result.scalars().all()

    if raw_records:
        for r in raw_records:
            sd = r.data.get('settlement_date') if r.data else None
            sp = r.data.get('settlement_period') if r.data else None
            print(f"   {r.identifier} | {r.period_start} | SD={sd} SP={sp} | {r.source_type} | val={r.value_extracted}")
    else:
        print("   No raw B1610 data found")

    # Check raw BOAV data
    print(f"\n2. RAW BOAV DATA:")
    result = await session.execute(
        select(GenerationDataRaw)
        .where(
            and_(
                GenerationDataRaw.source == 'ELEXON',
                GenerationDataRaw.identifier.in_(codes),
                GenerationDataRaw.period_start >= hour_start,
                GenerationDataRaw.period_start < hour_end,
                GenerationDataRaw.source_type == 'boav_bid'
            )
        )
    )
    boav_records = result.scalars().all()

    if boav_records:
        for r in boav_records:
            print(f"   {r.identifier} | {r.period_start} | val={r.value_extracted}")
    else:
        print("   No BOAV data found")

    # Check aggregated data
    print(f"\n3. AGGREGATED DATA:")
    result = await session.execute(
        select(GenerationData)
        .where(
            and_(
                GenerationData.source == 'ELEXON',
                GenerationData.generation_unit_id.in_([u[1] for u in units]),
                GenerationData.hour >= hour_start,
                GenerationData.hour < hour_end
            )
        )
    )
    agg_records = result.scalars().all()

    if agg_records:
        for r in agg_records:
            print(f"   {r.hour} | gen={r.generation_mwh} | metered={r.metered_mwh} | curtailed={r.curtailed_mwh}")
    else:
        print("   No aggregated data found - THIS IS THE PROBLEM!")


async def main():
    parser = argparse.ArgumentParser(description='ELEXON Data Processor')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--windfarm-ids', type=str, help='Comma-separated windfarm IDs')
    parser.add_argument('--verify', action='store_true', help='Verify mode')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    parser.add_argument('--date', type=str, help='Single date for debug (YYYY-MM-DD)')
    parser.add_argument('--windfarm-id', type=int, help='Single windfarm ID for debug')
    parser.add_argument('--hour', type=int, default=16, help='Hour for debug (0-23)')

    args = parser.parse_args()

    settings = get_settings()
    engine = create_async_engine(settings.database_url_async, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        if args.debug:
            if not args.date or not args.windfarm_id:
                print("Debug mode requires --date and --windfarm-id")
                return
            target_date = datetime.strptime(args.date, '%Y-%m-%d')
            await debug_hour(session, args.windfarm_id, target_date, args.hour)
            return

        if args.verify:
            if not args.windfarm_ids:
                print("Verify mode requires --windfarm-ids")
                return
            windfarm_ids = [int(x) for x in args.windfarm_ids.split(',')]
            start = datetime.strptime(args.start, '%Y-%m-%d') if args.start else datetime(2020, 1, 1)
            end = datetime.strptime(args.end, '%Y-%m-%d') if args.end else datetime(2024, 12, 31)
            for wf_id in windfarm_ids:
                await verify_windfarm(session, wf_id, start, end)
            return

        # Process mode
        if not args.start or not args.end:
            print("Process mode requires --start and --end dates")
            return

        start_date = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=UTC_TZ)
        end_date = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=UTC_TZ)
        windfarm_ids = [int(x) for x in args.windfarm_ids.split(',')] if args.windfarm_ids else None

        processor = ElexonProcessor(session)
        await processor.process_date_range(start_date, end_date, windfarm_ids)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
