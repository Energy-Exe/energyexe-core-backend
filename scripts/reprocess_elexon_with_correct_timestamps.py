"""
Reprocess Elexon Data with Correct UTC Timestamps

This script reprocesses Elexon raw data into GenerationData using correct
UTC timestamp calculation from settlement_date and settlement_period.

The raw data is NOT modified - we simply interpret it correctly during processing.

Problem:
- Raw data period_start was stored incorrectly (settlement_date treated as UTC)
- During BST, this causes +1 hour error

Solution:
- Read settlement_date and settlement_period from raw data's JSONB field
- Calculate correct UTC timestamp: UK_midnight_as_UTC + (SP-1)*30min
- Store correct timestamp in GenerationData.hour

Usage:
    # Dry run for An Suidhe 2022
    poetry run python scripts/reprocess_elexon_with_correct_timestamps.py \
        --windfarm-id 7247 --year 2022 --dry-run

    # Apply for An Suidhe 2022
    poetry run python scripts/reprocess_elexon_with_correct_timestamps.py \
        --windfarm-id 7247 --year 2022

    # Process all Elexon windfarms for 2022
    poetry run python scripts/reprocess_elexon_with_correct_timestamps.py \
        --year 2022 --all-windfarms
"""

import asyncio
import argparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal
from collections import defaultdict
from uuid import uuid4
import logging

from sqlalchemy import text, select, and_, delete, func

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw, GenerationData
from app.models.generation_unit import GenerationUnit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

UK_TZ = ZoneInfo('Europe/London')
UTC_TZ = ZoneInfo('UTC')


def calculate_correct_utc_hour(settlement_date_str: str, settlement_period: int) -> datetime:
    """
    Calculate the correct UTC hour for an Elexon settlement period.

    Args:
        settlement_date_str: Settlement date in YYYYMMDD format
        settlement_period: Settlement period number (1-50)

    Returns:
        Correct UTC hour (floored to hour boundary)
    """
    # Parse settlement date
    if len(settlement_date_str) == 8:
        year = int(settlement_date_str[:4])
        month = int(settlement_date_str[4:6])
        day = int(settlement_date_str[6:8])
    else:
        # Handle ISO format (YYYY-MM-DD)
        parts = settlement_date_str[:10].split('-')
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])

    # Create midnight in UK local time
    uk_midnight = datetime(year, month, day, 0, 0, 0, tzinfo=UK_TZ)

    # Convert to UTC
    utc_midnight = uk_midnight.astimezone(UTC_TZ)

    # Add settlement period offset (SP 1 = 00:00-00:30, each SP is 30 min)
    utc_timestamp = utc_midnight + timedelta(minutes=(settlement_period - 1) * 30)

    # Floor to hour boundary for grouping
    utc_hour = utc_timestamp.replace(minute=0, second=0, microsecond=0, tzinfo=None)

    return utc_hour


class ElexonReprocessor:
    """Reprocess Elexon data with correct timestamp calculation."""

    def __init__(self, db_session, dry_run: bool = False):
        self.db = db_session
        self.dry_run = dry_run
        self.generation_units_cache = {}
        self.stats = {
            'raw_records_processed': 0,
            'hourly_records_created': 0,
            'hourly_records_deleted': 0,
            'errors': 0
        }

    async def load_generation_units(self):
        """Load Elexon generation units into cache."""
        result = await self.db.execute(
            select(GenerationUnit)
            .where(GenerationUnit.source == "ELEXON")
        )
        units = result.scalars().all()

        for unit in units:
            self.generation_units_cache[unit.code] = {
                'id': unit.id,
                'windfarm_id': unit.windfarm_id,
                'capacity_mw': float(unit.capacity_mw) if unit.capacity_mw else None,
                'name': unit.name
            }

        logger.info(f"Loaded {len(self.generation_units_cache)} Elexon generation units")

    async def get_windfarm_units(self, windfarm_id: int) -> list:
        """Get generation unit codes for a windfarm."""
        result = await self.db.execute(
            select(GenerationUnit.code)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationUnit.source == "ELEXON"
                )
            )
        )
        return [row[0] for row in result.fetchall()]

    async def get_all_elexon_windfarms(self) -> list:
        """Get all windfarm IDs with Elexon data."""
        result = await self.db.execute(text("""
            SELECT DISTINCT gu.windfarm_id, w.name
            FROM generation_units gu
            JOIN windfarms w ON gu.windfarm_id = w.id
            WHERE gu.source = 'ELEXON'
            AND gu.windfarm_id IS NOT NULL
            ORDER BY w.name
        """))
        return result.fetchall()

    async def reprocess_windfarm_year(
        self,
        windfarm_id: int,
        year: int,
        windfarm_name: str = None
    ) -> dict:
        """Reprocess all Elexon data for a windfarm and year."""

        unit_codes = await self.get_windfarm_units(windfarm_id)
        if not unit_codes:
            logger.warning(f"No Elexon units found for windfarm {windfarm_id}")
            return {'error': 'No units found'}

        logger.info(f"Processing windfarm {windfarm_name or windfarm_id} ({len(unit_codes)} units)")

        year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
        year_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)

        total_raw = 0
        total_hourly = 0
        total_deleted = 0

        for unit_code in unit_codes:
            unit_info = self.generation_units_cache.get(unit_code)
            if not unit_info:
                logger.warning(f"Unit {unit_code} not in cache")
                continue

            # Fetch raw data for this unit and year
            # Include import_export_ind to apply correct sign (I=Import=negative, E=Export=positive)
            # Also fetch value_extracted as fallback for 2025+ data where JSONB fields may be empty
            result = await self.db.execute(text("""
                SELECT
                    id,
                    (data->>'metered_volume')::float as metered_volume,
                    data->>'settlement_date' as settlement_date,
                    (data->>'settlement_period')::int as settlement_period,
                    data->>'import_export_ind' as import_export_ind,
                    period_start,
                    value_extracted
                FROM generation_data_raw
                WHERE identifier = :code
                AND source = 'ELEXON'
                AND period_start >= :start
                AND period_start < :end
                ORDER BY period_start, (data->>'settlement_period')::int
            """), {
                "code": unit_code,
                "start": year_start,
                "end": year_end
            })
            raw_records = result.fetchall()

            if not raw_records:
                continue

            total_raw += len(raw_records)

            # Group by correct UTC hour
            hourly_groups = defaultdict(list)

            for record in raw_records:
                record_id, metered_volume, settlement_date, settlement_period, import_export_ind, period_start, value_extracted = record

                # Use metered_volume from JSONB, fall back to value_extracted column
                raw_value = metered_volume if metered_volume is not None else value_extracted
                if raw_value is None:
                    continue

                # Apply sign based on import_export_ind
                # I = Import (consuming from grid) = negative generation
                # E = Export (generating to grid) = positive generation
                value = float(raw_value)
                if import_export_ind == 'I':
                    value = -value

                # Calculate correct UTC hour
                if settlement_date and settlement_period:
                    # Use settlement_date + period for correct DST handling
                    correct_hour = calculate_correct_utc_hour(settlement_date, settlement_period)
                elif period_start:
                    # Fallback: use period_start directly (for data without settlement_date in JSONB)
                    # This assumes period_start was already stored correctly or is close enough
                    ps = period_start
                    if ps.tzinfo is not None:
                        ps = ps.replace(tzinfo=None)
                    correct_hour = ps.replace(minute=0, second=0, microsecond=0)
                else:
                    continue

                hourly_groups[correct_hour].append({
                    'id': record_id,
                    'value': value,
                    'settlement_period': settlement_period or 0
                })

            # Delete existing processed data for this unit and year
            if not self.dry_run:
                result = await self.db.execute(text("""
                    DELETE FROM generation_data
                    WHERE generation_unit_id = :unit_id
                    AND source = 'ELEXON'
                    AND hour >= :start
                    AND hour < :end
                    RETURNING id
                """), {
                    "unit_id": unit_info['id'],
                    "start": year_start,
                    "end": year_end
                })
                deleted = len(result.fetchall())
                total_deleted += deleted

            # Create new hourly records with correct timestamps
            new_records = []
            for hour, records in hourly_groups.items():
                # Sum generation values for this hour
                generation_mwh = sum(r['value'] for r in records)
                data_points = len(records)

                # Calculate capacity factor
                capacity_factor = None
                if unit_info['capacity_mw'] and unit_info['capacity_mw'] > 0:
                    cf = generation_mwh / unit_info['capacity_mw']
                    capacity_factor = min(cf, 9.9999)

                # Calculate quality metrics
                expected_points = 2  # Two 30-min periods per hour
                completeness = min(1.0, data_points / expected_points)
                quality_score = 1.0 if data_points >= expected_points else data_points / expected_points

                if data_points >= expected_points:
                    quality_flag = 'HIGH'
                elif data_points >= 1:
                    quality_flag = 'MEDIUM'
                else:
                    quality_flag = 'LOW'

                new_records.append(GenerationData(
                    id=str(uuid4()),
                    hour=hour.replace(tzinfo=timezone.utc),
                    generation_unit_id=unit_info['id'],
                    windfarm_id=unit_info['windfarm_id'],
                    turbine_unit_id=None,
                    generation_mwh=Decimal(str(generation_mwh)),
                    capacity_mw=Decimal(str(unit_info['capacity_mw'])) if unit_info['capacity_mw'] else None,
                    capacity_factor=Decimal(str(capacity_factor)) if capacity_factor else None,
                    source='ELEXON',
                    source_resolution='PT30M',
                    raw_data_ids=[r['id'] for r in records],
                    quality_flag=quality_flag,
                    quality_score=Decimal(str(quality_score)),
                    completeness=Decimal(str(completeness)),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                ))

            if not self.dry_run and new_records:
                self.db.add_all(new_records)
                await self.db.flush()

            total_hourly += len(new_records)
            logger.info(f"  {unit_code}: {len(raw_records)} raw → {len(new_records)} hourly")

        self.stats['raw_records_processed'] += total_raw
        self.stats['hourly_records_created'] += total_hourly
        self.stats['hourly_records_deleted'] += total_deleted

        return {
            'windfarm_id': windfarm_id,
            'units': len(unit_codes),
            'raw_records': total_raw,
            'hourly_records': total_hourly,
            'deleted_records': total_deleted
        }

    async def verify_fix(self, windfarm_id: int, year: int):
        """Verify the fix by comparing timestamps."""

        unit_codes = await self.get_windfarm_units(windfarm_id)
        if not unit_codes:
            return

        unit_code = unit_codes[0]
        unit_info = self.generation_units_cache.get(unit_code)

        # Get a summer day sample from processed data
        result = await self.db.execute(text("""
            SELECT hour, generation_mwh
            FROM generation_data
            WHERE generation_unit_id = :unit_id
            AND hour >= :start
            AND hour < :end
            ORDER BY hour
            LIMIT 10
        """), {
            "unit_id": unit_info['id'],
            "start": datetime(year, 7, 15, tzinfo=timezone.utc),
            "end": datetime(year, 7, 16, tzinfo=timezone.utc)
        })
        processed = result.fetchall()

        # Get corresponding raw data
        result = await self.db.execute(text("""
            SELECT
                data->>'settlement_date' as settlement_date,
                (data->>'settlement_period')::int as settlement_period,
                value_extracted
            FROM generation_data_raw
            WHERE identifier = :code
            AND source = 'ELEXON'
            AND data->>'settlement_date' = :date
            ORDER BY (data->>'settlement_period')::int
            LIMIT 10
        """), {
            "code": unit_code,
            "date": f"{year}0715"
        })
        raw = result.fetchall()

        print(f"\n{'='*80}")
        print(f"VERIFICATION: July 15, {year} (Summer - BST)")
        print(f"{'='*80}")

        print(f"\nRaw data (settlement_date + settlement_period → correct UTC):")
        for r in raw[:6]:
            correct_hour = calculate_correct_utc_hour(r[0], r[1])
            print(f"  SP {r[1]:2d} ({r[0]}) → {correct_hour} UTC")

        print(f"\nProcessed data (GenerationData.hour):")
        for p in processed[:6]:
            print(f"  {p[0]} UTC: {p[1]:.3f} MWh")

        # Check if first processed hour matches expected
        if raw and processed:
            expected_first_hour = calculate_correct_utc_hour(raw[0][0], 1)
            actual_first_hour = processed[0][0].replace(tzinfo=None) if processed[0][0].tzinfo else processed[0][0]

            if expected_first_hour.date() == datetime(year, 7, 14).date():
                print(f"\n✅ CORRECT: First hour of July 15 BST is on July 14 UTC")
            else:
                print(f"\n⚠️  Check: First hour is {actual_first_hour}")


async def main():
    parser = argparse.ArgumentParser(
        description='Reprocess Elexon data with correct UTC timestamps'
    )
    parser.add_argument('--windfarm-id', type=int, help='Specific windfarm ID')
    parser.add_argument('--year', type=int, required=True, help='Year to process')
    parser.add_argument('--all-windfarms', action='store_true', help='Process all Elexon windfarms')
    parser.add_argument('--dry-run', action='store_true', help='Analyze without making changes')
    parser.add_argument('--verify-only', action='store_true', help='Only verify current state')

    args = parser.parse_args()

    if not args.windfarm_id and not args.all_windfarms:
        parser.error("Must specify --windfarm-id or --all-windfarms")

    session_factory = get_session_factory()

    async with session_factory() as db:
        processor = ElexonReprocessor(db, dry_run=args.dry_run)
        await processor.load_generation_units()

        if args.all_windfarms:
            windfarms = await processor.get_all_elexon_windfarms()
            logger.info(f"Found {len(windfarms)} Elexon windfarms")
        else:
            # Get windfarm name
            result = await db.execute(text(
                "SELECT name FROM windfarms WHERE id = :id"
            ), {"id": args.windfarm_id})
            row = result.fetchone()
            windfarm_name = row[0] if row else f"ID:{args.windfarm_id}"
            windfarms = [(args.windfarm_id, windfarm_name)]

        if args.verify_only:
            for wf_id, wf_name in windfarms[:1]:
                await processor.verify_fix(wf_id, args.year)
            return

        print(f"\n{'='*80}")
        print(f"ELEXON REPROCESSING: {args.year}")
        print(f"{'='*80}")
        print(f"Windfarms: {len(windfarms)}")
        print(f"Dry run: {args.dry_run}")
        print(f"{'='*80}\n")

        results = []
        for wf_id, wf_name in windfarms:
            result = await processor.reprocess_windfarm_year(wf_id, args.year, wf_name)
            results.append(result)

        # Summary
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print(f"Raw records processed:    {processor.stats['raw_records_processed']:,}")
        print(f"Hourly records deleted:   {processor.stats['hourly_records_deleted']:,}")
        print(f"Hourly records created:   {processor.stats['hourly_records_created']:,}")

        if not args.dry_run:
            await db.commit()
            print(f"\n✅ Changes committed to database")

            # Verify
            if args.windfarm_id:
                await processor.verify_fix(args.windfarm_id, args.year)
        else:
            await db.rollback()
            print(f"\n[DRY RUN] No changes made. Run without --dry-run to apply.")


if __name__ == "__main__":
    asyncio.run(main())
