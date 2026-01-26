"""
One-time backfill script for missing Elexon data in April/May 2016.

Uses ENTSOE data as a substitute for windfarms that have both Elexon and ENTSOE
generation units configured.

This is a TEMPORARY FIX - the long-term solution is to obtain the original
Elexon CSV files for April/May 2016.

Usage:
    # Dry run (preview only)
    poetry run python scripts/seeds/raw_generation_data/elexon/backfill_apr_may_2016_from_entsoe.py --dry-run

    # Full backfill + aggregation to hourly
    poetry run python scripts/seeds/raw_generation_data/elexon/backfill_apr_may_2016_from_entsoe.py

    # Raw data only (skip aggregation)
    poetry run python scripts/seeds/raw_generation_data/elexon/backfill_apr_may_2016_from_entsoe.py --skip-aggregate

    # Aggregation only (if raw data already inserted)
    poetry run python scripts/seeds/raw_generation_data/elexon/backfill_apr_may_2016_from_entsoe.py --aggregate-only

    # Check statistics
    poetry run python scripts/seeds/raw_generation_data/elexon/backfill_apr_may_2016_from_entsoe.py --stats

Coverage:
    - 10 windfarms with ENTSOE data available
    - ~107,000 raw records (30-min resolution)
    - Aggregated to hourly in generation_data table
    - Marked with source_type='entsoe_backfill' for easy identification
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import argparse
import json
from zoneinfo import ZoneInfo

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from sqlalchemy import text


# Mapping of ENTSOE codes to Elexon BMU codes for windfarms with matching data
# Based on windfarm relationships in the database
ENTSOE_TO_ELEXON_MAPPING = {
    # Barrow (1:1)
    '48W00000BOWLW-1K': ['T_BOWLW-1'],
    # Burbo Bank (1:1)
    '48W00000BURBW-1L': ['E_BURBO'],
    # Gwynt Y Mor (4:4 - matched by suffix pattern)
    '48W0000GYMRO-15O': ['T_GYMR-15'],
    '48W0000GYMRO-17K': ['T_GYMR-17'],
    '48W0000GYMRO-26J': ['T_GYMR-26'],
    '48W0000GYMRO-28F': ['T_GYMR-28'],
    # Lincs (2:2 - matched by suffix)
    '48W00000LNCSO-1R': ['T_LNCSW-1'],
    '48W00000LNCSO-2P': ['T_LNCSW-2'],
    # London Array (4:4 - matched by suffix)
    '48W00000LARYO-1Z': ['T_LARYW-1'],
    '48W00000LARYO-2X': ['T_LARYW-2'],
    '48W00000LARYO-3V': ['T_LARYW-3'],
    '48W00000LARYO-4T': ['T_LARYW-4'],
    # Robin Rigg (2:2)
    '48W000000RREW-14': ['T_RREW-1'],
    '48W000000RRWW-1P': ['T_RRWW-1'],
    # Sheringham Shoal (2:2)
    '48W00000SHRSO-1Y': ['T_SHRSW-1'],
    '48W00000SHRSO-2W': ['T_SHRSW-2'],
    # Walney (1:2 - ENTSOE data split between 2 Elexon units)
    '48W00000WLNYW-1A': ['T_WLNYW-1', 'T_WLNYO-2'],
    # West of Duddon Sands (2:2)
    '48W00000WDNSO-1H': ['T_WDNSO-1'],
    '48W00000WDNSO-2F': ['T_WDNSO-2'],
    # Westermost Rough (1:1)
    '48W00000WTMSO-1M': ['T_WTMSO-1'],
}


async def get_entsoe_data_for_period(
    db,
    start_date: datetime,
    end_date: datetime
) -> List[Dict]:
    """Fetch ENTSOE generation data for the specified period."""

    entsoe_codes = list(ENTSOE_TO_ELEXON_MAPPING.keys())
    all_records = []

    # Fetch in batches by identifier to avoid timeout
    for code in entsoe_codes:
        result = await db.execute(
            text('''
                SELECT
                    id,
                    identifier,
                    period_start,
                    period_end,
                    value_extracted,
                    data
                FROM generation_data_raw
                WHERE source = 'ENTSOE'
                AND identifier = :code
                AND period_start >= :start_date
                AND period_start < :end_date
                ORDER BY period_start
            '''),
            {
                'code': code,
                'start_date': start_date,
                'end_date': end_date
            }
        )
        rows = result.fetchall()
        all_records.extend([dict(row._mapping) for row in rows])

    return all_records


def transform_entsoe_to_elexon(entsoe_record: Dict) -> List[Dict]:
    """
    Transform an ENTSOE record to Elexon format.

    ENTSOE data is hourly (PT60M), Elexon is 30-minute (PT30M).
    Creates 2 Elexon records per ENTSOE record.

    Value handling:
    - ENTSOE value is in MW (average power for the hour)
    - Elexon stores MWh for each 30-min period
    - 1 hour at X MW = X MWh total = 2 periods of X/2 MWh each
    """

    entsoe_code = entsoe_record['identifier']
    elexon_codes = ENTSOE_TO_ELEXON_MAPPING.get(entsoe_code, [])

    if not elexon_codes:
        return []

    period_start = entsoe_record['period_start']
    period_end = entsoe_record['period_end']
    value = float(entsoe_record['value_extracted']) if entsoe_record['value_extracted'] else 0

    # ENTSOE value is MW for the hour
    # Convert to MWh for 30-min period: MW * 0.5 hours = MWh
    value_per_30min = value * 0.5

    # If mapping to multiple Elexon units, split the value
    value_per_unit = value_per_30min / len(elexon_codes)

    records = []

    for elexon_code in elexon_codes:
        # First 30-minute period
        mid_point = period_start + timedelta(minutes=30)

        # Calculate settlement period (1-48 for UK)
        # Period 1 starts at 00:00, each period is 30 min
        hour = period_start.hour
        minute = period_start.minute
        settlement_period_1 = (hour * 2) + (1 if minute < 30 else 2)
        settlement_period_2 = settlement_period_1 + 1
        if settlement_period_2 > 48:
            settlement_period_2 = 1  # Wrap around for midnight

        # First half-hour
        records.append({
            'source': 'ELEXON',
            'source_type': 'entsoe_backfill',
            'identifier': elexon_code,
            'period_start': period_start,
            'period_end': mid_point,
            'period_type': 'PT30M',
            'value_extracted': round(value_per_unit, 3),
            'unit': 'MWh',
            'data': {
                'bmu_id': elexon_code,
                'settlement_date': period_start.strftime('%Y%m%d'),
                'settlement_period': settlement_period_1,
                'metered_volume': round(value_per_unit, 3),
                'backfill_source': 'ENTSOE',
                'original_entsoe_code': entsoe_code,
                'original_entsoe_value_mw': value,
                'backfill_timestamp': datetime.utcnow().isoformat(),
                'backfill_note': 'Temporary backfill for missing Apr/May 2016 Elexon data'
            }
        })

        # Second half-hour
        records.append({
            'source': 'ELEXON',
            'source_type': 'entsoe_backfill',
            'identifier': elexon_code,
            'period_start': mid_point,
            'period_end': period_end,
            'period_type': 'PT30M',
            'value_extracted': round(value_per_unit, 3),
            'unit': 'MWh',
            'data': {
                'bmu_id': elexon_code,
                'settlement_date': period_start.strftime('%Y%m%d'),
                'settlement_period': settlement_period_2,
                'metered_volume': round(value_per_unit, 3),
                'backfill_source': 'ENTSOE',
                'original_entsoe_code': entsoe_code,
                'original_entsoe_value_mw': value,
                'backfill_timestamp': datetime.utcnow().isoformat(),
                'backfill_note': 'Temporary backfill for missing Apr/May 2016 Elexon data'
            }
        })

    return records


async def insert_backfill_records(db, records: List[Dict], batch_size: int = 1000):
    """Insert backfill records into generation_data_raw using bulk INSERT."""

    total = len(records)
    inserted = 0

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]

        # Build multi-value INSERT statement
        values_list = []
        params = {}

        for idx, r in enumerate(batch):
            values_list.append(
                f"(:source_{idx}, :source_type_{idx}, :identifier_{idx}, "
                f":period_start_{idx}, :period_end_{idx}, :period_type_{idx}, "
                f":value_extracted_{idx}, :unit_{idx}, cast(:data_{idx} as jsonb), NOW(), NOW())"
            )
            params[f'source_{idx}'] = r['source']
            params[f'source_type_{idx}'] = r['source_type']
            params[f'identifier_{idx}'] = r['identifier']
            params[f'period_start_{idx}'] = r['period_start']
            params[f'period_end_{idx}'] = r['period_end']
            params[f'period_type_{idx}'] = r['period_type']
            params[f'value_extracted_{idx}'] = r['value_extracted']
            params[f'unit_{idx}'] = r['unit']
            params[f'data_{idx}'] = json.dumps(r['data'])

        values_sql = ',\n'.join(values_list)

        await db.execute(
            text(f'''
                INSERT INTO generation_data_raw
                    (source, source_type, identifier, period_start, period_end,
                     period_type, value_extracted, unit, data, created_at, updated_at)
                VALUES
                    {values_sql}
                ON CONFLICT DO NOTHING
            '''),
            params
        )

        inserted += len(batch)
        print(f'  Inserted {inserted}/{total} records...', end='\r')

        # Commit after each batch
        await db.commit()

    print(f'  Inserted {inserted}/{total} records      ')
    return inserted


async def check_existing_backfill(db) -> int:
    """Check if backfill data already exists."""
    result = await db.execute(
        text('''
            SELECT COUNT(*) as count
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND source_type = 'entsoe_backfill'
            AND period_start >= '2016-04-01'
            AND period_start < '2016-06-01'
        ''')
    )
    return result.scalar()


async def run_backfill(dry_run: bool = True, force: bool = False):
    """Main backfill function."""

    print('='*70)
    print('ELEXON Apr/May 2016 Backfill from ENTSOE Data')
    print('='*70)
    print(f'\nMode: {"DRY RUN (no changes)" if dry_run else "LIVE (will insert data)"}')
    print(f'Mapping {len(ENTSOE_TO_ELEXON_MAPPING)} ENTSOE units to Elexon BMUs')

    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Check for existing backfill
        existing = await check_existing_backfill(db)
        if existing > 0:
            print(f'\n⚠️  Found {existing} existing backfill records.')
            if not dry_run and not force:
                response = input('Continue and add more? (y/N): ')
                if response.lower() != 'y':
                    print('Aborted.')
                    return
            elif not dry_run and force:
                print('   --force specified, continuing...')

        all_records = []

        # Process April 2016
        # Use UK timezone to match Elexon settlement days
        # April 1 00:00 UK time -> UTC (BST starts March 27, 2016)
        uk_tz = ZoneInfo('Europe/London')
        april_start_uk = datetime(2016, 4, 1, 0, 0, tzinfo=uk_tz)
        may_start_uk = datetime(2016, 5, 1, 0, 0, tzinfo=uk_tz)
        june_start_uk = datetime(2016, 6, 1, 0, 0, tzinfo=uk_tz)

        print('\n📅 Fetching ENTSOE data for April 2016...')
        print(f'   UK date range: {april_start_uk} to {may_start_uk}')
        april_data = await get_entsoe_data_for_period(
            db,
            april_start_uk,
            may_start_uk
        )
        print(f'   Found {len(april_data)} ENTSOE records')

        for record in april_data:
            all_records.extend(transform_entsoe_to_elexon(record))

        # Process May 2016
        print('\n📅 Fetching ENTSOE data for May 2016...')
        print(f'   UK date range: {may_start_uk} to {june_start_uk}')
        may_data = await get_entsoe_data_for_period(
            db,
            may_start_uk,
            june_start_uk
        )
        print(f'   Found {len(may_data)} ENTSOE records')

        for record in may_data:
            all_records.extend(transform_entsoe_to_elexon(record))

        print(f'\n📊 Total Elexon records to create: {len(all_records)}')

        # Show breakdown by BMU
        bmu_counts = {}
        for r in all_records:
            bmu = r['identifier']
            bmu_counts[bmu] = bmu_counts.get(bmu, 0) + 1

        print('\nBreakdown by BMU:')
        for bmu in sorted(bmu_counts.keys()):
            print(f'   {bmu}: {bmu_counts[bmu]} records')

        if dry_run:
            print('\n✅ DRY RUN complete. No data was inserted.')
            print(f'   Run without --dry-run to insert {len(all_records)} records.')

            # Show sample record
            if all_records:
                print('\nSample record:')
                sample = all_records[0]
                for k, v in sample.items():
                    print(f'   {k}: {v}')
        else:
            print('\n📝 Inserting records...')
            inserted = await insert_backfill_records(db, all_records)

            print(f'\n✅ Successfully inserted {inserted} records')
            print('   source_type = "entsoe_backfill"')
            print('   These can be identified and removed later if needed.')

            return True  # Signal success for aggregation step

    return False


async def run_aggregation():
    """Run the aggregation to convert raw 30-min data to hourly generation_data."""

    print('\n' + '='*70)
    print('STEP 2: Aggregating to Hourly Data')
    print('='*70)

    # Import the aggregation processor
    from scripts.seeds.aggregate_generation_data.process_generation_data_robust import RobustGenerationProcessor
    from datetime import date

    processor = RobustGenerationProcessor(
        source='ELEXON',
        dry_run=False,
        log_dir='generation_processing_logs'
    )

    # Process April and May 2016
    start_date = date(2016, 4, 1)
    end_date = date(2016, 5, 31)

    processor.initialize_logging(start_date, end_date)

    print(f'\n📅 Processing {start_date} to {end_date}...')
    print('   This aggregates 30-min raw data into hourly generation_data records.')

    await processor.process_date_range(start_date, end_date)

    print(f'\n✅ Aggregation complete!')
    print(f'   Processed days: {processor.processed_days}')
    print(f'   Failed days: {processor.failed_days}')
    print(f'   Total hourly records created: {processor.total_hourly_records}')


async def show_stats():
    """Show current backfill statistics."""

    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text('''
                SELECT
                    identifier,
                    COUNT(*) as records,
                    MIN(period_start) as first_record,
                    MAX(period_end) as last_record
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                AND source_type = 'entsoe_backfill'
                GROUP BY identifier
                ORDER BY identifier
            ''')
        )
        rows = result.fetchall()

        if rows:
            print('\n📊 Existing backfill data:')
            print('-'*70)
            total = 0
            for r in rows:
                print(f'   {r.identifier}: {r.records} records ({r.first_record} to {r.last_record})')
                total += r.records
            print('-'*70)
            print(f'   Total: {total} records')
        else:
            print('\n📊 No backfill data found.')


def main():
    parser = argparse.ArgumentParser(
        description='Backfill missing Elexon Apr/May 2016 data from ENTSOE'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show current backfill statistics'
    )
    parser.add_argument(
        '--skip-aggregate',
        action='store_true',
        help='Skip the hourly aggregation step (only insert raw data)'
    )
    parser.add_argument(
        '--aggregate-only',
        action='store_true',
        help='Only run the aggregation step (skip raw data insertion)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Skip confirmation prompts (useful for non-interactive runs)'
    )

    args = parser.parse_args()

    if args.stats:
        asyncio.run(show_stats())
    elif args.aggregate_only:
        asyncio.run(run_aggregation())
    else:
        success = asyncio.run(run_backfill(dry_run=args.dry_run, force=args.force))

        # Run aggregation if backfill succeeded and not skipped
        if success and not args.dry_run and not args.skip_aggregate:
            asyncio.run(run_aggregation())
        elif success and not args.dry_run and args.skip_aggregate:
            print('\n⚠️  Aggregation skipped. Run with --aggregate-only to aggregate later.')
            print('   Or run:')
            print('   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \\')
            print('     --source ELEXON --start 2016-04-01 --end 2016-05-31')


if __name__ == '__main__':
    main()
