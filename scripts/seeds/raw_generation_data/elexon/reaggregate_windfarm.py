#!/usr/bin/env python3
"""
Re-aggregate ELEXON data for a specific windfarm.

This script re-runs the aggregation process for a specific windfarm or BMU,
useful when raw data has been updated and aggregated data needs to be refreshed.

Usage:
    # Re-aggregate by windfarm ID
    poetry run python scripts/seeds/raw_generation_data/elexon/reaggregate_windfarm.py \
        --windfarm-id 7244 --start 2025-01-01 --end 2025-12-31

    # Re-aggregate by BMU code
    poetry run python scripts/seeds/raw_generation_data/elexon/reaggregate_windfarm.py \
        --bmu T_AFTOW-1 --start 2025-01-01 --end 2025-12-31

    # Dry run
    poetry run python scripts/seeds/raw_generation_data/elexon/reaggregate_windfarm.py \
        --bmu T_AFTOW-1 --start 2025-01-01 --end 2025-12-31 --dry-run
"""

import asyncio
import argparse
import functools
import sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Override print to always flush
_original_print = print
print = functools.partial(_original_print, flush=True)

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from app.core.database import get_session_factory
from scripts.seeds.aggregate_generation_data.process_generation_data_daily import DailyGenerationProcessor
import asyncpg
import os
from dotenv import load_dotenv


def parse_args():
    parser = argparse.ArgumentParser(
        description='Re-aggregate ELEXON data for a specific windfarm',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # By windfarm ID
  %(prog)s --windfarm-id 7244 --start 2025-01-01 --end 2025-12-31

  # By BMU code
  %(prog)s --bmu T_AFTOW-1 --start 2025-01-01 --end 2025-12-31

  # Dry run
  %(prog)s --bmu T_AFTOW-1 --start 2025-01-01 --end 2025-12-31 --dry-run
        """
    )
    parser.add_argument('--windfarm-id', type=int,
                        help='Windfarm ID to re-aggregate')
    parser.add_argument('--bmu', type=str,
                        help='BMU code to re-aggregate (will lookup windfarm ID)')
    parser.add_argument('--start', type=str, required=True,
                        help='Start date YYYY-MM-DD')
    parser.add_argument('--end', type=str, required=True,
                        help='End date YYYY-MM-DD')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without making changes')
    return parser.parse_args()


async def get_windfarm_id_from_bmu(bmu_code: str) -> tuple:
    """Get windfarm ID and info from BMU code."""
    load_dotenv()
    DATABASE_URL = os.getenv('DATABASE_URL')
    if DATABASE_URL and DATABASE_URL.startswith('postgresql+asyncpg://'):
        DATABASE_URL = DATABASE_URL.replace('postgresql+asyncpg://', 'postgresql://', 1)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        row = await conn.fetchrow('''
            SELECT gu.id, gu.code, gu.windfarm_id, w.name as windfarm_name
            FROM generation_units gu
            LEFT JOIN windfarms w ON w.id = gu.windfarm_id
            WHERE gu.code = $1
        ''', bmu_code)

        if not row:
            return None, None, None

        return row['windfarm_id'], row['windfarm_name'], row['id']
    finally:
        await conn.close()


async def run_aggregation(start_date: str, end_date: str, windfarm_id: int, dry_run: bool) -> dict:
    """Run aggregation for a specific windfarm."""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=ZoneInfo('UTC'))
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=ZoneInfo('UTC'))

    session_factory = get_session_factory()
    stats = {
        'days_processed': 0,
        'hourly_records_created': 0,
        'errors': 0
    }

    total_days = (end_dt - start_dt).days + 1
    print(f"\nProcessing {total_days} days for windfarm {windfarm_id}...")

    async with session_factory() as session:
        processor = DailyGenerationProcessor(session, dry_run=dry_run)

        # Load generation units once
        await processor.load_generation_units()

        current_date = start_dt
        while current_date <= end_dt:
            try:
                result = await processor.process_day(
                    date=current_date,
                    sources=['ELEXON'],
                    windfarm_id=windfarm_id,
                    skip_load_units=True,
                    skip_commit=True
                )

                stats['days_processed'] += 1
                elexon_result = result.get('sources', {}).get('ELEXON', {})
                stats['hourly_records_created'] += elexon_result.get('hourly_records', 0)

                # Progress output every 30 days
                if stats['days_processed'] % 30 == 0:
                    print(f"  Processed {stats['days_processed']}/{total_days} days...")

            except Exception as e:
                print(f"  Error processing {current_date.date()}: {e}")
                stats['errors'] += 1

            current_date += timedelta(days=1)

        # Commit all changes
        if not dry_run:
            await session.commit()
            print("  Changes committed to database")
        else:
            await session.rollback()
            print("  Dry run - changes rolled back")

    return stats


async def main():
    args = parse_args()

    if not args.windfarm_id and not args.bmu:
        print("Error: Must specify either --windfarm-id or --bmu")
        sys.exit(1)

    print("=" * 70)
    print("ELEXON Data Re-Aggregation")
    print("=" * 70)

    # Get windfarm ID
    windfarm_id = args.windfarm_id
    windfarm_name = None

    if args.bmu:
        print(f"\nLooking up BMU: {args.bmu}")
        windfarm_id, windfarm_name, unit_id = await get_windfarm_id_from_bmu(args.bmu)
        if not windfarm_id:
            print(f"Error: BMU '{args.bmu}' not found or has no windfarm")
            sys.exit(1)
        print(f"  Found: Windfarm ID={windfarm_id}, Name={windfarm_name}")

    print(f"\nConfiguration:")
    print(f"  Windfarm ID: {windfarm_id}")
    if windfarm_name:
        print(f"  Windfarm Name: {windfarm_name}")
    print(f"  Date Range: {args.start} to {args.end}")
    print(f"  Dry Run: {args.dry_run}")

    # Run aggregation
    print("\n" + "-" * 70)
    print("Running Aggregation")
    print("-" * 70)

    stats = await run_aggregation(args.start, args.end, windfarm_id, args.dry_run)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Days processed: {stats['days_processed']}")
    print(f"  Hourly records created/updated: {stats['hourly_records_created']}")
    if stats['errors'] > 0:
        print(f"  Errors: {stats['errors']}")

    if args.dry_run:
        print("\nDRY RUN - No changes made to database")
    else:
        print("\nRE-AGGREGATION COMPLETE")

    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
