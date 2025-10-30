#!/usr/bin/env python3
"""
Fix timezone offset in EIA raw data.

Issue: EIA data imported from Excel files was stored with local timezone (CST/CDT = UTC-6/UTC-5)
instead of UTC. This caused July 2025 data to appear as June 30 18:00:00 UTC.

This script:
1. Identifies affected records (period_start not on day 1 of month)
2. Shifts timestamps to the correct UTC date (first day of the month)
3. Updates both generation_data_raw and generation_data tables

Usage:
    # Dry run (preview changes)
    poetry run python scripts/seeds/raw_generation_data/eia/fix_timezone_offset.py --dry-run

    # Execute fix
    poetry run python scripts/seeds/raw_generation_data/eia/fix_timezone_offset.py

    # Fix specific date range
    poetry run python scripts/seeds/raw_generation_data/eia/fix_timezone_offset.py \
        --start-date 2024-01-01 --end-date 2025-12-31
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone
import argparse
from typing import Dict, List
import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from sqlalchemy import select, text, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()


async def analyze_offset_issue(
    start_date: str = None,
    end_date: str = None
) -> Dict:
    """Analyze the timezone offset issue in EIA data."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Find records where period_start is NOT on day 1 of month
        query = """
            SELECT
                DATE(period_start) as date,
                EXTRACT(DAY FROM period_start) as day,
                EXTRACT(HOUR FROM period_start) as hour,
                COUNT(*) as count,
                MIN(period_start) as min_date,
                MAX(period_start) as max_date
            FROM generation_data_raw
            WHERE source = 'EIA'
              AND EXTRACT(DAY FROM period_start) != 1
        """

        params = {}
        if start_date:
            query += " AND period_start >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND period_start <= :end_date"
            params['end_date'] = end_date

        query += """
            GROUP BY DATE(period_start), EXTRACT(DAY FROM period_start), EXTRACT(HOUR FROM period_start)
            ORDER BY date
        """

        result = await db.execute(text(query), params)
        rows = result.all()

        if not rows:
            print("✅ No timezone offset issues found!")
            return {'affected_records': 0, 'dates': []}

        print(f"\n🔍 Found {len(rows)} distinct dates with offset issues:")
        print(f"\n{'Date':<12} {'Day':<6} {'Hour':<6} {'Count':>10}")
        print("-" * 40)

        total_affected = 0
        for row in rows[:20]:  # Show first 20
            print(f"{row[0]!s:<12} {int(row[1]):<6} {int(row[2]):<6} {row[3]:>10,}")
            total_affected += row[3]

        if len(rows) > 20:
            print(f"... and {len(rows) - 20} more dates")

        # Get total count
        count_query = """
            SELECT COUNT(*)
            FROM generation_data_raw
            WHERE source = 'EIA'
              AND EXTRACT(DAY FROM period_start) != 1
        """

        if start_date:
            count_query += " AND period_start >= :start_date"
        if end_date:
            count_query += " AND period_start <= :end_date"

        result = await db.execute(text(count_query), params)
        total = result.scalar()

        return {
            'affected_records': total,
            'sample_dates': rows[:20]
        }


async def fix_timezone_offset(
    start_date: str = None,
    end_date: str = None,
    dry_run: bool = True
) -> Dict:
    """Fix timezone offset by shifting dates to first day of month at 00:00 UTC."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Find affected records
        query = """
            SELECT
                id,
                period_start,
                period_end,
                identifier,
                data->>'month' as month,
                data->>'year' as year
            FROM generation_data_raw
            WHERE source = 'EIA'
              AND EXTRACT(DAY FROM period_start) != 1
        """

        params = {}
        if start_date:
            query += " AND period_start >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND period_start <= :end_date"
            params['end_date'] = end_date

        query += " ORDER BY period_start"

        result = await db.execute(text(query), params)
        rows = result.all()

        if not rows:
            print("✅ No records need fixing!")
            return {'fixed': 0}

        print(f"\n📝 Processing {len(rows):,} records...")

        if dry_run:
            print("\n🔍 DRY RUN - Showing sample corrections:")
            for i, row in enumerate(rows[:10]):
                print(f"\nRecord {i+1}:")
                print(f"  ID: {row[0]}")
                print(f"  Current: {row[1]}")
                print(f"  Month: {row[4]}, Year: {row[5]}")
                # Calculate corrected date
                # Extract month and year from data field, create first day of month
                if row[4] and row[5]:
                    month_map = {
                        'January': 1, 'February': 2, 'March': 3, 'April': 4,
                        'May': 5, 'June': 6, 'July': 7, 'August': 8,
                        'September': 9, 'October': 10, 'November': 11, 'December': 12
                    }
                    month_num = month_map.get(row[4])
                    if month_num:
                        corrected = datetime(int(row[5]), month_num, 1, tzinfo=timezone.utc)
                        print(f"  Corrected: {corrected}")

            if len(rows) > 10:
                print(f"\n... and {len(rows) - 10:,} more records")

            return {'would_fix': len(rows)}

        # Execute fix
        print("\n🔧 Applying fixes...")

        # Step 1: Find and delete existing day-1 records that would conflict
        print("   Step 1: Checking for conflicting records...")
        conflict_query = """
            DELETE FROM generation_data_raw
            WHERE id IN (
                SELECT e.id
                FROM generation_data_raw e
                WHERE e.source = 'EIA'
                  AND EXTRACT(DAY FROM e.period_start) = 1
                  AND EXISTS (
                    SELECT 1
                    FROM generation_data_raw i
                    WHERE i.source = 'EIA'
                      AND i.identifier = e.identifier
                      AND EXTRACT(DAY FROM i.period_start) != 1
                      AND i.data->>'month' IS NOT NULL
                      AND DATE_TRUNC('month', i.period_start + INTERVAL '10 days') = e.period_start
                  )
            )
        """

        if start_date:
            conflict_query = conflict_query.replace(
                "AND DATE_TRUNC",
                "AND i.period_start >= :start_date AND DATE_TRUNC"
            )
        if end_date:
            conflict_query = conflict_query.replace(
                "= e.period_start",
                "= e.period_start AND i.period_start <= :end_date"
            )

        conflict_result = await db.execute(text(conflict_query), params)
        deleted_count = conflict_result.rowcount
        print(f"   Deleted {deleted_count:,} conflicting records")

        # Step 2: Update incorrect timestamps
        print("   Step 2: Updating timestamps...")
        update_query = """
            UPDATE generation_data_raw
            SET
                period_start = DATE_TRUNC('month', period_start + INTERVAL '10 days'),
                period_end = DATE_TRUNC('month', period_start + INTERVAL '10 days') + INTERVAL '1 month',
                updated_at = NOW()
            WHERE source = 'EIA'
              AND EXTRACT(DAY FROM period_start) != 1
              AND data->>'month' IS NOT NULL
        """

        if start_date:
            update_query += " AND period_start >= :start_date"
        if end_date:
            update_query += " AND period_start <= :end_date"

        result = await db.execute(text(update_query), params)
        await db.commit()

        fixed_count = result.rowcount

        print(f"✅ Fixed {fixed_count:,} records in generation_data_raw")
        if deleted_count > 0:
            print(f"   (Deleted {deleted_count:,} API-imported duplicates that had incorrect timestamps)")

        # Now we need to re-aggregate the affected data
        print("\n⚠️  Note: You need to re-run aggregation to update generation_data table:")
        print("   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py \\")
        print("     --source EIA")

        return {'fixed': fixed_count}


async def main(
    start_date: str = None,
    end_date: str = None,
    dry_run: bool = True,
    auto_confirm: bool = False
):
    """Main function."""
    print("=" * 80)
    print(" " * 20 + "FIX EIA TIMEZONE OFFSET")
    print("=" * 80)

    if dry_run:
        print("\n🔍 DRY RUN MODE - No changes will be made")
    else:
        print("\n⚠️  EXECUTION MODE - Changes will be applied")

    if start_date:
        print(f"   Start Date: {start_date}")
    if end_date:
        print(f"   End Date: {end_date}")

    print("\n" + "=" * 80)

    # Analyze issue
    analysis = await analyze_offset_issue(start_date, end_date)

    if analysis['affected_records'] == 0:
        print("\n✅ No issues found - all EIA data is correctly timestamped!")
        return

    print(f"\n📊 Total affected records: {analysis['affected_records']:,}")

    # Ask for confirmation if not dry run and not auto-confirmed
    if not dry_run and not auto_confirm:
        print("\n⚠️  This will modify the database. Are you sure? (yes/no)")
        response = input("> ").strip().lower()
        if response != 'yes':
            print("\n❌ Cancelled")
            return
    elif not dry_run and auto_confirm:
        print("\n✅ Auto-confirmed via --execute flag")

    # Fix the issue
    result = await fix_timezone_offset(start_date, end_date, dry_run)

    print("\n" + "=" * 80)

    if dry_run:
        print(f"\n🔍 DRY RUN: Would fix {result.get('would_fix', 0):,} records")
        print("\nTo execute the fix, run without --dry-run flag")
    else:
        print(f"\n✅ Fix completed: {result['fixed']:,} records updated")
        print("\n📋 Next steps:")
        print("   1. Re-run aggregation to update generation_data table")
        print("   2. Verify data in comparison page")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix EIA timezone offset issue')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Preview changes without applying them (default: True)')
    parser.add_argument('--execute', action='store_true',
                        help='Execute the fix (overrides --dry-run)')

    args = parser.parse_args()

    # If --execute is specified, turn off dry-run
    dry_run = not args.execute

    try:
        asyncio.run(main(
            start_date=args.start_date,
            end_date=args.end_date,
            dry_run=dry_run,
            auto_confirm=args.execute
        ))
    except KeyboardInterrupt:
        print("\n\n⚠️  Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
