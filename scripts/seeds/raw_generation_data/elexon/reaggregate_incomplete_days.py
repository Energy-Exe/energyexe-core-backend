#!/usr/bin/env python3
"""
Re-aggregate ELEXON data for days that had incomplete B1610 data.

After re-importing raw data with reimport_incomplete_days.py, run this script
to re-aggregate those days into the generation_data table.

Uses single-day processing to avoid the monthly batch mode bug.

Usage:
    poetry run python scripts/seeds/raw_generation_data/elexon/reaggregate_incomplete_days.py
    poetry run python scripts/seeds/raw_generation_data/elexon/reaggregate_incomplete_days.py --dry-run
"""

import asyncio
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import asyncpg

current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.config import get_settings


async def find_incomplete_days():
    """Find all settlement days that had incomplete B1610 data."""
    settings = get_settings()
    dsn = str(settings.DATABASE_URL).replace('postgresql+asyncpg://', 'postgresql://')
    conn = await asyncpg.connect(dsn)

    rows = await conn.fetch('''
        WITH daily_bmu_sp AS (
            SELECT
                r.data->>'settlement_date' as sd,
                r.identifier,
                COUNT(DISTINCT (r.data->>'settlement_period')::int) as sp_count
            FROM generation_data_raw r
            WHERE r.source = 'ELEXON' AND r.source_type = 'api'
              AND r.period_start >= '2025-01-01' AND r.period_start < '2026-01-01'
              AND r.data->>'settlement_date' IS NOT NULL
            GROUP BY r.data->>'settlement_date', r.identifier
        ),
        daily_stats AS (
            SELECT
                sd,
                COUNT(*) FILTER (WHERE sp_count < 46) as incomplete_bmus
            FROM daily_bmu_sp
            GROUP BY sd
        )
        SELECT sd
        FROM daily_stats
        WHERE incomplete_bmus > 10
        ORDER BY sd
    ''')

    await conn.close()
    return [r['sd'] for r in rows if r['sd'] is not None]


def group_into_ranges(dates, max_gap=2):
    """Group dates into contiguous ranges."""
    if not dates:
        return []

    sorted_dates = sorted([datetime.strptime(d, '%Y-%m-%d') for d in dates])
    ranges = []
    range_start = sorted_dates[0]
    range_end = sorted_dates[0]

    for d in sorted_dates[1:]:
        if (d - range_end).days <= max_gap:
            range_end = d
        else:
            ranges.append((range_start.strftime('%Y-%m-%d'), range_end.strftime('%Y-%m-%d')))
            range_start = d
            range_end = d

    ranges.append((range_start.strftime('%Y-%m-%d'), range_end.strftime('%Y-%m-%d')))
    return ranges


def run_aggregation(start_date, end_date):
    """Run the robust aggregation script for a date range."""
    cmd = [
        'poetry', 'run', 'python',
        'scripts/seeds/aggregate_generation_data/process_generation_data_robust.py',
        '--source', 'ELEXON',
        '--start', start_date,
        '--end', end_date,
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=7200,  # 2 hour timeout per range
        cwd=str(current_dir.parent.parent.parent.parent),
    )
    return result


async def main(dry_run=False):
    print("=" * 80)
    print("ELEXON INCOMPLETE DAY RE-AGGREGATION")
    print("=" * 80)

    # Find incomplete days
    print("\nFinding days to re-aggregate...")
    incomplete_days = await find_incomplete_days()
    print(f"Found {len(incomplete_days)} days to re-aggregate")

    # Group into ranges
    ranges = group_into_ranges(incomplete_days)
    total_days = sum(
        (datetime.strptime(e, '%Y-%m-%d') - datetime.strptime(s, '%Y-%m-%d')).days + 1
        for s, e in ranges
    )
    print(f"Grouped into {len(ranges)} contiguous ranges ({total_days} total days)")

    if dry_run:
        print("\nDRY RUN - showing ranges that would be re-aggregated:")
        for i, (start, end) in enumerate(ranges, 1):
            days = (datetime.strptime(end, '%Y-%m-%d') - datetime.strptime(start, '%Y-%m-%d')).days + 1
            print(f"  Range {i}: {start} to {end} ({days} days)")
        return

    print(f"\nStarting re-aggregation...")
    print("=" * 80)

    results = []
    start_time = datetime.now()

    for i, (start, end) in enumerate(ranges, 1):
        days = (datetime.strptime(end, '%Y-%m-%d') - datetime.strptime(start, '%Y-%m-%d')).days + 1
        print(f"\n[Range {i}/{len(ranges)}] {start} to {end} ({days} days)")
        print("-" * 60)

        range_start = datetime.now()
        try:
            result = run_aggregation(start, end)
            elapsed = (datetime.now() - range_start).total_seconds()

            if result.returncode == 0:
                print(f"  OK ({elapsed:.0f}s)")
                results.append(('ok', start, end))
            else:
                print(f"  FAILED ({elapsed:.0f}s)")
                stderr_lines = result.stderr.strip().split('\n')[-5:]
                for line in stderr_lines:
                    print(f"    {line}")
                results.append(('failed', start, end))
        except subprocess.TimeoutExpired:
            print(f"  TIMEOUT (>7200s)")
            results.append(('timeout', start, end))
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append(('error', start, end))

    total_elapsed = (datetime.now() - start_time).total_seconds()

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total ranges: {len(ranges)}")
    print(f"Successful: {sum(1 for r in results if r[0] == 'ok')}")
    print(f"Failed: {sum(1 for r in results if r[0] != 'ok')}")
    print(f"Duration: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")

    failed = [r for r in results if r[0] != 'ok']
    if failed:
        print(f"\nFailed ranges:")
        for status, start, end in failed:
            print(f"  {start} to {end}: {status}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Re-aggregate incomplete ELEXON days')
    parser.add_argument('--dry-run', action='store_true', help='Show ranges without aggregating')
    args = parser.parse_args()

    asyncio.run(main(dry_run=args.dry_run))
