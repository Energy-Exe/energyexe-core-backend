#!/usr/bin/env python3
"""
Re-process aggregation for a year in parallel by month.

This script runs aggregation for multiple months concurrently to speed up processing.

Usage:
    # Re-process all of 2021 with 4 parallel workers (default source: ELEXON)
    poetry run python scripts/seeds/aggregate_generation_data/reprocess_year_parallel.py --year 2021 --workers 4

    # Re-process ENTSOE data for 2024
    poetry run python scripts/seeds/aggregate_generation_data/reprocess_year_parallel.py --year 2024 --source ENTSOE --workers 4

    # Re-process specific months
    poetry run python scripts/seeds/aggregate_generation_data/reprocess_year_parallel.py --year 2021 --months 3,6,11

    # Dry run
    poetry run python scripts/seeds/aggregate_generation_data/reprocess_year_parallel.py --year 2021 --dry-run
"""

import asyncio
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import calendar

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def get_month_range(year: int, month: int) -> tuple:
    """Get start and end date for a month."""
    start = f"{year}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    end = f"{year}-{month:02d}-{last_day:02d}"
    return start, end


def process_month(args: tuple) -> dict:
    """Process a single month using subprocess."""
    year, month, dry_run, source = args
    start, end = get_month_range(year, month)

    cmd = [
        "poetry", "run", "python",
        "scripts/seeds/aggregate_generation_data/process_generation_data_robust.py",
        "--start", start,
        "--end", end,
        "--source", source,
        "--monthly"
    ]

    month_name = datetime(year, month, 1).strftime("%B")
    print(f"[{month_name} {year}] Starting aggregation: {start} to {end}", flush=True)

    if dry_run:
        print(f"[{month_name} {year}] DRY RUN - would run: {' '.join(cmd)}", flush=True)
        return {"month": month, "status": "dry_run", "start": start, "end": end}

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1800  # 30 min timeout per month
        )

        if result.returncode == 0:
            print(f"[{month_name} {year}] ✓ Completed successfully", flush=True)
            return {"month": month, "status": "success", "start": start, "end": end}
        else:
            print(f"[{month_name} {year}] ✗ Failed: {result.stderr[:200]}", flush=True)
            return {"month": month, "status": "failed", "error": result.stderr[:500]}

    except subprocess.TimeoutExpired:
        print(f"[{month_name} {year}] ✗ Timeout after 30 minutes", flush=True)
        return {"month": month, "status": "timeout"}
    except Exception as e:
        print(f"[{month_name} {year}] ✗ Error: {e}", flush=True)
        return {"month": month, "status": "error", "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description='Re-process aggregation for a year in parallel')
    parser.add_argument('--year', type=int, required=True, help='Year to process')
    parser.add_argument('--months', type=str, help='Comma-separated months (1-12), default: all')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parser.add_argument('--source', type=str, default='ELEXON',
                        choices=['ENTSOE', 'ELEXON', 'TAIPOWER', 'NVE', 'ENERGISTYRELSEN'],
                        help='Data source to reprocess (default: ELEXON)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    args = parser.parse_args()

    year = args.year
    source = args.source

    if args.months:
        months = [int(m) for m in args.months.split(',')]
    else:
        months = list(range(1, 13))

    print("=" * 70)
    print(f"PARALLEL AGGREGATION REPROCESSING - {year}")
    print("=" * 70)
    print(f"Year: {year}")
    print(f"Source: {source}")
    print(f"Months: {months}")
    print(f"Workers: {args.workers}")
    if args.dry_run:
        print("MODE: DRY RUN")
    print("=" * 70)

    # Prepare tasks
    tasks = [(year, month, args.dry_run, source) for month in months]

    # Run in parallel
    results = []
    start_time = datetime.now()

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_month, task): task for task in tasks}

        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    success = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] in ('failed', 'error', 'timeout')]
    dry_runs = [r for r in results if r['status'] == 'dry_run']

    print(f"Total months: {len(results)}")
    print(f"Successful: {len(success)}")
    print(f"Failed: {len(failed)}")
    if dry_runs:
        print(f"Dry runs: {len(dry_runs)}")
    print(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")

    if failed:
        print("\nFailed months:")
        for r in failed:
            print(f"  Month {r['month']}: {r['status']} - {r.get('error', 'N/A')[:100]}")

    print("=" * 70)


if __name__ == "__main__":
    main()
