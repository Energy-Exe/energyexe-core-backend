#!/usr/bin/env python3
"""
ERA5 Historical Data Backfill Script

Orchestrates historical backfill of ERA5 weather data from 1995 to present.
Fetches and processes data month-by-month.

Usage:
    # Full backfill (1995-2025, 30 years)
    poetry run python scripts/seeds/weather_data/era5/backfill_historical.py

    # Partial backfill (2020-2025)
    poetry run python scripts/seeds/weather_data/era5/backfill_historical.py \
        --start-year 2020

    # Specific year range
    poetry run python scripts/seeds/weather_data/era5/backfill_historical.py \
        --start-year 2020 --end-year 2022

    # Resume from specific month
    poetry run python scripts/seeds/weather_data/era5/backfill_historical.py \
        --start-year 2020 --start-month 6
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import argparse
import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

# Import the main fetch and process functions
from import_from_api import import_era5_data

logger = structlog.get_logger()


async def backfill_historical(
    start_year: int = 1995,
    end_year: int = None,
    start_month: int = 1,
    windfarm_ids: list = None
):
    """
    Backfill historical ERA5 weather data.

    Args:
        start_year: Year to start backfill (default: 1995)
        end_year: Year to end backfill (default: current year)
        start_month: Month to start in start_year (default: 1)
        windfarm_ids: Optional list of windfarm IDs
    """
    if end_year is None:
        end_year = datetime.now().year

    logger.info("="*70)
    logger.info("ERA5 HISTORICAL BACKFILL")
    logger.info("="*70)
    logger.info(f"Date range: {start_year}-{start_month:02d} to {end_year}-12")
    logger.info(f"Total years: {end_year - start_year + 1}")
    logger.info(f"Total months: {(end_year - start_year) * 12 + (12 - start_month + 1)}")

    total_months_processed = 0
    failed_months = []

    # Process year by year, month by month
    for year in range(start_year, end_year + 1):
        logger.info(f"\n{'='*70}")
        logger.info(f"YEAR {year}")
        logger.info(f"{'='*70}")

        # Determine month range for this year
        first_month = start_month if year == start_year else 1
        last_month = 12

        for month in range(first_month, last_month + 1):
            month_start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)

            # Last day of month
            if month == 12:
                month_end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
            else:
                next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
                month_end = next_month - timedelta(seconds=1)

            logger.info(f"\nMonth {month:02d}/{year}: {month_start.date()} to {month_end.date()}")

            try:
                # Fetch and process this month (day-by-day internally)
                await import_era5_data(
                    start_date=month_start,
                    end_date=month_end,
                    windfarm_ids=windfarm_ids,
                    dry_run=False
                )

                total_months_processed += 1
                logger.info(f"✓ Completed {year}-{month:02d}")

                # No sleep needed - import_era5_data now handles daily fetching
                # Days within a month don't need rate limiting between them

            except Exception as e:
                logger.error(f"✗ Failed {year}-{month:02d}: {e}")
                failed_months.append(f"{year}-{month:02d}")
                continue

        logger.info(f"\n✓ Year {year} complete")

    # Final summary
    logger.info(f"\n{'='*70}")
    logger.info("BACKFILL COMPLETE")
    logger.info(f"{'='*70}")
    logger.info(f"Months processed: {total_months_processed}")
    logger.info(f"Failed months: {len(failed_months)}")

    if failed_months:
        logger.warning(f"\nFailed months (retry manually):")
        for month_str in failed_months:
            logger.warning(f"  - {month_str}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Backfill historical ERA5 weather data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--start-year', type=int, default=1995, help='Start year (default: 1995)')
    parser.add_argument('--end-year', type=int, default=None, help='End year (default: current year)')
    parser.add_argument('--start-month', type=int, default=1, help='Start month (default: 1)')
    parser.add_argument('--windfarms', nargs='+', type=int, help='Windfarm IDs (optional)')

    args = parser.parse_args()

    # Validate
    if args.start_year < 1940 or args.start_year > 2025:
        print("Error: start-year must be between 1940 and 2025")
        return

    if args.start_month < 1 or args.start_month > 12:
        print("Error: start-month must be between 1 and 12")
        return

    # Run backfill
    asyncio.run(backfill_historical(
        start_year=args.start_year,
        end_year=args.end_year,
        start_month=args.start_month,
        windfarm_ids=args.windfarms
    ))


if __name__ == '__main__':
    main()
