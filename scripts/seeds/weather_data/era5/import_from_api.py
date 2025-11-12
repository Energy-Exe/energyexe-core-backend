#!/usr/bin/env python3
"""
ERA5 Weather Data Import Script

Fetches weather data from ERA5 Copernicus API and stores in weather_data_raw table.

Usage:
    # Fetch single day
    poetry run python scripts/seeds/weather_data/era5/import_from_api.py \
        --start 2025-01-15 --end 2025-01-15

    # Fetch month
    poetry run python scripts/seeds/weather_data/era5/import_from_api.py \
        --start 2025-01-01 --end 2025-01-31

    # Fetch specific windfarms
    poetry run python scripts/seeds/weather_data/era5/import_from_api.py \
        --start 2025-01-01 --end 2025-01-31 --windfarms 1 2 3

    # Dry run (see what would be fetched)
    poetry run python scripts/seeds/weather_data/era5/import_from_api.py \
        --start 2025-01-15 --end 2025-01-15 --dry-run
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import argparse
import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.windfarm import Windfarm
from app.models.weather_data import WeatherDataRaw
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
import cdsapi

from helpers import (
    get_windfarm_coordinates,
    create_bounding_box,
    create_daily_chunks,
)
from parse_grib import parse_grib_file

logger = structlog.get_logger()


async def get_active_windfarms(windfarm_ids: Optional[List[int]] = None):
    """Get active windfarms from database."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        query = select(Windfarm).where(
            Windfarm.lat.isnot(None),
            Windfarm.lng.isnot(None)
        )

        if windfarm_ids:
            query = query.where(Windfarm.id.in_(windfarm_ids))

        result = await db.execute(query)
        windfarms = result.scalars().all()

        logger.info(f"Found {len(windfarms)} windfarms with coordinates")
        return windfarms


def fetch_era5_daily(
    coordinates: List[tuple[float, float, int]],
    date: datetime
) -> str:
    """
    Fetch ERA5 data for all windfarm coordinates for one day.

    Args:
        coordinates: List of (lat, lon, windfarm_id) tuples
        date: Date to fetch

    Returns:
        Path to downloaded GRIB file
    """
    logger.info(f"Fetching ERA5 data for {date.date()}")
    logger.info(f"Coordinates: {len(coordinates)} windfarms")

    # Create bounding box
    bbox = create_bounding_box(coordinates)
    bbox_size = (bbox['north'] - bbox['south']) * (bbox['east'] - bbox['west'])
    logger.info(f"Bounding box: N={bbox['north']:.2f}, S={bbox['south']:.2f}, E={bbox['east']:.2f}, W={bbox['west']:.2f}")
    logger.info(f"Bounding box area: {bbox_size:.1f} square degrees")

    # Initialize CDS API client
    c = cdsapi.Client()

    # Build request for single day
    request = {
        'product_type': 'reanalysis',
        'format': 'grib',
        'variable': [
            '100m_u_component_of_wind',
            '100m_v_component_of_wind',
            '2m_temperature',
            # Optional - store for future use:
            'surface_pressure',
        ],
        'year': str(date.year),
        'month': f'{date.month:02d}',
        'day': f'{date.day:02d}',
        'time': [f'{h:02d}:00' for h in range(24)],  # All 24 hours
        'area': [bbox['north'], bbox['west'], bbox['south'], bbox['east']],  # [N, W, S, E]
    }

    output_file = f'/tmp/era5_{date.strftime("%Y%m%d")}.grib'

    logger.info(f"Downloading to: {output_file}")
    logger.info("This may take 2-5 minutes...")

    try:
        c.retrieve('reanalysis-era5-single-levels', request, output_file)
        logger.info(f"Download complete: {output_file}")

        # Check file size
        file_size = os.path.getsize(output_file)
        logger.info(f"File size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")

        return output_file

    except Exception as e:
        logger.error(f"ERA5 API error: {e}")
        raise


async def bulk_insert_weather_raw(records: List[Dict]):
    """
    Bulk insert raw weather data with conflict resolution.

    Batches inserts to avoid PostgreSQL parameter limit (32,767).

    Args:
        records: List of record dicts to insert
    """
    if not records:
        logger.warning("No records to insert")
        return

    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # PostgreSQL parameter limit: 32,767
        # Each record has 8 columns, so max ~4000 records per batch
        BATCH_SIZE = 4000

        total_inserted = 0

        # Split into batches
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

            logger.info(f"Inserting batch {batch_num}/{total_batches}: {len(batch)} records")

            stmt = insert(WeatherDataRaw).values(batch)

            # On conflict (duplicate timestamp + location), update data
            stmt = stmt.on_conflict_do_update(
                constraint='uq_weather_raw_grid_time',
                set_={
                    'data': stmt.excluded.data,
                    'updated_at': datetime.utcnow(),
                }
            )

            await db.execute(stmt)
            total_inserted += len(batch)

        await db.commit()

        logger.info(f"Successfully inserted/updated {total_inserted} raw weather records")


async def import_era5_data(
    start_date: datetime,
    end_date: datetime,
    windfarm_ids: Optional[List[int]] = None,
    dry_run: bool = False
):
    """
    Main function to import ERA5 data for date range (day-by-day).

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        windfarm_ids: Optional list of windfarm IDs to fetch
        dry_run: If True, only show what would be fetched
    """
    logger.info("="*60)
    logger.info("ERA5 WEATHER DATA IMPORT (Day-by-Day)")
    logger.info("="*60)
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")

    # 1. Get windfarms
    windfarms = await get_active_windfarms(windfarm_ids)

    if not windfarms:
        logger.error("No windfarms found")
        return

    # 2. Get exact coordinates
    coordinates = get_windfarm_coordinates(windfarms)
    logger.info(f"Windfarms: {len(coordinates)}")

    for lat, lon, wf_id in coordinates[:5]:  # Show first 5
        logger.info(f"  - Windfarm {wf_id}: ({lat:.6f}, {lon:.6f})")
    if len(coordinates) > 5:
        logger.info(f"  ... and {len(coordinates) - 5} more")

    # 3. Create daily chunks
    daily_dates = create_daily_chunks(start_date, end_date)
    logger.info(f"Days to fetch: {len(daily_dates)}")

    if dry_run:
        logger.info("DRY RUN - No data will be fetched")
        for i, day in enumerate(daily_dates[:7]):  # Show first 7 days
            logger.info(f"  Would fetch: {day.strftime('%Y-%m-%d')}")
        if len(daily_dates) > 7:
            logger.info(f"  ... and {len(daily_dates) - 7} more days")
        return

    # 4. Fetch and process each day
    total_records = 0
    failed_days = []

    for i, day in enumerate(daily_dates, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"Day {i}/{len(daily_dates)}: {day.strftime('%Y-%m-%d')}")
        logger.info(f"{'='*60}")

        try:
            # Fetch GRIB file for this day
            grib_file = fetch_era5_daily(coordinates, day)

            # Parse GRIB file
            records = parse_grib_file(grib_file, coordinates)

            # Bulk insert
            await bulk_insert_weather_raw(records)

            total_records += len(records)

            # Cleanup GRIB file
            if os.path.exists(grib_file):
                os.remove(grib_file)
                logger.info(f"Cleaned up GRIB file: {grib_file}")

            logger.info(f"✓ Day {i}/{len(daily_dates)} complete: {len(records)} records")

        except Exception as e:
            logger.error(f"Failed to process {day.strftime('%Y-%m-%d')}: {e}")
            failed_days.append(day.strftime('%Y-%m-%d'))
            continue

    logger.info(f"\n{'='*60}")
    logger.info("IMPORT COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"Total days processed: {len(daily_dates) - len(failed_days)}/{len(daily_dates)}")
    logger.info(f"Total records inserted: {total_records}")

    if failed_days:
        logger.warning(f"\nFailed days ({len(failed_days)}):")
        for day_str in failed_days:
            logger.warning(f"  - {day_str}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Import ERA5 weather data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--windfarms', nargs='+', type=int, help='Windfarm IDs (optional)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be fetched without fetching')

    args = parser.parse_args()

    # Parse dates
    start = datetime.fromisoformat(args.start).replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)

    # Run import
    asyncio.run(import_era5_data(start, end, args.windfarms, args.dry_run))


if __name__ == '__main__':
    main()
