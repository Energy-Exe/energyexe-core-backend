#!/usr/bin/env python3
"""
Fetch ERA5 Weather Data - Single Call Per Day with Bilinear Interpolation

Fetches ERA5 data once per day for all windfarms using a single API call.
Uses bilinear interpolation for each windfarm's exact location for better accuracy.

Usage:
    # Fetch single day
    poetry run python scripts/seeds/weather_data/fetch_daily_all_windfarms.py --date 2024-01-15

    # Fetch date range
    poetry run python scripts/seeds/weather_data/fetch_daily_all_windfarms.py \
        --start 2024-01-01 --end 2024-01-07

    # Dry run
    poetry run python scripts/seeds/weather_data/fetch_daily_all_windfarms.py \
        --date 2024-01-15 --dry-run
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import argparse
import structlog
import cdsapi
import xarray as xr
import pandas as pd
import math

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.windfarm import Windfarm
from app.models.weather_data import WeatherDataRaw, WeatherData
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()


def calculate_wind_metrics(u100, v100):
    """Calculate wind speed and direction."""
    wind_speed = math.sqrt(u100**2 + v100**2)
    math_angle = math.atan2(v100, u100)
    wind_direction = (270 - math.degrees(math_angle)) % 360
    return wind_speed, wind_direction


async def get_all_windfarms():
    """Get all windfarms with coordinates."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        query = select(Windfarm).where(
            Windfarm.lat.isnot(None),
            Windfarm.lng.isnot(None)
        ).order_by(Windfarm.id)

        result = await db.execute(query)
        return result.scalars().all()


async def fetch_era5_for_day(date: datetime):
    """
    Fetch ERA5 data for one day covering all windfarms.

    Args:
        date: Date to fetch

    Returns:
        Path to GRIB file
    """
    logger.info(f"Fetching ERA5 for all windfarms", date=date.strftime('%Y-%m-%d'))

    # Get windfarm bounds
    windfarms = await get_all_windfarms()
    lats = [float(wf.lat) for wf in windfarms]
    lons = [float(wf.lng) for wf in windfarms]

    # Create bounding box covering all windfarms
    bbox = [
        max(lats) + 0.5,  # North (add buffer for interpolation)
        min(lons) - 0.5,  # West
        min(lats) - 0.5,  # South
        max(lons) + 0.5   # East
    ]

    logger.info(f"Bounding box: N={bbox[0]:.1f}, W={bbox[1]:.1f}, S={bbox[2]:.1f}, E={bbox[3]:.1f}")

    c = cdsapi.Client()

    request = {
        'product_type': 'reanalysis',
        'format': 'grib',
        'variable': [
            '100m_u_component_of_wind',
            '100m_v_component_of_wind',
            '2m_temperature',
            'surface_pressure',
        ],
        'year': str(date.year),
        'month': f'{date.month:02d}',
        'day': f'{date.day:02d}',
        'time': [f'{h:02d}:00' for h in range(24)],
        'area': bbox,
    }

    # Create GRIB directory
    grib_dir = Path(__file__).parent.parent.parent.parent / 'grib_files' / 'daily'
    grib_dir.mkdir(parents=True, exist_ok=True)

    output_file = str(grib_dir / f'era5_{date.strftime("%Y%m%d")}.grib')

    logger.info(f"Downloading to {output_file}")

    c.retrieve('reanalysis-era5-single-levels', request, output_file)

    file_size = os.path.getsize(output_file)
    logger.info(f"Downloaded", file_size_mb=round(file_size/1024/1024, 1))

    return output_file


def bilinear_interpolate_all_times(ds, lat, lon, var_name):
    """
    Bilinear interpolation for all time points at once (OPTIMIZED).

    Args:
        ds: xarray Dataset
        lat: Target latitude
        lon: Target longitude
        var_name: Variable name

    Returns:
        Array of values for all time points
    """
    # Interpolate once for all times (much faster!)
    interpolated = ds[var_name].interp(
        latitude=lat,
        longitude=lon,
        method='linear'
    )

    # Return all time values
    return interpolated.values


def extract_windfarm_data_with_interpolation(ds, windfarms):
    """
    Extract data for all windfarms using bilinear interpolation.

    Args:
        ds: xarray Dataset with ERA5 data
        windfarms: List of Windfarm objects

    Returns:
        (raw_records, processed_records) for database insertion
    """
    logger.info(f"Extracting data for {len(windfarms)} windfarms with bilinear interpolation")

    raw_records = []
    processed_records = []

    for i, wf in enumerate(windfarms):
        if i % 100 == 0:
            logger.info(f"Processing windfarm {i+1}/{len(windfarms)}")

        wf_lat = float(wf.lat)
        wf_lng = float(wf.lng)

        try:
            # Interpolate ALL time points at once (MUCH faster!)
            u100_all_times = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 'u100')
            v100_all_times = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 'v100')
            t2m_all_times = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 't2m')

            # Extract all time points for this windfarm
            num_times = len(ds.time)
            for time_idx in range(num_times):
                try:
                    # ERA5 timestamps are in UTC - must explicitly specify to avoid local timezone conversion
                    timestamp = pd.Timestamp(ds.time.values[time_idx], tz='UTC').to_pydatetime()

                    u100 = float(u100_all_times[time_idx])
                    v100 = float(v100_all_times[time_idx])
                    t2m = float(t2m_all_times[time_idx])

                    # Calculate wind metrics
                    wind_speed, wind_direction = calculate_wind_metrics(u100, v100)

                    # Create processed record (direct to weather_data)
                    processed_records.append({
                        'hour': timestamp,
                        'windfarm_id': wf.id,
                        'wind_speed_100m': round(wind_speed, 3),
                        'wind_direction_deg': round(wind_direction, 2),
                        'temperature_2m_k': round(t2m, 2),
                        'temperature_2m_c': round(t2m - 273.15, 2),
                        'source': 'ERA5',
                        'raw_data_id': None,
                    })
                except Exception as time_error:
                    logger.error(f"Failed to extract hour {time_idx} for windfarm {wf.id}: {time_error}")
                    continue

        except Exception as e:
            logger.warning(f"Failed to interpolate for windfarm {wf.id}: {e}")
            continue

    logger.info(f"Extracted {len(processed_records)} records")

    return raw_records, processed_records


async def bulk_insert_processed(records: List[dict]):
    """Bulk insert processed data."""
    if not records:
        return

    AsyncSessionLocal = get_session_factory()
    BATCH_SIZE = 2900

    async with AsyncSessionLocal() as db:
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1

            logger.info(f"Inserting batch {batch_num}/{total_batches}: {len(batch)} records")

            stmt = insert(WeatherData).values(batch)

            stmt = stmt.on_conflict_do_update(
                constraint='uq_weather_hour_windfarm_source',
                set_={
                    'wind_speed_100m': stmt.excluded.wind_speed_100m,
                    'wind_direction_deg': stmt.excluded.wind_direction_deg,
                    'temperature_2m_k': stmt.excluded.temperature_2m_k,
                    'temperature_2m_c': stmt.excluded.temperature_2m_c,
                    'updated_at': datetime.utcnow(),
                }
            )

            await db.execute(stmt)

        await db.commit()

        logger.info(f"Bulk insert complete", total=len(records), batches=total_batches)


async def check_day_complete(date: datetime) -> bool:
    """
    Check if a day already has complete data in the database.

    Args:
        date: Date to check

    Returns:
        True if day has 38,184 records (1,591 windfarms × 24 hours)
    """
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        from sqlalchemy import text
        query = text("""
            SELECT COUNT(*)
            FROM weather_data
            WHERE DATE(hour) = :date
        """)
        result = await db.execute(query, {"date": date.date()})
        count = result.scalar()

        expected = 38184  # 1,591 windfarms × 24 hours
        is_complete = count >= expected

        if is_complete:
            logger.info(f"Day already complete: {date.strftime('%Y-%m-%d')} ({count} records)")

        return is_complete


def cleanup_grib_file(grib_path: Path) -> bool:
    """
    Delete GRIB file after processing to save disk space.

    Args:
        grib_path: Path to GRIB file

    Returns:
        True if deleted successfully, False otherwise
    """
    try:
        if grib_path.exists():
            grib_path.unlink()
            logger.info(f"Deleted GRIB file: {grib_path.name}")
            print("FILES_DELETED: 1")  # For parsing by service layer
            return True
    except Exception as e:
        logger.warning(f"Failed to delete GRIB file: {e}")
    return False


async def update_job_progress(
    job_id: int,
    completed_date: datetime,
    records_count: int
):
    """
    Update weather import job progress in database.

    Args:
        job_id: Weather import job ID
        completed_date: Date that was just completed
        records_count: Number of records processed
    """
    try:
        from app.core.database import get_session_factory
        from app.models.weather_import_job import WeatherImportJob

        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            job = await db.get(WeatherImportJob, job_id)
            if job and job.job_metadata:
                current_completed = job.job_metadata.get('dates_completed', 0)
                job.update_progress(
                    dates_completed=current_completed + 1,
                    current_date=completed_date.strftime('%Y-%m-%d'),
                    current_phase='processing',
                    records_processed=job.records_imported + records_count
                )
                await db.commit()
                logger.info(f"Updated job progress", job_id=job_id, dates_completed=current_completed + 1)
    except Exception as e:
        logger.warning(f"Failed to update job progress: {e}")


async def process_single_day(date: datetime, dry_run: bool = False, job_id: Optional[int] = None):
    """
    Fetch and process ERA5 data for one day for all windfarms.

    Args:
        date: Date to process
        dry_run: If True, only show what would be done
        job_id: Optional weather import job ID for progress tracking
    """
    logger.info("="*60)
    logger.info("FETCH DAILY ALL WINDFARMS (Bilinear Interpolation)")
    logger.info("="*60)
    logger.info(f"Date: {date.strftime('%Y-%m-%d')}")

    # Check if day is already complete
    if await check_day_complete(date):
        logger.info(f"Skipping {date.strftime('%Y-%m-%d')} - already complete")
        print(f"PROGRESS: Date {date.strftime('%Y-%m-%d')} skipped (already complete)")
        return

    # Get all windfarms
    windfarms = await get_all_windfarms()
    logger.info(f"Total windfarms: {len(windfarms)}")

    if dry_run:
        logger.info("DRY RUN - No data will be fetched")
        return

    # Check if GRIB already exists
    grib_dir = Path(__file__).parent.parent.parent.parent / 'grib_files' / 'daily'
    expected_grib = grib_dir / f'era5_{date.strftime("%Y%m%d")}.grib'

    downloaded_new = False
    if expected_grib.exists():
        logger.info(f"GRIB file already exists: {expected_grib}")
        logger.info("Using existing file for processing")
        grib_file = str(expected_grib)
        print("FILES_DOWNLOADED: 0")  # Reused existing file
    else:
        # Fetch ERA5 data (single call for all windfarms)
        grib_file = await fetch_era5_for_day(date)
        downloaded_new = True
        print("FILES_DOWNLOADED: 1")
        print("API_CALLS: 1")

    # Parse and interpolate
    logger.info("Parsing GRIB and interpolating for each windfarm...")

    ds = xr.open_dataset(grib_file, engine='cfgrib')

    logger.info(f"GRIB grid size: {len(ds.latitude)} × {len(ds.longitude)} = {len(ds.latitude) * len(ds.longitude)} points")
    logger.info(f"Time points: {len(ds.time)}")

    # Extract with bilinear interpolation
    _, processed_records = extract_windfarm_data_with_interpolation(ds, windfarms)

    ds.close()

    # Insert to database
    await bulk_insert_processed(processed_records)

    # Print structured output for parsing
    print(f"RECORDS: {len(processed_records)}")

    # Cleanup GRIB file to save disk space
    cleanup_grib_file(Path(grib_file))

    # Update job progress if job_id provided
    if job_id:
        await update_job_progress(job_id, date, len(processed_records))

    logger.info("="*60)
    logger.info("COMPLETE")
    logger.info("="*60)
    logger.info(f"Date: {date.strftime('%Y-%m-%d')}")
    logger.info(f"Windfarms: {len(windfarms)}")
    logger.info(f"Records: {len(processed_records)}")
    logger.info(f"GRIB file: {grib_file} (deleted)")

    # Print completion marker for parsing
    print(f"PROGRESS: Date {date.strftime('%Y-%m-%d')} completed")


async def process_date_range(start_date: datetime, end_date: datetime, dry_run: bool = False, job_id: Optional[int] = None):
    """Process multiple days."""
    current = start_date

    days = []
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)

    logger.info(f"Processing {len(days)} days from {start_date.date()} to {end_date.date()}")

    for i, day in enumerate(days, 1):
        logger.info(f"\nDay {i}/{len(days)}: {day.strftime('%Y-%m-%d')}")
        await process_single_day(day, dry_run, job_id)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Fetch ERA5 daily for all windfarms with bilinear interpolation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--date', help='Single date (YYYY-MM-DD)')
    group.add_argument('--start', help='Start date (YYYY-MM-DD)')

    parser.add_argument('--end', help='End date (YYYY-MM-DD), requires --start')
    parser.add_argument('--dry-run', action='store_true', help='Dry run')
    parser.add_argument('--job-id', type=int, help='Weather import job ID for progress tracking')

    args = parser.parse_args()

    if args.date:
        date = datetime.fromisoformat(args.date).replace(tzinfo=timezone.utc)
        asyncio.run(process_single_day(date, args.dry_run, args.job_id))
    elif args.start:
        if not args.end:
            print("Error: --start requires --end")
            sys.exit(1)
        start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        asyncio.run(process_date_range(start, end, args.dry_run, args.job_id))


if __name__ == '__main__':
    main()
