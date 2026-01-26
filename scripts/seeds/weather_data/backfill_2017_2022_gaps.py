#!/usr/bin/env python3
"""
Backfill missing weather data gaps for 2017-2022.
Specific dates identified from coverage analysis.
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List
import structlog
import cdsapi
import xarray as xr
import math

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.windfarm import Windfarm
from app.models.weather_data import WeatherData
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

logger = structlog.get_logger()

# Missing dates identified from coverage analysis
MISSING_DATES = {
    2017: [
        "2017-11-16",
        "2017-11-29", "2017-11-30",
        "2017-12-04", "2017-12-05", "2017-12-06",
        "2017-12-10", "2017-12-11",
        "2017-12-13", "2017-12-14", "2017-12-15", "2017-12-16", "2017-12-17",
        "2017-12-19", "2017-12-20", "2017-12-21", "2017-12-22", "2017-12-23",
        "2017-12-25", "2017-12-26", "2017-12-27", "2017-12-28", "2017-12-29",
        "2017-12-31",
    ],
    2018: [
        "2018-11-26",
        "2018-11-30",
        "2018-12-02",
        "2018-12-04", "2018-12-05", "2018-12-06",
        "2018-12-09",
        "2018-12-11", "2018-12-12", "2018-12-13", "2018-12-14", "2018-12-15", "2018-12-16",
        "2018-12-18", "2018-12-19", "2018-12-20", "2018-12-21",
        "2018-12-23", "2018-12-24",
        "2018-12-27",
    ],
    2019: [
        "2019-11-28",
        "2019-12-03", "2019-12-04", "2019-12-05", "2019-12-06",
        "2019-12-10", "2019-12-11", "2019-12-12",
        "2019-12-14", "2019-12-15", "2019-12-16", "2019-12-17", "2019-12-18",
        "2019-12-21", "2019-12-22",
        "2019-12-24", "2019-12-25", "2019-12-26", "2019-12-27", "2019-12-28", "2019-12-29",
        "2019-12-31",
    ],
    2020: [
        "2020-03-05",
        "2020-11-18", "2020-11-19",
        "2020-11-21",
        "2020-11-28",
        "2020-12-01", "2020-12-02", "2020-12-03", "2020-12-04", "2020-12-05", "2020-12-06",
        "2020-12-08", "2020-12-09", "2020-12-10", "2020-12-11", "2020-12-12",
        "2020-12-15", "2020-12-16", "2020-12-17",
        "2020-12-19", "2020-12-20", "2020-12-21", "2020-12-22", "2020-12-23", "2020-12-24",
        "2020-12-26", "2020-12-27", "2020-12-28", "2020-12-29", "2020-12-30", "2020-12-31",
    ],
    2021: [
        "2021-01-03", "2021-01-18", "2021-01-29",
        "2021-02-05",
        "2021-03-01", "2021-03-15", "2021-03-19", "2021-03-24", "2021-03-29",
        "2021-04-05", "2021-04-18", "2021-04-24", "2021-04-27", "2021-04-30",
        "2021-05-10", "2021-05-22", "2021-05-26", "2021-05-30",
        "2021-06-26",
        "2021-07-23", "2021-07-29",
        "2021-08-10", "2021-08-21",
        "2021-09-02", "2021-09-23",
        "2021-10-23", "2021-10-27", "2021-10-29",
        "2021-11-07", "2021-11-18", "2021-11-21", "2021-11-24", "2021-11-27",
        "2021-12-08", "2021-12-11", "2021-12-14", "2021-12-17", "2021-12-20", "2021-12-23", "2021-12-26",
    ],
    2022: [
        "2022-01-02", "2022-01-26",
        "2022-02-07", "2022-02-14",
        "2022-03-01", "2022-03-07", "2022-03-14", "2022-03-28",
        "2022-04-04", "2022-04-27", "2022-04-30",
        "2022-05-03", "2022-05-10", "2022-05-13", "2022-05-22", "2022-05-26",
        "2022-06-02", "2022-06-16", "2022-06-21",
        "2022-07-01", "2022-07-13", "2022-07-19", "2022-07-28",
        "2022-08-26", "2022-08-31",
        "2022-09-06", "2022-09-18", "2022-09-26",
        "2022-10-13", "2022-10-20", "2022-10-23", "2022-10-30",
        "2022-11-21", "2022-11-24", "2022-11-27", "2022-11-30",
        "2022-12-11", "2022-12-14", "2022-12-21", "2022-12-24", "2022-12-27", "2022-12-29", "2022-12-31",
    ],
}


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


async def fetch_era5_for_day(date: datetime, windfarms):
    """Fetch ERA5 data for one day."""
    logger.info(f"Fetching ERA5 for {date.strftime('%Y-%m-%d')}")

    lats = [float(wf.lat) for wf in windfarms]
    lons = [float(wf.lng) for wf in windfarms]

    bbox = [
        max(lats) + 0.5,
        min(lons) - 0.5,
        min(lats) - 0.5,
        max(lons) + 0.5
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

    grib_dir = Path(__file__).parent.parent.parent.parent / 'grib_files' / 'daily'
    grib_dir.mkdir(parents=True, exist_ok=True)
    output_file = str(grib_dir / f'era5_{date.strftime("%Y%m%d")}.grib')

    logger.info(f"Downloading to {output_file}")
    c.retrieve('reanalysis-era5-single-levels', request, output_file)

    file_size = os.path.getsize(output_file)
    logger.info(f"Downloaded {file_size/1024/1024:.1f} MB")

    return output_file


def bilinear_interpolate_all_times(ds, lat, lon, var_name):
    """Bilinear interpolation for all time points."""
    interpolated = ds[var_name].interp(
        latitude=lat,
        longitude=lon,
        method='linear'
    )
    return interpolated.values


def extract_windfarm_data(ds, windfarms):
    """Extract data for all windfarms using bilinear interpolation."""
    logger.info(f"Extracting data for {len(windfarms)} windfarms")

    processed_records = []

    for i, wf in enumerate(windfarms):
        if i % 200 == 0:
            logger.info(f"Processing windfarm {i+1}/{len(windfarms)}")

        wf_lat = float(wf.lat)
        wf_lng = float(wf.lng)

        try:
            u100_all_times = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 'u100')
            v100_all_times = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 'v100')
            t2m_all_times = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 't2m')

            num_times = len(ds.time)
            for time_idx in range(num_times):
                try:
                    timestamp = pd.Timestamp(ds.time.values[time_idx], tz='UTC').to_pydatetime()

                    u100 = float(u100_all_times[time_idx])
                    v100 = float(v100_all_times[time_idx])
                    t2m = float(t2m_all_times[time_idx])

                    wind_speed, wind_direction = calculate_wind_metrics(u100, v100)

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
                    logger.error(f"Failed hour {time_idx} for windfarm {wf.id}: {time_error}")
                    continue

        except Exception as e:
            logger.warning(f"Failed windfarm {wf.id}: {e}")
            continue

    logger.info(f"Extracted {len(processed_records)} records")
    return processed_records


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
        logger.info(f"Bulk insert complete: {len(records)} records")


async def process_single_day(date: datetime, windfarms):
    """Process a single day."""
    logger.info("="*60)
    logger.info(f"Processing {date.strftime('%Y-%m-%d')}")
    logger.info("="*60)

    # Check if GRIB already exists
    grib_dir = Path(__file__).parent.parent.parent.parent / 'grib_files' / 'daily'
    expected_grib = grib_dir / f'era5_{date.strftime("%Y%m%d")}.grib'

    if expected_grib.exists():
        logger.info(f"Using existing GRIB: {expected_grib}")
        grib_file = str(expected_grib)
    else:
        grib_file = await fetch_era5_for_day(date, windfarms)

    # Parse and interpolate
    logger.info("Parsing GRIB and interpolating...")
    ds = xr.open_dataset(grib_file, engine='cfgrib')

    logger.info(f"Grid: {len(ds.latitude)} × {len(ds.longitude)}, Time points: {len(ds.time)}")

    processed_records = extract_windfarm_data(ds, windfarms)
    ds.close()

    # Insert to database
    await bulk_insert_processed(processed_records)

    # Cleanup GRIB file
    if expected_grib.exists():
        expected_grib.unlink()
        logger.info("Deleted GRIB file")

    logger.info(f"✓ Completed {date.strftime('%Y-%m-%d')}: {len(processed_records)} records")
    return len(processed_records)


async def main():
    """Backfill 2017-2022 gaps."""
    logger.info("="*60)
    logger.info("BACKFILL 2017-2022 WEATHER DATA GAPS")
    logger.info("="*60)

    # Collect all missing dates
    all_dates = []
    for year, dates in MISSING_DATES.items():
        for date_str in dates:
            all_dates.append(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc))

    all_dates.sort()
    total_days = len(all_dates)

    logger.info(f"Total missing dates to backfill: {total_days}")
    for year in sorted(MISSING_DATES.keys()):
        logger.info(f"  {year}: {len(MISSING_DATES[year])} days")

    # Get windfarms once
    logger.info("\nLoading windfarms...")
    windfarms = await get_all_windfarms()
    logger.info(f"Found {len(windfarms)} windfarms")

    # Process each missing day
    day_num = 0
    total_records = 0
    failed_dates = []

    for date in all_dates:
        day_num += 1
        logger.info(f"\nDay {day_num}/{total_days}")

        try:
            records = await process_single_day(date, windfarms)
            total_records += records
        except Exception as e:
            logger.error(f"Failed {date.strftime('%Y-%m-%d')}: {e}")
            failed_dates.append(date.strftime('%Y-%m-%d'))

    logger.info("="*60)
    logger.info("BACKFILL COMPLETE")
    logger.info("="*60)
    logger.info(f"Days processed: {day_num - len(failed_dates)}/{total_days}")
    logger.info(f"Total records: {total_records}")

    if failed_dates:
        logger.warning(f"Failed dates ({len(failed_dates)}):")
        for d in failed_dates:
            logger.warning(f"  - {d}")


if __name__ == '__main__':
    asyncio.run(main())
