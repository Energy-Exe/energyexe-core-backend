#!/usr/bin/env python3
"""
Backfill missing weather data for 2014 only.
"""

import asyncio
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Set
import structlog
import cdsapi
import xarray as xr
import math

current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.windfarm import Windfarm
from app.models.weather_data import WeatherData
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

logger = structlog.get_logger()

YEAR = 2014


def calculate_wind_metrics(u100, v100):
    wind_speed = math.sqrt(u100**2 + v100**2)
    math_angle = math.atan2(v100, u100)
    wind_direction = (270 - math.degrees(math_angle)) % 360
    return wind_speed, wind_direction


async def get_all_windfarms():
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        query = select(Windfarm).where(
            Windfarm.lat.isnot(None),
            Windfarm.lng.isnot(None)
        ).order_by(Windfarm.id)
        result = await db.execute(query)
        return result.scalars().all()


async def get_existing_dates() -> Set:
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        result = await db.execute(text(f"""
            WITH sample_farm AS (
                SELECT id FROM windfarms LIMIT 1
            )
            SELECT DISTINCT DATE(hour) as d
            FROM weather_data wd
            JOIN sample_farm sf ON wd.windfarm_id = sf.id
            WHERE hour >= '{YEAR}-01-01' AND hour < '{YEAR + 1}-01-01'
            ORDER BY d
        """))
        return {row.d for row in result}


async def fetch_era5_for_day(date: datetime, windfarms):
    logger.info(f"Fetching ERA5 for {date.strftime('%Y-%m-%d')}")

    lats = [float(wf.lat) for wf in windfarms]
    lons = [float(wf.lng) for wf in windfarms]

    bbox = [max(lats) + 0.5, min(lons) - 0.5, min(lats) - 0.5, max(lons) + 0.5]

    c = cdsapi.Client()

    request = {
        'product_type': 'reanalysis',
        'format': 'grib',
        'variable': ['100m_u_component_of_wind', '100m_v_component_of_wind', '2m_temperature', 'surface_pressure'],
        'year': str(date.year),
        'month': f'{date.month:02d}',
        'day': f'{date.day:02d}',
        'time': [f'{h:02d}:00' for h in range(24)],
        'area': bbox,
    }

    grib_dir = Path(__file__).parent.parent.parent.parent / 'grib_files' / f'daily_{YEAR}'
    grib_dir.mkdir(parents=True, exist_ok=True)
    output_file = str(grib_dir / f'era5_{date.strftime("%Y%m%d")}.grib')

    logger.info(f"Downloading to {output_file}")
    c.retrieve('reanalysis-era5-single-levels', request, output_file)

    file_size = os.path.getsize(output_file)
    logger.info(f"Downloaded {file_size/1024/1024:.1f} MB")

    return output_file


def bilinear_interpolate_all_times(ds, lat, lon, var_name):
    interpolated = ds[var_name].interp(latitude=lat, longitude=lon, method='linear')
    return interpolated.values


def extract_windfarm_data(ds, windfarms):
    logger.info(f"Extracting data for {len(windfarms)} windfarms")
    processed_records = []

    for i, wf in enumerate(windfarms):
        if i % 200 == 0:
            logger.info(f"Processing windfarm {i+1}/{len(windfarms)}")

        wf_lat, wf_lng = float(wf.lat), float(wf.lng)

        try:
            u100_all = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 'u100')
            v100_all = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 'v100')
            t2m_all = bilinear_interpolate_all_times(ds, wf_lat, wf_lng, 't2m')

            for time_idx in range(len(ds.time)):
                try:
                    timestamp = pd.Timestamp(ds.time.values[time_idx], tz='UTC').to_pydatetime()
                    u100, v100, t2m = float(u100_all[time_idx]), float(v100_all[time_idx]), float(t2m_all[time_idx])
                    wind_speed, wind_direction = calculate_wind_metrics(u100, v100)

                    processed_records.append({
                        'hour': timestamp, 'windfarm_id': wf.id,
                        'wind_speed_100m': round(wind_speed, 3), 'wind_direction_deg': round(wind_direction, 2),
                        'temperature_2m_k': round(t2m, 2), 'temperature_2m_c': round(t2m - 273.15, 2),
                        'source': 'ERA5', 'raw_data_id': None,
                    })
                except Exception as e:
                    continue
        except Exception as e:
            continue

    logger.info(f"Extracted {len(processed_records)} records")
    return processed_records


async def bulk_insert_processed(records: List[dict]):
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
                set_={'wind_speed_100m': stmt.excluded.wind_speed_100m, 'wind_direction_deg': stmt.excluded.wind_direction_deg,
                      'temperature_2m_k': stmt.excluded.temperature_2m_k, 'temperature_2m_c': stmt.excluded.temperature_2m_c,
                      'updated_at': datetime.utcnow()}
            )
            await db.execute(stmt)

        await db.commit()
        logger.info(f"Bulk insert complete: {len(records)} records")


async def process_single_day(date: datetime, windfarms):
    logger.info("="*60)
    logger.info(f"Processing {date.strftime('%Y-%m-%d')}")
    logger.info("="*60)

    grib_dir = Path(__file__).parent.parent.parent.parent / 'grib_files' / f'daily_{YEAR}'
    expected_grib = grib_dir / f'era5_{date.strftime("%Y%m%d")}.grib'

    if expected_grib.exists():
        grib_file = str(expected_grib)
    else:
        grib_file = await fetch_era5_for_day(date, windfarms)

    ds = xr.open_dataset(grib_file, engine='cfgrib')
    processed_records = extract_windfarm_data(ds, windfarms)
    ds.close()

    await bulk_insert_processed(processed_records)

    if expected_grib.exists():
        expected_grib.unlink()

    logger.info(f"✓ Completed {date.strftime('%Y-%m-%d')}: {len(processed_records)} records")
    return len(processed_records)


async def main():
    logger.info("="*60)
    logger.info(f"BACKFILL {YEAR} WEATHER DATA")
    logger.info("="*60)

    existing_dates = await get_existing_dates()
    logger.info(f"Found {len(existing_dates)} days already with data")

    all_dates = []
    current = datetime(YEAR, 1, 1, tzinfo=timezone.utc)
    end = datetime(YEAR, 12, 31, tzinfo=timezone.utc)
    while current <= end:
        if current.date() not in existing_dates:
            all_dates.append(current)
        current += timedelta(days=1)

    total_days = len(all_dates)
    logger.info(f"Missing dates to backfill: {total_days}")

    if total_days == 0:
        logger.info(f"{YEAR} is complete!")
        return

    windfarms = await get_all_windfarms()
    logger.info(f"Found {len(windfarms)} windfarms")

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


if __name__ == '__main__':
    asyncio.run(main())
