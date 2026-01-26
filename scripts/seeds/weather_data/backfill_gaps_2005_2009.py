"""
Backfill all missing weather data gaps for 2005-2009
"""
import asyncio
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
import structlog
import cdsapi
import xarray as xr
import math
import pandas as pd
from app.core.database import get_session_factory
from app.models.windfarm import Windfarm
from app.models.weather_data import WeatherData
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()

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

async def get_all_missing_dates():
    """Get all missing dates across 2005-2009"""
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        result = await db.execute(text('''
            WITH sample_farm AS (
                SELECT id FROM windfarms LIMIT 1
            )
            SELECT DISTINCT DATE(hour) as d
            FROM weather_data wd
            JOIN sample_farm sf ON wd.windfarm_id = sf.id
            WHERE hour >= '2005-01-01' AND hour < '2010-01-01'
            ORDER BY d
        '''))
        existing = {row.d for row in result}

    # Find all missing dates
    missing = []
    for year in range(2005, 2010):
        start = date(year, 1, 1)
        end = date(year, 12, 31)
        current = start
        while current <= end:
            if current not in existing:
                missing.append(current)
            current += timedelta(days=1)

    return sorted(missing)

async def bulk_insert_processed(records: list):
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        for i in range(0, len(records), 2900):
            batch = records[i:i + 2900]
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

async def process_single_day(date_obj, windfarms):
    year = date_obj.year
    grib_dir = Path(f'grib_files/daily_{year}')
    grib_dir.mkdir(parents=True, exist_ok=True)

    date_str = date_obj.strftime('%Y%m%d')
    output_file = str(grib_dir / f'era5_{date_str}.grib')

    lats = [float(wf.lat) for wf in windfarms]
    lons = [float(wf.lng) for wf in windfarms]
    bbox = [max(lats) + 0.5, min(lons) - 0.5, min(lats) - 0.5, max(lons) + 0.5]

    logger.info(f"Fetching ERA5 for {date_obj}")
    c = cdsapi.Client()
    request = {
        'product_type': 'reanalysis',
        'format': 'grib',
        'variable': ['100m_u_component_of_wind', '100m_v_component_of_wind', '2m_temperature', 'surface_pressure'],
        'year': str(date_obj.year),
        'month': f'{date_obj.month:02d}',
        'day': f'{date_obj.day:02d}',
        'time': [f'{h:02d}:00' for h in range(24)],
        'area': bbox,
    }

    logger.info(f"Downloading to {output_file}")
    c.retrieve('reanalysis-era5-single-levels', request, output_file)

    ds = xr.open_dataset(output_file, engine='cfgrib')

    logger.info(f"Extracting data for {len(windfarms)} windfarms")
    processed_records = []

    for i, wf in enumerate(windfarms):
        if i % 400 == 0:
            logger.info(f"Processing windfarm {i+1}/{len(windfarms)}")

        wf_lat, wf_lng = float(wf.lat), float(wf.lng)
        try:
            u100_all = ds['u100'].interp(latitude=wf_lat, longitude=wf_lng, method='linear').values
            v100_all = ds['v100'].interp(latitude=wf_lat, longitude=wf_lng, method='linear').values
            t2m_all = ds['t2m'].interp(latitude=wf_lat, longitude=wf_lng, method='linear').values

            for time_idx in range(len(ds.time)):
                timestamp = pd.Timestamp(ds.time.values[time_idx], tz='UTC').to_pydatetime()
                u100 = float(u100_all[time_idx])
                v100 = float(v100_all[time_idx])
                t2m = float(t2m_all[time_idx])

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
        except Exception as e:
            continue

    ds.close()

    logger.info(f"Inserting {len(processed_records)} records")
    await bulk_insert_processed(processed_records)

    Path(output_file).unlink(missing_ok=True)

    return len(processed_records)

async def main():
    logger.info("=" * 60)
    logger.info("BACKFILL ALL GAPS 2005-2009")
    logger.info("=" * 60)

    missing_dates = await get_all_missing_dates()
    logger.info(f"Total missing dates to backfill: {len(missing_dates)}")

    if not missing_dates:
        logger.info("No missing dates found!")
        return

    # Group by year for display
    by_year = {}
    for d in missing_dates:
        by_year.setdefault(d.year, []).append(d)
    for year in sorted(by_year.keys()):
        logger.info(f"  {year}: {len(by_year[year])} days missing")

    windfarms = await get_all_windfarms()
    logger.info(f"Found {len(windfarms)} windfarms")

    total_records = 0
    for i, date_obj in enumerate(missing_dates):
        logger.info("")
        logger.info(f"Day {i+1}/{len(missing_dates)}")
        logger.info("=" * 60)
        logger.info(f"Processing {date_obj}")
        logger.info("=" * 60)

        try:
            records = await process_single_day(date_obj, windfarms)
            total_records += records
            logger.info(f"✓ Completed {date_obj}: {records} records (total: {total_records})")
        except Exception as e:
            logger.error(f"Error processing {date_obj}: {e}")
            continue

    logger.info("")
    logger.info("=" * 60)
    logger.info("BACKFILL GAPS 2005-2009 COMPLETE")
    logger.info(f"Total records inserted: {total_records}")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
