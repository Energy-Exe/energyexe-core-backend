"""Export hourly wind speed (ERA5 weather) data for Haram windfarm to CSV."""

import asyncio
import csv
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.models.weather_data import WeatherData
from app.models.windfarm import Windfarm
from app.core.config import get_settings


async def export_haram():
    settings = get_settings()
    engine = create_async_engine(str(settings.DATABASE_URL), echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    start_dt = datetime(2024, 12, 31, 0, 0, 0)
    end_dt = datetime(2026, 1, 2, 23, 59, 59)
    output_file = str(Path(__file__).parent.parent / "haram_hourly_wind_data.csv")

    async with async_session() as db:
        # Find Haram windfarm
        wf_query = select(Windfarm).where(Windfarm.name.ilike('%Haram%'))
        result = await db.execute(wf_query)
        windfarms = result.scalars().all()

        if not windfarms:
            print("No windfarm found matching 'Haram'")
            await engine.dispose()
            return

        for wf in windfarms:
            print(f"Found windfarm: id={wf.id}, name={wf.name}, code={wf.code}")

        windfarm = windfarms[0]
        windfarm_id = windfarm.id

        # Count rows first
        count_query = select(func.count()).select_from(WeatherData).where(
            and_(
                WeatherData.windfarm_id == windfarm_id,
                WeatherData.hour >= start_dt,
                WeatherData.hour <= end_dt,
            )
        )
        result = await db.execute(count_query)
        total = result.scalar()
        print(f"Total rows to export: {total:,}")

        # Query data (uses idx_weather_windfarm_hour index)
        query = select(WeatherData).where(
            and_(
                WeatherData.windfarm_id == windfarm_id,
                WeatherData.hour >= start_dt,
                WeatherData.hour <= end_dt,
            )
        ).order_by(WeatherData.hour)

        result = await db.execute(query)
        rows = result.scalars().all()

        # Write CSV
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'hour',
                'windfarm_name',
                'wind_speed_100m',
                'wind_direction_deg',
                'temperature_2m_c',
                'source',
            ])

            for row in rows:
                writer.writerow([
                    row.hour.strftime('%Y-%m-%d %H:%M:%S'),
                    windfarm.name,
                    float(row.wind_speed_100m) if row.wind_speed_100m is not None else '',
                    float(row.wind_direction_deg) if row.wind_direction_deg is not None else '',
                    float(row.temperature_2m_c) if row.temperature_2m_c is not None else '',
                    row.source,
                ])

        print(f"\nExported {len(rows):,} rows to {output_file}")
        file_size = Path(output_file).stat().st_size
        if file_size > 1024 * 1024:
            print(f"File size: {file_size / (1024 * 1024):.2f} MB")
        else:
            print(f"File size: {file_size / 1024:.2f} KB")

    await engine.dispose()


if __name__ == '__main__':
    asyncio.run(export_haram())
