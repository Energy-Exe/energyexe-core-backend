#!/usr/bin/env python3
"""
Check ERA5 Weather Data Import Status

Shows import coverage for raw and processed weather data.

Usage:
    poetry run python scripts/seeds/weather_data/era5/check_import_status.py
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from collections import defaultdict
import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.windfarm import Windfarm
from app.models.weather_data import WeatherDataRaw, WeatherData
from sqlalchemy import select, func

logger = structlog.get_logger()


async def check_import_status():
    """Check and display import status for weather data."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        print("\n" + "="*70)
        print("ERA5 WEATHER DATA IMPORT STATUS")
        print("="*70)

        # 1. Check raw data coverage
        print("\n" + "-"*70)
        print("RAW DATA (weather_data_raw)")
        print("-"*70)

        # Get unique grid points with their coverage
        raw_query = select(
            WeatherDataRaw.latitude,
            WeatherDataRaw.longitude,
            func.min(WeatherDataRaw.timestamp).label('min_time'),
            func.max(WeatherDataRaw.timestamp).label('max_time'),
            func.count(WeatherDataRaw.id).label('record_count')
        ).where(
            WeatherDataRaw.source == 'ERA5'
        ).group_by(
            WeatherDataRaw.latitude,
            WeatherDataRaw.longitude
        ).order_by(
            WeatherDataRaw.latitude.desc(),
            WeatherDataRaw.longitude
        )

        result = await db.execute(raw_query)
        raw_stats = result.all()

        if raw_stats:
            print(f"\nGrid Points: {len(raw_stats)}")
            print(f"{'Latitude':<12} {'Longitude':<12} {'Start Date':<12} {'End Date':<12} {'Records':<10} {'Status':<10}")
            print("-"*70)

            total_raw_records = 0
            for stat in raw_stats:
                lat = float(stat.latitude)
                lon = float(stat.longitude)
                min_time = stat.min_time
                max_time = stat.max_time
                count = stat.record_count

                total_raw_records += count

                # Calculate expected hours
                hours_diff = (max_time - min_time).total_seconds() / 3600
                expected_records = int(hours_diff) + 1

                status = "OK" if count >= expected_records * 0.95 else "INCOMPLETE"

                print(f"{lat:<12.4f} {lon:<12.4f} {min_time.date()!s:<12} {max_time.date()!s:<12} {count:<10,} {status:<10}")

            print(f"\nTotal Raw Records: {total_raw_records:,}")
        else:
            print("\n⚠ No raw data found")

        # 2. Check processed data coverage
        print("\n" + "-"*70)
        print("PROCESSED DATA (weather_data)")
        print("-"*70)

        # Get windfarms with processed data
        processed_query = select(
            WeatherData.windfarm_id,
            func.min(WeatherData.hour).label('min_time'),
            func.max(WeatherData.hour).label('max_time'),
            func.count(WeatherData.id).label('record_count')
        ).where(
            WeatherData.source == 'ERA5'
        ).group_by(
            WeatherData.windfarm_id
        ).order_by(
            WeatherData.windfarm_id
        )

        result = await db.execute(processed_query)
        processed_stats = result.all()

        if processed_stats:
            # Get windfarm names
            wf_ids = [stat.windfarm_id for stat in processed_stats]
            wf_query = select(Windfarm).where(Windfarm.id.in_(wf_ids))
            wf_result = await db.execute(wf_query)
            windfarms = {wf.id: wf for wf in wf_result.scalars().all()}

            print(f"\nWindfarms: {len(processed_stats)}")
            print(f"{'ID':<6} {'Name':<30} {'Start Date':<12} {'End Date':<12} {'Records':<10} {'Status':<10}")
            print("-"*70)

            total_processed_records = 0
            for stat in processed_stats:
                wf_id = stat.windfarm_id
                wf_name = windfarms[wf_id].name if wf_id in windfarms else "Unknown"
                min_time = stat.min_time
                max_time = stat.max_time
                count = stat.record_count

                total_processed_records += count

                # Calculate expected hours
                hours_diff = (max_time - min_time).total_seconds() / 3600
                expected_records = int(hours_diff) + 1

                status = "OK" if count >= expected_records * 0.95 else "INCOMPLETE"

                # Truncate name if too long
                display_name = wf_name[:28] + '..' if len(wf_name) > 30 else wf_name

                print(f"{wf_id:<6} {display_name:<30} {min_time.date()!s:<12} {max_time.date()!s:<12} {count:<10,} {status:<10}")

            print(f"\nTotal Processed Records: {total_processed_records:,}")
        else:
            print("\n⚠ No processed data found")

        # 3. Summary statistics
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)

        if raw_stats:
            earliest_raw = min(s.min_time for s in raw_stats)
            latest_raw = max(s.max_time for s in raw_stats)
            print(f"Raw Data Coverage: {earliest_raw.date()} to {latest_raw.date()}")
            print(f"Total Raw Records: {total_raw_records:,}")
            print(f"Unique Grid Points: {len(raw_stats)}")

        if processed_stats:
            earliest_proc = min(s.min_time for s in processed_stats)
            latest_proc = max(s.max_time for s in processed_stats)
            print(f"\nProcessed Data Coverage: {earliest_proc.date()} to {latest_proc.date()}")
            print(f"Total Processed Records: {total_processed_records:,}")
            print(f"Windfarms Processed: {len(processed_stats)}")

        print("\n")


def main():
    """CLI entry point."""
    asyncio.run(check_import_status())


if __name__ == '__main__':
    main()
