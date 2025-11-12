#!/usr/bin/env python3
"""
Verify that timezone fix worked correctly.
Checks that all 24 hours are present for each date.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from sqlalchemy import text


async def verify_dates():
    """Verify that all dates have 24 hours of data."""

    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Get distinct dates
        query = text("""
            SELECT
                DATE(hour AT TIME ZONE 'UTC') as date,
                COUNT(*) as total_records,
                COUNT(DISTINCT windfarm_id) as windfarms,
                COUNT(*) / COUNT(DISTINCT windfarm_id) as hours_per_windfarm
            FROM weather_data
            WHERE source = 'ERA5'
            GROUP BY DATE(hour AT TIME ZONE 'UTC')
            ORDER BY date
        """)

        result = await db.execute(query)
        rows = result.fetchall()

        print("="*80)
        print("TIMEZONE FIX VERIFICATION")
        print("="*80)
        print()

        if not rows:
            print("⚠️  No data found in weather_data table")
            return

        print(f"{'Date':<15} {'Total Records':<15} {'Windfarms':<15} {'Hours/WF':<15} {'Status'}")
        print("-"*80)

        all_good = True

        for row in rows:
            date = row[0]
            total = row[1]
            windfarms = row[2]
            hours_per_wf = row[3]

            if hours_per_wf == 24:
                status = "✓ GOOD"
            else:
                status = f"✗ BAD (missing {24 - hours_per_wf} hours)"
                all_good = False

            print(f"{str(date):<15} {total:<15} {windfarms:<15} {hours_per_wf:<15} {status}")

        print()
        print("="*80)

        if all_good:
            print("✓ SUCCESS: All dates have 24 hours per windfarm")
        else:
            print("✗ FAILURE: Some dates are missing hours")

        print("="*80)
        print()

        # Sample data for verification
        print("Sample data from first windfarm, first date:")
        print("-"*80)

        sample_query = text("""
            SELECT
                hour AT TIME ZONE 'UTC' as hour_utc,
                wind_speed_100m,
                wind_direction_deg,
                temperature_2m_c
            FROM weather_data
            WHERE source = 'ERA5'
            ORDER BY hour
            LIMIT 24
        """)

        result = await db.execute(sample_query)
        sample_rows = result.fetchall()

        for row in sample_rows:
            print(f"  {row[0]} | {row[1]:>6.3f} m/s | {row[2]:>6.2f}° | {row[3]:>5.2f}°C")


if __name__ == '__main__':
    asyncio.run(verify_dates())
