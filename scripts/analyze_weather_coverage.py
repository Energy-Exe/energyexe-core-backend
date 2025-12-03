"""Comprehensive weather data coverage analysis script."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent))

from app.core.database import get_session_factory
from sqlalchemy import text

async def analyze_weather_coverage():
    """Analyze weather data coverage and identify gaps."""

    AsyncSessionLocal = get_session_factory()

    print("=" * 80)
    print("WEATHER DATA COVERAGE ANALYSIS")
    print("=" * 80)
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    async with AsyncSessionLocal() as db:
        # 1. Check if tables have data
        print("1. TABLE STATUS:")
        print("-" * 80)

        try:
            result = await db.execute(text("SELECT reltuples::bigint FROM pg_class WHERE relname = 'weather_data'"))
            est_count = result.scalar() or 0
            print(f"   weather_data: ~{est_count:,} records (estimated)")
        except Exception as e:
            print(f"   weather_data: Error - {e}")

        try:
            result = await db.execute(text("SELECT reltuples::bigint FROM pg_class WHERE relname = 'weather_data_raw'"))
            est_raw_count = result.scalar() or 0
            print(f"   weather_data_raw: ~{est_raw_count:,} records (estimated)")
        except Exception as e:
            print(f"   weather_data_raw: Error - {e}")

        # 2. Import jobs summary
        print("\n2. IMPORT JOBS:")
        print("-" * 80)

        try:
            result = await db.execute(text("""
                SELECT
                    status,
                    COUNT(*) as count,
                    SUM(records_imported) as total_records
                FROM weather_import_jobs
                GROUP BY status
                ORDER BY count DESC
            """))

            for row in result:
                print(f"   {row.status}: {row.count} jobs, {row.total_records:,} records imported" if row.total_records else f"   {row.status}: {row.count} jobs")
        except Exception as e:
            print(f"   Error: {e}")

        # 3. Latest import job
        print("\n3. LATEST IMPORT JOBS:")
        print("-" * 80)

        try:
            result = await db.execute(text("""
                SELECT
                    id, job_name, status,
                    import_start_date, import_end_date,
                    records_imported, started_at, completed_at,
                    error_message
                FROM weather_import_jobs
                ORDER BY id DESC
                LIMIT 5
            """))

            for row in result:
                status_icon = '✓' if row.status == 'success' else '✗' if row.status == 'failed' else '⏳' if row.status == 'running' else '○'
                print(f"\n   {status_icon} Job {row.id}: {row.job_name}")
                print(f"      Status: {row.status}")
                print(f"      Date range: {row.import_start_date} to {row.import_end_date}")
                if row.records_imported:
                    print(f"      Records: {row.records_imported:,}")
                if row.started_at:
                    print(f"      Started: {row.started_at}")
                if row.completed_at:
                    duration = (row.completed_at - row.started_at).total_seconds() / 60
                    print(f"      Duration: {duration:.1f} minutes")
                if row.error_message:
                    print(f"      Error: {row.error_message[:100]}")
        except Exception as e:
            print(f"   Error: {e}")

        # 4. Date coverage (if data exists)
        if est_count > 0:
            print("\n4. DATE COVERAGE (sampling):")
            print("-" * 80)

            try:
                # Get min/max using index
                result = await db.execute(text("""
                    SELECT MIN(hour) as earliest, MAX(hour) as latest
                    FROM weather_data
                    WHERE windfarm_id = (SELECT MIN(id) FROM windfarms LIMIT 1)
                """))
                stats = result.fetchone()
                if stats and stats.earliest:
                    print(f"   Earliest: {stats.earliest}")
                    print(f"   Latest: {stats.latest}")

                    days_span = (stats.latest - stats.earliest).days
                    print(f"   Span: {days_span} days")

                    # Expected from 2010
                    expected_start = datetime(2010, 1, 1)
                    expected_days = (datetime.now() - expected_start).days
                    print(f"\n   Expected start (2010-01-01): {expected_days} days ago")
                    print(f"   Actual start: {(datetime.now() - stats.earliest.replace(tzinfo=None)).days} days ago")

                    missing_days = (stats.earliest.replace(tzinfo=None) - expected_start).days
                    if missing_days > 0:
                        print(f"   ❌ Missing early data: ~{missing_days} days from 2010 start")
            except Exception as e:
                print(f"   Error: {e}")

        # 5. Windfarm coverage sample
        print("\n5. WINDFARM COVERAGE (sample):")
        print("-" * 80)

        if est_count > 0:
            try:
                result = await db.execute(text("""
                    SELECT COUNT(DISTINCT windfarm_id) as wf_count
                    FROM weather_data
                    LIMIT 1
                """))
                wf_count = result.scalar()
                print(f"   Windfarms with weather data: ~{wf_count} / 1,591")
            except Exception as e:
                print(f"   Error: {e}")

        # 6. Recommendations
        print("\n6. ANALYSIS SUMMARY:")
        print("=" * 80)

        if est_count == 0:
            print("""
   ❌ NO WEATHER DATA FOUND

   The weather_data table is empty. You need to import data using:

   poetry run python scripts/weather/import_era5.py \\
       --start-date 2010-01-01 \\
       --end-date 2025-12-02 \\
       --parallel-days 30

   Expected import time: ~2-3 days for full 15-year backfill
            """)
        else:
            print(f"""
   Database contains ~{est_count:,} weather records

   See detailed analysis in:
   /Users/mdfaisal/Documents/energyexe/WEATHER_DATA_COVERAGE_ANALYSIS.md

   Run gap detection queries from the report to identify missing dates.
            """)

if __name__ == "__main__":
    asyncio.run(analyze_weather_coverage())
