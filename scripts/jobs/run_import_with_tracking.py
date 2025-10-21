#!/usr/bin/env python3
"""
Wrapper script for running import jobs with database tracking.

Called by cron to execute scheduled import jobs.
Creates database record, executes import, updates status.

Usage:
    python scripts/jobs/run_import_with_tracking.py entsoe-daily
    python scripts/jobs/run_import_with_tracking.py taipower-hourly
    python scripts/jobs/run_import_with_tracking.py elexon-daily
    python scripts/jobs/run_import_with_tracking.py eia-monthly
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from app.core.database import get_session_factory
from app.models.import_job_execution import ImportJobExecution, ImportJobType
from app.services.import_job_service import ImportJobService
from app.schemas.import_job import ImportJobCreate


# Job configurations with appropriate delays
JOB_CONFIGS = {
    "entsoe-daily": {
        "source": "ENTSOE",
        "delay_days": 3,  # Import data from 3 days ago
        "description": "ENTSOE daily import (3-day lag)",
    },
    "elexon-daily": {
        "source": "ELEXON",
        "delay_days": 3,  # Import data from 3 days ago
        "description": "ELEXON daily import (3-day lag)",
    },
    "taipower-hourly": {
        "source": "Taipower",
        "delay_days": 0,  # Current snapshot
        "description": "Taipower hourly snapshot (live data)",
    },
    "eia-monthly": {
        "source": "EIA",
        "delay_months": 2,  # Import from 2 months ago
        "description": "EIA monthly import (2-month lag)",
    },
}


async def run_scheduled_import(job_name: str):
    """
    Execute a scheduled import job with database tracking.

    Args:
        job_name: Name of the job (e.g., 'entsoe-daily')
    """
    if job_name not in JOB_CONFIGS:
        print(f"❌ Unknown job: {job_name}")
        print(f"Available jobs: {', '.join(JOB_CONFIGS.keys())}")
        sys.exit(1)

    config = JOB_CONFIGS[job_name]

    # Calculate import date range based on delay
    today = datetime.now(timezone.utc)

    if "delay_days" in config:
        import_date = today - timedelta(days=config["delay_days"])
        import_start = import_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        import_end = import_date.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=None)
    elif "delay_months" in config:
        # For monthly jobs, calculate start of month from N months ago
        months_ago = config["delay_months"]
        year = today.year
        month = today.month - months_ago

        while month < 1:
            month += 12
            year -= 1

        import_start = datetime(year, month, 1, 0, 0, 0)

        # End of that month
        if month == 12:
            import_end = datetime(year + 1, 1, 1, 0, 0, 0) - timedelta(seconds=1)
        else:
            import_end = datetime(year, month + 1, 1, 0, 0, 0) - timedelta(seconds=1)
    else:
        import_start = today
        import_end = today

    print("=" * 80)
    print(f"SCHEDULED IMPORT: {job_name}")
    print("=" * 80)
    print(f"Job: {config['description']}")
    print(f"Source: {config['source']}")
    print(f"Import Period: {import_start.date()} to {import_end.date()}")
    print("=" * 80)

    # Create job in database
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        service = ImportJobService(db)

        # Create job record
        job_request = ImportJobCreate(
            source=config["source"],
            import_start_date=import_start,
            import_end_date=import_end,
            job_metadata={"job_config": job_name, "trigger": "cron"},
        )

        job = await service.create_job(
            job_request,
            user_id=None,  # No user for scheduled jobs
            job_type=ImportJobType.SCHEDULED,
        )

        print(f"\n✅ Created job execution record: ID {job.id}")
        print(f"Status: {job.status}")

        # Execute the job
        try:
            print(f"\n🚀 Executing import...")
            result = await service.execute_job(job.id)

            print(f"\n" + "=" * 80)
            print(f"EXECUTION COMPLETE")
            print("=" * 80)
            print(f"Status: {result.status}")
            print(f"Duration: {result.duration_seconds:.1f}s" if result.duration_seconds else "Duration: N/A")
            print(f"Records Imported: {result.records_imported:,}")
            if result.records_updated > 0:
                print(f"Records Updated: {result.records_updated:,}")
            if result.api_calls_made:
                print(f"API Calls: {result.api_calls_made}")

            if result.status == "success":
                print("\n✅ Import completed successfully!")
                sys.exit(0)
            else:
                print(f"\n❌ Import failed: {result.error_message}")
                sys.exit(1)

        except Exception as e:
            print(f"\n❌ Execution error: {str(e)}")
            sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_import_with_tracking.py <job_name>")
        print(f"\nAvailable jobs:")
        for job_name, config in JOB_CONFIGS.items():
            print(f"  {job_name:20} - {config['description']}")
        sys.exit(1)

    job_name = sys.argv[1]
    asyncio.run(run_scheduled_import(job_name))
