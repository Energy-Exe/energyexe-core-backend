"""ENTSOE data fetching scheduler."""

import asyncio
from datetime import datetime, timedelta

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.services.entsoe_historical_service import ENTSOEHistoricalService

logger = structlog.get_logger()

# Get settings
settings = get_settings()

# Create async engine for cron jobs
engine = create_async_engine(settings.database_url_async, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def fetch_daily_generation_data():
    """Cron job to fetch yesterday's generation data."""
    async with AsyncSessionLocal() as db:
        try:
            service = ENTSOEHistoricalService(db)

            # Fetch yesterday's data
            yesterday = datetime.utcnow().date() - timedelta(days=1)
            start_date = datetime.combine(yesterday, datetime.min.time())
            end_date = datetime.combine(yesterday, datetime.max.time())

            # Configure areas and types
            area_codes = settings.ENTSOE_DEFAULT_AREAS.split(",")
            production_types = ["wind", "solar"]

            logger.info(
                "Starting daily ENTSOE data fetch", date=yesterday.isoformat(), areas=area_codes
            )

            result = await service.fetch_and_store_historical_data(
                start_date=start_date,
                end_date=end_date,
                area_codes=area_codes,
                production_types=production_types,
                batch_days=1,  # Daily batches
            )

            logger.info(
                "Daily fetch completed",
                records=result["total_records"],
                errors=len(result.get("errors", [])),
            )

        except Exception as e:
            logger.error("Daily ENTSOE fetch failed", error=str(e))


async def fetch_hourly_generation_data():
    """Cron job to fetch last hour's generation data."""
    async with AsyncSessionLocal() as db:
        try:
            service = ENTSOEHistoricalService(db)

            # Fetch last 2 hours (overlap for data consistency)
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(hours=2)

            # Configure areas and types
            area_codes = settings.ENTSOE_DEFAULT_AREAS.split(",")
            production_types = ["wind", "solar"]

            logger.info(
                "Starting hourly ENTSOE data fetch",
                start=start_date.isoformat(),
                end=end_date.isoformat(),
            )

            result = await service.fetch_and_store_historical_data(
                start_date=start_date,
                end_date=end_date,
                area_codes=area_codes,
                production_types=production_types,
                batch_days=1,
            )

            logger.info("Hourly fetch completed", records=result["total_records"])

        except Exception as e:
            logger.error("Hourly ENTSOE fetch failed", error=str(e))


async def backfill_historical_data(days_back: int = 30):
    """One-time backfill of historical data."""
    async with AsyncSessionLocal() as db:
        try:
            service = ENTSOEHistoricalService(db)

            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days_back)

            area_codes = settings.ENTSOE_DEFAULT_AREAS.split(",")
            production_types = ["wind", "solar"]

            logger.info(
                "Starting backfill",
                days=days_back,
                start=start_date.isoformat(),
                end=end_date.isoformat(),
            )

            result = await service.fetch_and_store_historical_data(
                start_date=start_date,
                end_date=end_date,
                area_codes=area_codes,
                production_types=production_types,
                batch_days=settings.ENTSOE_FETCH_BATCH_DAYS,
            )

            logger.info(
                "Backfill completed",
                records=result["total_records"],
                batches=result["batches_processed"],
                errors=len(result.get("errors", [])),
            )

        except Exception as e:
            logger.error("Backfill failed", error=str(e))


async def cleanup_old_fetch_history():
    """Clean up old fetch history records."""
    async with AsyncSessionLocal() as db:
        try:
            # Keep only last 30 days of fetch history
            cutoff_date = datetime.utcnow() - timedelta(days=30)

            query = """
                DELETE FROM entsoe_fetch_history
                WHERE created_at < :cutoff_date
                AND status IN ('success', 'failed')
            """

            result = await db.execute(query, {"cutoff_date": cutoff_date})
            await db.commit()

            logger.info("Cleaned up fetch history", deleted_rows=result.rowcount)

        except Exception as e:
            logger.error("Fetch history cleanup failed", error=str(e))


# Initialize scheduler
scheduler = AsyncIOScheduler()

# Schedule daily fetch at 2 AM UTC
scheduler.add_job(
    fetch_daily_generation_data,
    "cron",
    hour=2,
    minute=0,
    id="daily_entsoe_fetch",
    replace_existing=True,
)

# Schedule hourly fetch at 15 minutes past the hour
scheduler.add_job(
    fetch_hourly_generation_data, "cron", minute=15, id="hourly_entsoe_fetch", replace_existing=True
)

# Schedule cleanup weekly on Sundays at 3 AM UTC
scheduler.add_job(
    cleanup_old_fetch_history,
    "cron",
    day_of_week=0,
    hour=3,
    minute=0,
    id="weekly_fetch_history_cleanup",
    replace_existing=True,
)


def start_scheduler():
    """Start the cron scheduler."""
    scheduler.start()
    logger.info("ENTSOE cron scheduler started")


def stop_scheduler():
    """Stop the cron scheduler."""
    scheduler.shutdown()
    logger.info("ENTSOE cron scheduler stopped")


# Command to run backfill manually
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        asyncio.run(backfill_historical_data(days))
    else:
        print("Usage: python entsoe_scheduler.py backfill [days]")
        print("Example: python entsoe_scheduler.py backfill 30")
