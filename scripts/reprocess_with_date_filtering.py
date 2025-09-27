#!/usr/bin/env python3
"""
Reprocess generation data with proper date filtering for unit lifespans.
This script processes data in batches to handle the date filtering updates.
"""

import asyncio
import sys
from datetime import datetime, timedelta, date
from sqlalchemy import select, and_, or_, func
from app.core.database import get_session_factory
from app.models.generation_unit import GenerationUnit
from app.models.generation_data import GenerationData
from scripts.seeds.aggregate_generation_data.process_generation_data_daily import DailyGenerationProcessor
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def reprocess_date_range(source: str, start_date: date, end_date: date, batch_size: int = 30):
    """Reprocess a date range for a specific source."""

    session_factory = get_session_factory()

    current_date = start_date
    processed_days = 0
    failed_days = []

    while current_date <= end_date:
        batch_end = min(current_date + timedelta(days=batch_size - 1), end_date)

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing batch: {current_date} to {batch_end} ({source})")
        logger.info(f"{'='*60}")

        # Process each day in the batch
        batch_current = current_date
        while batch_current <= batch_end:
            async with session_factory() as db:
                processor = DailyGenerationProcessor(db, dry_run=False)

                try:
                    day_start = datetime.combine(batch_current, datetime.min.time())
                    result = await processor.process_day(day_start, sources=[source])

                    source_result = result['sources'].get(source, {})
                    if 'error' in source_result:
                        logger.error(f"  {batch_current}: ERROR - {source_result['error']}")
                        failed_days.append(batch_current)
                    else:
                        logger.info(f"  {batch_current}: {source_result.get('raw_records', 0):,} raw → {source_result.get('saved', 0):,} saved")
                        processed_days += 1

                except Exception as e:
                    logger.error(f"  {batch_current}: FAILED - {e}")
                    failed_days.append(batch_current)
                    await db.rollback()

            batch_current += timedelta(days=1)

        current_date = batch_end + timedelta(days=1)

        # Short pause between batches
        if current_date <= end_date:
            logger.info(f"\nCompleted batch. Pausing before next batch...")
            await asyncio.sleep(2)

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"REPROCESSING COMPLETE FOR {source}")
    logger.info(f"{'='*60}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Successfully processed: {processed_days} days")
    logger.info(f"Failed: {len(failed_days)} days")

    if failed_days:
        logger.warning("Failed days:")
        for day in failed_days:
            logger.warning(f"  - {day}")

async def main():
    """Main entry point."""

    parser = argparse.ArgumentParser(
        description='Reprocess generation data with date filtering'
    )
    parser.add_argument(
        '--source',
        type=str,
        required=True,
        choices=['ENTSOE', 'ELEXON', 'NVE', 'TAIPOWER', 'EIA', 'ENERGISTYRELSEN', 'EEX'],
        help='Data source to reprocess'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        required=True,
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=30,
        help='Number of days to process in each batch (default: 30)'
    )

    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)

    if start_date > end_date:
        logger.error("Start date must be before end date")
        sys.exit(1)

    total_days = (end_date - start_date).days + 1
    logger.info(f"\n{'='*60}")
    logger.info(f"STARTING REPROCESSING")
    logger.info(f"{'='*60}")
    logger.info(f"Source: {args.source}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Total days: {total_days}")
    logger.info(f"Batch size: {args.batch_size} days")
    logger.info(f"Estimated batches: {(total_days + args.batch_size - 1) // args.batch_size}")

    # Confirmation prompt for large reprocessing
    if total_days > 365:
        response = input(f"\nThis will reprocess {total_days} days of data. Continue? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Reprocessing cancelled")
            sys.exit(0)

    await reprocess_date_range(args.source, start_date, end_date, args.batch_size)

if __name__ == "__main__":
    sys.path.insert(0, '/Users/mohammadfaisal/Documents/energyexe/energyexe-core-backend')
    asyncio.run(main())