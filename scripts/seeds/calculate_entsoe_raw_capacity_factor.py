"""
Calculate raw capacity factor for ENTSOE generation data.

This script calculates the raw capacity factor for ENTSOE records that have
raw_capacity_mw data but no raw_capacity_factor.

Raw Capacity Factor = generation_mwh / raw_capacity_mw
(for hourly data, generation in MWh equals average MW over the hour)
"""

import asyncio
import sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime
import logging
from typing import Optional

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy import select, and_, func, update
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_session_factory
from app.models.generation_data import GenerationData

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_entsoe_stats(db: AsyncSession) -> dict:
    """Get statistics about ENTSOE data before processing."""
    result = await db.execute(
        select(
            func.count(GenerationData.id).label('total_records'),
            func.count(GenerationData.raw_capacity_mw).label('with_raw_capacity'),
            func.count(GenerationData.raw_capacity_factor).label('with_raw_cf'),
            func.min(GenerationData.raw_capacity_mw).label('min_capacity'),
            func.max(GenerationData.raw_capacity_mw).label('max_capacity'),
            func.min(GenerationData.generation_mwh).label('min_generation'),
            func.max(GenerationData.generation_mwh).label('max_generation')
        ).where(
            GenerationData.source == 'ENTSOE'
        )
    )

    row = result.first()
    return {
        'total_records': row.total_records or 0,
        'with_raw_capacity': row.with_raw_capacity or 0,
        'with_raw_cf': row.with_raw_cf or 0,
        'min_capacity': float(row.min_capacity) if row.min_capacity else 0,
        'max_capacity': float(row.max_capacity) if row.max_capacity else 0,
        'min_generation': float(row.min_generation) if row.min_generation else 0,
        'max_generation': float(row.max_generation) if row.max_generation else 0,
    }


async def calculate_raw_capacity_factor_batch(
    db: AsyncSession,
    limit: int = 10000,
    dry_run: bool = False
) -> int:
    """Calculate raw capacity factor for a batch of records."""

    # Select records that have raw_capacity_mw but no raw_capacity_factor
    result = await db.execute(
        select(GenerationData).where(
            and_(
                GenerationData.source == 'ENTSOE',
                GenerationData.raw_capacity_mw.isnot(None),
                GenerationData.raw_capacity_mw > 0,
                GenerationData.raw_capacity_factor.is_(None)
            )
        ).limit(limit)
    )

    records = result.scalars().all()

    if not records:
        return 0

    updated_count = 0
    for record in records:
        try:
            # Calculate raw capacity factor
            # For hourly data: CF = generation_mwh / raw_capacity_mw
            # generation_mwh represents the average MW output over the hour

            generation = float(record.generation_mwh) if record.generation_mwh else 0
            raw_capacity = float(record.raw_capacity_mw)

            if raw_capacity > 0:
                raw_cf = generation / raw_capacity
                # Cap at 9.9999 to fit in NUMERIC(5,4)
                # Values > 1.0 can occur when generation exceeds nameplate capacity
                raw_cf = min(raw_cf, 9.9999)

                if not dry_run:
                    record.raw_capacity_factor = Decimal(str(raw_cf))

                updated_count += 1

                if updated_count % 1000 == 0:
                    logger.info(f"Processed {updated_count} records...")

        except Exception as e:
            logger.error(f"Error processing record {record.id}: {e}")
            continue

    if not dry_run:
        await db.commit()
        logger.info(f"Updated {updated_count} records with raw capacity factor")
    else:
        logger.info(f"Dry run: Would update {updated_count} records")

    return updated_count


async def process_all_entsoe_records(dry_run: bool = False):
    """Process all ENTSOE records to calculate raw capacity factor."""

    SessionLocal = get_session_factory()

    async with SessionLocal() as db:
        # Get initial statistics
        logger.info("Getting initial statistics...")
        stats_before = await get_entsoe_stats(db)

        logger.info(f"ENTSOE Data Statistics:")
        logger.info(f"  Total records: {stats_before['total_records']:,}")
        logger.info(f"  With raw_capacity_mw: {stats_before['with_raw_capacity']:,}")
        logger.info(f"  With raw_capacity_factor: {stats_before['with_raw_cf']:,}")
        logger.info(f"  Capacity range: {stats_before['min_capacity']:.1f} - {stats_before['max_capacity']:.1f} MW")
        logger.info(f"  Generation range: {stats_before['min_generation']:.3f} - {stats_before['max_generation']:.1f} MWh")

        records_to_process = stats_before['with_raw_capacity'] - stats_before['with_raw_cf']
        logger.info(f"\nRecords to process: {records_to_process:,}")

        if records_to_process == 0:
            logger.info("No records to process. All ENTSOE records with raw capacity already have raw capacity factor.")
            return

        # Process in batches
        batch_size = 50000
        total_updated = 0
        batch_num = 0

        while True:
            batch_num += 1
            logger.info(f"\nProcessing batch {batch_num} (up to {batch_size:,} records)...")

            updated = await calculate_raw_capacity_factor_batch(db, limit=batch_size, dry_run=dry_run)

            if updated == 0:
                break

            total_updated += updated
            logger.info(f"Total processed so far: {total_updated:,}")

            # Don't continue if dry run
            if dry_run:
                break

        # Get final statistics
        if not dry_run:
            logger.info("\nGetting final statistics...")
            stats_after = await get_entsoe_stats(db)

            logger.info(f"\nFinal ENTSOE Data Statistics:")
            logger.info(f"  Total records: {stats_after['total_records']:,}")
            logger.info(f"  With raw_capacity_mw: {stats_after['with_raw_capacity']:,}")
            logger.info(f"  With raw_capacity_factor: {stats_after['with_raw_cf']:,}")
            logger.info(f"  Newly calculated: {stats_after['with_raw_cf'] - stats_before['with_raw_cf']:,}")

        logger.info(f"\n{'Dry run complete' if dry_run else 'Processing complete'}!")
        logger.info(f"Total records {'would be' if dry_run else ''} updated: {total_updated:,}")


async def show_sample_calculations(limit: int = 10):
    """Show sample calculations for verification."""

    SessionLocal = get_session_factory()

    async with SessionLocal() as db:
        # Get sample records with raw capacity
        result = await db.execute(
            select(GenerationData).where(
                and_(
                    GenerationData.source == 'ENTSOE',
                    GenerationData.raw_capacity_mw.isnot(None),
                    GenerationData.raw_capacity_mw > 0,
                    GenerationData.generation_mwh > 0
                )
            ).order_by(GenerationData.hour.desc()).limit(limit)
        )

        records = result.scalars().all()

        logger.info("\nSample Raw Capacity Factor Calculations:")
        logger.info("-" * 80)

        for record in records:
            generation = float(record.generation_mwh) if record.generation_mwh else 0
            raw_capacity = float(record.raw_capacity_mw) if record.raw_capacity_mw else 0

            if raw_capacity > 0:
                calculated_cf = generation / raw_capacity
                calculated_cf = min(calculated_cf, 9.9999)

                logger.info(f"\nWindfarm ID: {record.windfarm_id}, Unit ID: {record.generation_unit_id}")
                logger.info(f"  Date/Hour: {record.hour}")
                logger.info(f"  Generation: {generation:.3f} MWh")
                logger.info(f"  Raw Capacity: {raw_capacity:.1f} MW")
                logger.info(f"  Calculated Raw CF: {calculated_cf:.4f} ({calculated_cf * 100:.2f}%)")

                if record.raw_capacity_factor:
                    stored_cf = float(record.raw_capacity_factor)
                    logger.info(f"  Stored Raw CF: {stored_cf:.4f} ({stored_cf * 100:.2f}%)")
                    if abs(calculated_cf - stored_cf) > 0.0001:
                        logger.warning(f"  ⚠️  Difference detected!")
                else:
                    logger.info(f"  Stored Raw CF: None (will be calculated)")


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Calculate raw capacity factor for ENTSOE data')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without updating database')
    parser.add_argument('--sample', action='store_true', help='Show sample calculations only')
    parser.add_argument('--sample-count', type=int, default=10, help='Number of samples to show')

    args = parser.parse_args()

    if args.sample:
        await show_sample_calculations(limit=args.sample_count)
    else:
        await process_all_entsoe_records(dry_run=args.dry_run)


if __name__ == '__main__':
    asyncio.run(main())