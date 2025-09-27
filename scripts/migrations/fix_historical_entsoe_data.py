#!/usr/bin/env python3
"""
Fix historical ENTSOE data that wasn't updated in the first migration.
The mappings exist but the historical generation_data records need updating.
"""

import asyncio
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Get database URL from environment or use default
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:RwaN9FJDCgP2AhuALxZ4Wa7QfvbKXQ647AAickORJ0rq5N6lUG19UneFJJTJ9Jnv@146.235.201.245:5432/energyexe_db"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def update_all_orphaned_entsoe_data(session: AsyncSession, dry_run: bool = False) -> int:
    """Update ALL orphaned ENTSOE generation_data records with windfarm_id."""

    # First check how many records we need to update
    count_query = text("""
        SELECT COUNT(*) as count
        FROM generation_data gd
        WHERE gd.windfarm_id IS NULL
            AND gd.source = 'ENTSOE'
            AND EXISTS (
                SELECT 1 FROM generation_unit_mapping gum
                WHERE gum.generation_unit_id = gd.generation_unit_id
                    AND gum.windfarm_id IS NOT NULL
            )
    """)

    result = await session.execute(count_query)
    total_to_update = result.fetchone().count

    logger.info(f"Found {total_to_update:,} ENTSOE records that need updating")

    if not dry_run and total_to_update > 0:
        # Update in batches to avoid timeouts
        batch_size = 50000
        updated_total = 0

        while updated_total < total_to_update:
            # Update batch
            update_query = text("""
                WITH to_update AS (
                    SELECT gd.id
                    FROM generation_data gd
                    INNER JOIN generation_unit_mapping gum ON gd.generation_unit_id = gum.generation_unit_id
                    WHERE gd.windfarm_id IS NULL
                        AND gd.source = 'ENTSOE'
                        AND gum.windfarm_id IS NOT NULL
                    LIMIT :batch_size
                )
                UPDATE generation_data gd
                SET windfarm_id = gum.windfarm_id,
                    updated_at = NOW()
                FROM generation_unit_mapping gum, to_update tu
                WHERE gd.id = tu.id
                    AND gd.generation_unit_id = gum.generation_unit_id
            """)

            result = await session.execute(update_query, {'batch_size': batch_size})
            batch_updated = result.rowcount
            updated_total += batch_updated

            await session.commit()

            if batch_updated == 0:
                break

            logger.info(f"Updated {updated_total:,}/{total_to_update:,} records ({updated_total*100//total_to_update}%)")

        logger.info(f"Successfully updated {updated_total:,} records")
        return updated_total

    return total_to_update


async def verify_results(session: AsyncSession):
    """Verify the update results."""

    # Check remaining orphaned ENTSOE records
    query = text("""
        SELECT
            COUNT(*) as orphaned_count,
            COUNT(DISTINCT generation_unit_id) as orphaned_units
        FROM generation_data
        WHERE windfarm_id IS NULL
            AND source = 'ENTSOE'
    """)
    result = await session.execute(query)
    stats = result.fetchone()

    logger.info("\n=== Verification Results ===")
    logger.info(f"Remaining orphaned ENTSOE records: {stats.orphaned_count:,}")
    logger.info(f"Remaining orphaned ENTSOE units: {stats.orphaned_units}")

    # Show windfarms that now have ENTSOE data
    query = text("""
        SELECT
            w.name as windfarm_name,
            COUNT(DISTINCT gd.id) as record_count,
            MIN(gd.hour) as first_date,
            MAX(gd.hour) as last_date
        FROM windfarms w
        INNER JOIN generation_data gd ON gd.windfarm_id = w.id
        WHERE gd.source = 'ENTSOE'
        GROUP BY w.id, w.name
        ORDER BY record_count DESC
        LIMIT 20
    """)

    result = await session.execute(query)
    windfarms = result.fetchall()

    logger.info("\n=== Top 20 Windfarms with ENTSOE Data ===")
    logger.info(f"{'Windfarm':<40} {'Records':<12} {'First Date':<20} {'Last Date':<20}")
    logger.info("-" * 100)

    for wf in windfarms:
        logger.info(f"{wf.windfarm_name:<40} {wf.record_count:<12,} {str(wf.first_date):<20} {str(wf.last_date):<20}")

    return stats.orphaned_count


async def main():
    """Main function to fix historical ENTSOE data."""
    import argparse

    parser = argparse.ArgumentParser(description='Fix historical ENTSOE data')
    parser.add_argument('--dry-run', action='store_true',
                       help='Run in dry-run mode (no database changes)')

    args = parser.parse_args()

    logger.info("=== Fix Historical ENTSOE Data ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    # Create database engine
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Update orphaned data
        logger.info("\nUpdating orphaned ENTSOE records...")
        updated = await update_all_orphaned_entsoe_data(session, dry_run=args.dry_run)

        if args.dry_run:
            logger.info(f"Would update {updated:,} records")
        else:
            logger.info(f"Updated {updated:,} records")

        # Verify results
        if not args.dry_run:
            logger.info("\nVerifying results...")
            orphaned = await verify_results(session)

            if orphaned == 0:
                logger.info("\n✅ Success! All ENTSOE records now have windfarm_id")
            else:
                logger.warning(f"\n⚠️ {orphaned:,} records still orphaned - these may need additional mappings")

    # Dispose of the engine
    await engine.dispose()
    logger.info("\n=== Update Complete ===")


if __name__ == "__main__":
    asyncio.run(main())