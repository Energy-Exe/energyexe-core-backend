#!/usr/bin/env python3
"""
Fix ENTSOE generation unit to windfarm mappings.
This script creates the missing relationships between ENTSOE generation units and windfarms.
"""

import asyncio
import logging
from typing import Dict, List, Tuple
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Comprehensive mapping of ENTSOE generation units to windfarms
# Format: 'Windfarm Name': (windfarm_id, [unit_patterns])
ENTSOE_WINDFARM_MAPPINGS: Dict[str, Tuple[int, List[str]]] = {
    'Greater Gabbard': (7376, ['Greater Gabbard GRGBW-1', 'Greater Gabbard GRGBW-2', 'Greater Gabbard GRGBW-3']),
    'London Array': (7392, ['London Array Wind Farm LARYO-1', 'London Array Wind Farm LARYO-2',
                            'London Array Wind Farm LARYO-3', 'London Array Wind Farm LARYO-4']),
    'Lincs': (7391, ['Lincs Wind Farm LNCSO-1', 'Lincs Wind Farm LNCSO-2']),
    'Westermost Rough': (7424, ['Westermost Rough W/F WTMSO-1']),
    'Sheringham Shoal': (7414, ['Sheringham Shoal Wind Farm SHRSO-1', 'Sheringham Shoal Wind Farm SHRSO-2']),
    'Walney 1&2': (7421, ['Walney Wind Farm WLNYW-1']),
    'Barrow': (7358, ['Barrow Offshore Wind Farm BOWLW-1']),
    'Robin Rigg': (7408, ['Robin Rigg East RREW-1', 'Robin Rigg West RRWW-1']),
    'Burbo Bank': (7365, ['Burbo Wind Farm BURBW-1']),
    'Gwynt Y Mor': (7378, [f'Gwynt Y Mor GYMRO-{i}' for i in range(1, 29)]),  # GYMRO-1 to GYMRO-28
    'Ormonde': (7385, ['Ormonde Eng Ltd']),
}


async def analyze_orphaned_units(session: AsyncSession) -> Dict:
    """Analyze current orphaned generation units."""
    query = text("""
        SELECT
            gu.id,
            gu.name,
            COUNT(gd.id) as orphaned_records,
            MIN(gd.hour) as first_date,
            MAX(gd.hour) as last_date
        FROM generation_units gu
        INNER JOIN generation_data gd ON gd.generation_unit_id = gu.id
        WHERE gd.windfarm_id IS NULL
            AND gd.source = 'ENTSOE'
        GROUP BY gu.id, gu.name
        ORDER BY orphaned_records DESC
    """)

    result = await session.execute(query)
    units = result.fetchall()

    logger.info(f"Found {len(units)} ENTSOE generation units with orphaned data")

    stats = {
        'total_units': len(units),
        'total_orphaned_records': sum(u.orphaned_records for u in units),
        'units': [{'id': u.id, 'name': u.name, 'records': u.orphaned_records} for u in units[:10]]
    }

    return stats


async def create_mappings(session: AsyncSession, dry_run: bool = True) -> int:
    """Create generation_unit_mapping records."""
    created_count = 0

    for windfarm_name, (windfarm_id, unit_names) in ENTSOE_WINDFARM_MAPPINGS.items():
        logger.info(f"Processing windfarm: {windfarm_name} (ID: {windfarm_id})")

        for unit_name in unit_names:
            # Find generation unit by name
            unit_query = text("""
                SELECT id FROM generation_units
                WHERE name = :unit_name AND source = 'ENTSOE'
            """)
            result = await session.execute(unit_query, {'unit_name': unit_name})
            unit = result.fetchone()

            if not unit:
                logger.warning(f"  Generation unit not found: {unit_name}")
                continue

            # Check if mapping already exists
            check_query = text("""
                SELECT id FROM generation_unit_mapping
                WHERE generation_unit_id = :unit_id
                    AND windfarm_id = :windfarm_id
            """)
            result = await session.execute(
                check_query,
                {'unit_id': unit.id, 'windfarm_id': windfarm_id}
            )
            existing = result.fetchone()

            if existing:
                logger.info(f"  Mapping already exists for {unit_name}")
                continue

            if not dry_run:
                # Insert new mapping
                insert_query = text("""
                    INSERT INTO generation_unit_mapping
                    (source, source_identifier, generation_unit_id, windfarm_id, is_active)
                    VALUES ('ENTSOE', :identifier, :unit_id, :windfarm_id, true)
                """)
                await session.execute(
                    insert_query,
                    {
                        'identifier': f'ENTSOE:{unit_name}',
                        'unit_id': unit.id,
                        'windfarm_id': windfarm_id
                    }
                )
                created_count += 1
                logger.info(f"  ✓ Created mapping for {unit_name} -> {windfarm_name}")
            else:
                created_count += 1
                logger.info(f"  [DRY RUN] Would create mapping for {unit_name} -> {windfarm_name}")

    if not dry_run:
        await session.commit()

    return created_count


async def update_orphaned_data(session: AsyncSession, dry_run: bool = True) -> int:
    """Update orphaned generation_data records with windfarm_id."""

    # Count records to update
    count_query = text("""
        SELECT COUNT(*) as count
        FROM generation_data gd
        INNER JOIN generation_unit_mapping gum ON gd.generation_unit_id = gum.generation_unit_id
        WHERE gd.windfarm_id IS NULL
            AND gd.source = 'ENTSOE'
            AND gum.windfarm_id IS NOT NULL
    """)
    result = await session.execute(count_query)
    total_to_update = result.fetchone().count

    logger.info(f"Found {total_to_update:,} ENTSOE records to update")

    if not dry_run and total_to_update > 0:
        # Update in batches to avoid locking
        batch_size = 10000
        updated = 0

        while updated < total_to_update:
            update_query = text("""
                UPDATE generation_data gd
                SET windfarm_id = gum.windfarm_id,
                    updated_at = NOW()
                FROM generation_unit_mapping gum
                WHERE gd.generation_unit_id = gum.generation_unit_id
                    AND gd.windfarm_id IS NULL
                    AND gd.source = 'ENTSOE'
                    AND gum.windfarm_id IS NOT NULL
                    AND gd.id IN (
                        SELECT id FROM generation_data
                        WHERE windfarm_id IS NULL
                            AND source = 'ENTSOE'
                        LIMIT :batch_size
                    )
            """)

            result = await session.execute(update_query, {'batch_size': batch_size})
            batch_updated = result.rowcount
            updated += batch_updated

            await session.commit()
            logger.info(f"  Updated {updated:,}/{total_to_update:,} records ({updated*100//total_to_update}%)")

            if batch_updated < batch_size:
                break

    return total_to_update


async def verify_results(session: AsyncSession):
    """Verify the migration results."""

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

    # Count windfarms with ENTSOE data
    query = text("""
        SELECT COUNT(DISTINCT windfarm_id) as windfarm_count
        FROM generation_data
        WHERE source = 'ENTSOE'
            AND windfarm_id IS NOT NULL
    """)
    result = await session.execute(query)
    windfarm_count = result.fetchone().windfarm_count

    logger.info(f"ENTSOE windfarms with data: {windfarm_count}")

    return {
        'orphaned_records': stats.orphaned_count,
        'orphaned_units': stats.orphaned_units,
        'windfarms_with_data': windfarm_count
    }


async def main():
    """Main migration function."""
    import argparse

    parser = argparse.ArgumentParser(description='Fix ENTSOE windfarm mappings')
    parser.add_argument('--dry-run', action='store_true',
                       help='Run in dry-run mode (no database changes)')
    parser.add_argument('--skip-analysis', action='store_true',
                       help='Skip initial analysis')
    parser.add_argument('--skip-update', action='store_true',
                       help='Skip updating generation_data records')

    args = parser.parse_args()

    logger.info("=== ENTSOE Windfarm Mapping Migration ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    async with get_db_session() as session:
        # Step 1: Analyze current state
        if not args.skip_analysis:
            logger.info("\nStep 1: Analyzing current state...")
            stats = await analyze_orphaned_units(session)
            logger.info(f"  Total orphaned units: {stats['total_units']}")
            logger.info(f"  Total orphaned records: {stats['total_orphaned_records']:,}")

        # Step 2: Create mappings
        logger.info("\nStep 2: Creating generation_unit_mapping records...")
        created = await create_mappings(session, dry_run=args.dry_run)
        logger.info(f"  {'Would create' if args.dry_run else 'Created'} {created} mappings")

        # Step 3: Update orphaned data
        if not args.skip_update:
            logger.info("\nStep 3: Updating orphaned generation_data records...")
            updated = await update_orphaned_data(session, dry_run=args.dry_run)
            logger.info(f"  {'Would update' if args.dry_run else 'Updated'} {updated:,} records")

        # Step 4: Verify results
        if not args.dry_run:
            logger.info("\nStep 4: Verifying results...")
            results = await verify_results(session)

            if results['orphaned_records'] == 0:
                logger.info("✅ Migration successful! All ENTSOE records now have windfarm_id")
            else:
                logger.warning(f"⚠️ {results['orphaned_records']:,} records still orphaned")

    logger.info("\n=== Migration Complete ===")


if __name__ == "__main__":
    asyncio.run(main())