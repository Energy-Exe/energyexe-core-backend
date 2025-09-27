#!/usr/bin/env python3
"""Fix ENTSOE and TAIPOWER mapping issues."""

import asyncio
from sqlalchemy import select, update, and_, text
from app.core.database import get_session_factory
from app.models.generation_unit import GenerationUnit
from app.models.generation_data import GenerationData

async def fix_mappings(dry_run=True):
    """Fix ENTSOE and TAIPOWER mappings."""

    session_factory = get_session_factory()
    async with session_factory() as db:
        print("\n" + "="*70)
        print("FIXING ENTSOE & TAIPOWER MAPPINGS")
        print("="*70)

        if dry_run:
            print("\n⚠️  DRY RUN MODE - No changes will be made")
            print("    Run with --apply to make changes\n")

        total_fixed = 0

        # ==================== FIX ENTSOE ====================
        print("\n🇪🇺 FIXING ENTSOE MAPPINGS:")

        entsoe_mappings = [
            # Beatrice
            {'pattern': '48W00000BEATO-%', 'windfarm_id': 7359, 'name': 'Beatrice'},

            # Dogger Bank (all units to same windfarm)
            {'pattern': '48W00000DBBWO-%', 'windfarm_id': 7369, 'name': 'Dogger Bank A&B'},
            {'pattern': '48W00000DBAWO-%', 'windfarm_id': 7369, 'name': 'Dogger Bank A&B'},

            # East Anglia One
            {'pattern': '48W000000EAAO-%', 'windfarm_id': 7371, 'name': 'East Anglia One'},

            # Hornsea (need to check which Hornsea)
            {'pattern': '48W00000HOWAO-%', 'windfarm_id': 7388, 'name': 'Hornsea 1'},
            {'pattern': '48W00000HOWBO-%', 'windfarm_id': 7389, 'name': 'Hornsea 2'},

            # Humber Gateway
            {'pattern': '48W00000HMGTO-%', 'windfarm_id': 7390, 'name': 'Humber Gateway'},

            # Moray East (MOWEO)
            {'pattern': '48W000000MOWEO%', 'windfarm_id': 7394, 'name': 'Moray East'},
            {'pattern': '48W00000MOWEO-%', 'windfarm_id': 7394, 'name': 'Moray East'},

            # Moray West (MOWWO)
            {'pattern': '48W00000MOWWO-%', 'windfarm_id': 7395, 'name': 'Moray West'},
            {'pattern': '48WW00000MOWWO-%', 'windfarm_id': 7395, 'name': 'Moray West'},

            # Neart Na Gaoithe
            {'pattern': '48W00000NNGAO-%', 'windfarm_id': 7396, 'name': 'Neart Na Gaoithe'},

            # Seagreen
            {'pattern': '48W00000SGRWO-%', 'windfarm_id': 7412, 'name': 'Seagreen'},

            # Thanet
            {'pattern': '48W00000THNTO-%', 'windfarm_id': 7420, 'name': 'Thanet'},
        ]

        for mapping in entsoe_mappings:
            # Find units matching the pattern
            units_query = select(GenerationUnit).where(
                and_(
                    GenerationUnit.source == 'ENTSOE',
                    GenerationUnit.code.like(mapping['pattern']),
                    GenerationUnit.windfarm_id.is_(None)
                )
            )

            result = await db.execute(units_query)
            units = result.scalars().all()

            if units:
                print(f"\n  Mapping {len(units)} units to {mapping['name']} (ID: {mapping['windfarm_id']})")
                for unit in units:
                    print(f"    - {unit.code}: {unit.name}")
                    if not dry_run:
                        unit.windfarm_id = mapping['windfarm_id']
                        db.add(unit)

                total_fixed += len(units)

        # ==================== CHECK TAIPOWER ====================
        print("\n\n🇹🇼 CHECKING TAIPOWER:")

        # TAIPOWER units already have windfarm_id, but let's check why data is orphaned
        taipower_check = text("""
            SELECT
                COUNT(DISTINCT gu.id) as total_units,
                COUNT(DISTINCT CASE WHEN gu.windfarm_id IS NOT NULL THEN gu.id END) as units_with_wf,
                COUNT(DISTINCT gd.id) as data_records,
                COUNT(DISTINCT CASE WHEN gd.windfarm_id IS NOT NULL THEN gd.id END) as data_with_wf
            FROM generation_units gu
            LEFT JOIN generation_data gd ON gu.id = gd.generation_unit_id
            WHERE gu.source IN ('Taipower', 'TAIPOWER')
        """)

        result = await db.execute(taipower_check)
        taipower_stats = result.first()

        print(f"\n  TAIPOWER Statistics:")
        print(f"    Total units: {taipower_stats.total_units}")
        print(f"    Units with windfarm_id: {taipower_stats.units_with_wf}")
        print(f"    Data records: {taipower_stats.data_records or 0}")
        print(f"    Data with windfarm_id: {taipower_stats.data_with_wf or 0}")

        if taipower_stats.data_records and taipower_stats.data_with_wf == 0:
            print(f"\n  ⚠️  TAIPOWER data exists but windfarm_id not propagated!")
            print(f"      This needs to be fixed by re-running aggregation")

            # Update existing TAIPOWER data to have windfarm_id
            if not dry_run:
                print(f"\n  Fixing TAIPOWER data windfarm_id...")

                fix_taipower_sql = text("""
                    UPDATE generation_data gd
                    SET windfarm_id = gu.windfarm_id
                    FROM generation_units gu
                    WHERE gd.generation_unit_id = gu.id
                    AND gd.source IN ('TAIPOWER', 'Taipower')
                    AND gd.windfarm_id IS NULL
                    AND gu.windfarm_id IS NOT NULL
                """)

                result = await db.execute(fix_taipower_sql)
                print(f"    Updated {result.rowcount} TAIPOWER records with windfarm_id")

        # ==================== SUMMARY ====================
        if not dry_run:
            await db.commit()
            print(f"\n✅ Fixed {total_fixed} ENTSOE unit mappings")
            print(f"✅ Updated TAIPOWER data")
        else:
            print(f"\n📊 Would fix {total_fixed} ENTSOE unit mappings")

        print(f"\n💡 NEXT STEPS:")
        print(f"1. Run this script with --apply to fix mappings")
        print(f"2. Re-run aggregation for ENTSOE:")
        print(f"   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \\")
        print(f"     --start 2015-01-01 --end 2024-12-31 --source ENTSOE")
        print(f"3. Re-run aggregation for TAIPOWER:")
        print(f"   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \\")
        print(f"     --start 2020-01-01 --end 2024-12-31 --source TAIPOWER")

if __name__ == "__main__":
    import sys
    dry_run = "--apply" not in sys.argv

    sys.path.insert(0, '/Users/mohammadfaisal/Documents/energyexe/energyexe-core-backend')
    asyncio.run(fix_mappings(dry_run=dry_run))