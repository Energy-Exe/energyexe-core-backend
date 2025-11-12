"""
Fast cleanup script to remove ELEXON records that aren't the latest settlement run.

This is a simplified version that skips the detailed analysis and directly
performs the cleanup for better performance on large datasets.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from sqlalchemy import text


async def cleanup_fast():
    """Perform fast cleanup without detailed analysis."""

    print("""
╔════════════════════════════════════════════════════════════════════════════╗
║              FAST ELEXON SETTLEMENT RUN CLEANUP SCRIPT                     ║
╚════════════════════════════════════════════════════════════════════════════╝

This script removes ELEXON records that are not the latest settlement run
for each settlement period.

""")

    AsyncSessionLocal = get_session_factory()

    # Quick count of ELEXON records
    print("📊 Checking database...")
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("""
            SELECT COUNT(*) FROM generation_data_raw WHERE source = 'ELEXON'
        """))
        total_before = result.scalar()
        print(f"   Total ELEXON records: {total_before:,}")

    response = input(f"\n⚠️  This will delete records that aren't the max cdca_run_number per settlement period.\nContinue? (type 'yes' to confirm): ")

    if response.lower() != 'yes':
        print("\n❌ Cleanup cancelled")
        return

    print("\n🗑️  Deleting non-latest settlement runs...")
    print("   (This may take a few minutes for large datasets)")
    start_time = datetime.now()

    async with AsyncSessionLocal() as db:
        # Direct delete using a more efficient query
        # Delete all records where a higher run number exists for the same period
        result = await db.execute(text("""
            DELETE FROM generation_data_raw gdr
            WHERE source = 'ELEXON'
            AND EXISTS (
                SELECT 1
                FROM generation_data_raw gdr2
                WHERE gdr2.source = 'ELEXON'
                AND gdr2.identifier = gdr.identifier
                AND gdr2.data->>'settlement_date' = gdr.data->>'settlement_date'
                AND gdr2.data->>'settlement_period' = gdr.data->>'settlement_period'
                AND (gdr2.data->>'cdca_run_number')::int > (gdr.data->>'cdca_run_number')::int
            )
        """))

        await db.commit()

        # Get final count
        result = await db.execute(text("""
            SELECT COUNT(*) FROM generation_data_raw WHERE source = 'ELEXON'
        """))
        total_after = result.scalar()

    duration = (datetime.now() - start_time).total_seconds()
    deleted = total_before - total_after

    print(f"\n✅ Cleanup complete in {duration:.1f} seconds")
    print(f"\n📊 Results:")
    print(f"   • Records before: {total_before:,}")
    print(f"   • Records after:  {total_after:,}")
    print(f"   • Records deleted: {deleted:,}")
    print(f"   • Percentage removed: {deleted/total_before*100:.1f}%")

    print("\n" + "="*80)
    print(" "*25 + "✨ CLEANUP COMPLETE ✨")
    print("="*80)
    print("\nAll ELEXON records now use the latest (highest) settlement run number.")


if __name__ == "__main__":
    asyncio.run(cleanup_fast())
