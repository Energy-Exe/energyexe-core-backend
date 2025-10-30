"""
Cleanup script to remove ELEXON records that aren't the latest settlement run.

This script identifies and removes records from generation_data_raw where:
- Source is ELEXON
- The cdca_run_number is NOT the maximum for that BMU/date/period combination

This ensures we only keep the most accurate/final settlement data.
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


async def analyze_mixed_runs():
    """Analyze how many records have mixed settlement runs."""
    print("\n" + "="*80)
    print(" "*20 + "🔍 ANALYZING MIXED SETTLEMENT RUNS 🔍")
    print("="*80)

    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        # Count records that would be deleted
        result = await db.execute(text("""
            WITH latest_runs AS (
                SELECT
                    identifier,
                    data->>'settlement_date' as settlement_date,
                    data->>'settlement_period' as settlement_period,
                    MAX((data->>'cdca_run_number')::int) as max_run
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                GROUP BY identifier, data->>'settlement_date', data->>'settlement_period'
            )
            SELECT
                COUNT(*) as total_to_delete
            FROM generation_data_raw r
            INNER JOIN latest_runs lr ON (
                r.identifier = lr.identifier
                AND r.data->>'settlement_date' = lr.settlement_date
                AND r.data->>'settlement_period' = lr.settlement_period
                AND (r.data->>'cdca_run_number')::int < lr.max_run
            )
            WHERE r.source = 'ELEXON'
        """))
        to_delete = result.scalar()

        # Count total ELEXON records
        result = await db.execute(text("""
            SELECT COUNT(*) FROM generation_data_raw WHERE source = 'ELEXON'
        """))
        total = result.scalar()

        # Count unique periods (how many records should remain)
        result = await db.execute(text("""
            SELECT COUNT(DISTINCT (identifier, data->>'settlement_date', data->>'settlement_period'))
            FROM generation_data_raw
            WHERE source = 'ELEXON'
        """))
        unique_periods = result.scalar()

        print(f"\n📊 Current State:")
        print(f"  • Total ELEXON records: {total:,}")
        print(f"  • Unique settlement periods: {unique_periods:,}")
        print(f"  • Records with non-max run numbers: {to_delete:,}")
        print(f"  • Records to keep (latest runs): {total - to_delete:,}")

        if to_delete > 0:
            print(f"\n⚠️  Will delete {to_delete:,} records ({to_delete/total*100:.1f}% of ELEXON data)")
            print(f"✅ Will keep {total - to_delete:,} records ({(total-to_delete)/total*100:.1f}% of ELEXON data)")
        else:
            print(f"\n✅ No cleanup needed - all records are already latest runs!")

        # Show some examples of what would be deleted
        if to_delete > 0:
            result = await db.execute(text("""
                WITH latest_runs AS (
                    SELECT
                        identifier,
                        data->>'settlement_date' as settlement_date,
                        data->>'settlement_period' as settlement_period,
                        MAX((data->>'cdca_run_number')::int) as max_run
                    FROM generation_data_raw
                    WHERE source = 'ELEXON'
                    GROUP BY identifier, data->>'settlement_date', data->>'settlement_period'
                )
                SELECT
                    r.identifier,
                    r.data->>'settlement_date' as date,
                    r.data->>'settlement_period' as period,
                    (r.data->>'cdca_run_number')::int as run,
                    lr.max_run,
                    COUNT(*) as count
                FROM generation_data_raw r
                INNER JOIN latest_runs lr ON (
                    r.identifier = lr.identifier
                    AND r.data->>'settlement_date' = lr.settlement_date
                    AND r.data->>'settlement_period' = lr.settlement_period
                    AND (r.data->>'cdca_run_number')::int < lr.max_run
                )
                WHERE r.source = 'ELEXON'
                GROUP BY r.identifier, r.data->>'settlement_date', r.data->>'settlement_period',
                         (r.data->>'cdca_run_number')::int, lr.max_run
                LIMIT 10
            """))
            examples = result.fetchall()

            print(f"\n📋 Example records to be deleted (showing 10):")
            print(f"{'BMU ID':<15} {'Date':<10} {'Period':<8} {'Run':<6} {'Max Run':<8}")
            print("-" * 60)
            for ex in examples:
                print(f"{ex.identifier:<15} {ex.date:<10} {ex.period:<8} {ex.run:<6} {ex.max_run:<8}")

        return to_delete


async def cleanup_mixed_runs(dry_run=True):
    """Remove records that aren't the latest settlement run."""

    to_delete = await analyze_mixed_runs()

    if to_delete == 0:
        print("\n✅ No cleanup needed!")
        return

    print("\n" + "="*80)

    if dry_run:
        print(" "*25 + "🔍 DRY RUN MODE 🔍")
        print("="*80)
        print("\nThis was a DRY RUN - no data was deleted.")
        print("Run with --execute flag to actually delete the records.")
        return

    print(" "*22 + "⚠️  EXECUTING CLEANUP ⚠️")
    print("="*80)

    response = input(f"\n⚠️  Are you sure you want to delete {to_delete:,} records? (type 'yes' to confirm): ")

    if response.lower() != 'yes':
        print("\n❌ Cleanup cancelled")
        return

    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        print("\n🗑️  Deleting non-latest settlement runs...")
        start_time = datetime.now()

        # Delete records that aren't the max run for their period
        result = await db.execute(text("""
            WITH latest_runs AS (
                SELECT
                    identifier,
                    data->>'settlement_date' as settlement_date,
                    data->>'settlement_period' as settlement_period,
                    MAX((data->>'cdca_run_number')::int) as max_run
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                GROUP BY identifier, data->>'settlement_date', data->>'settlement_period'
            )
            DELETE FROM generation_data_raw
            WHERE id IN (
                SELECT r.id
                FROM generation_data_raw r
                INNER JOIN latest_runs lr ON (
                    r.identifier = lr.identifier
                    AND r.data->>'settlement_date' = lr.settlement_date
                    AND r.data->>'settlement_period' = lr.settlement_period
                    AND (r.data->>'cdca_run_number')::int < lr.max_run
                )
                WHERE r.source = 'ELEXON'
            )
        """))

        await db.commit()

        duration = (datetime.now() - start_time).total_seconds()

        print(f"✅ Deleted {to_delete:,} records in {duration:.1f} seconds")

        # Verify final state
        result = await db.execute(text("""
            SELECT COUNT(*) FROM generation_data_raw WHERE source = 'ELEXON'
        """))
        remaining = result.scalar()

        print(f"\n📊 Final State:")
        print(f"  • Remaining ELEXON records: {remaining:,}")
        print(f"  • All records are now latest settlement runs ✅")

    print("\n" + "="*80)
    print(" "*25 + "✨ CLEANUP COMPLETE ✨")
    print("="*80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Cleanup ELEXON data to keep only latest settlement runs'
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually execute the cleanup (default is dry-run)'
    )

    args = parser.parse_args()

    print("""
╔════════════════════════════════════════════════════════════════════════════╗
║                  ELEXON SETTLEMENT RUN CLEANUP SCRIPT                      ║
╚════════════════════════════════════════════════════════════════════════════╝

This script removes ELEXON records that are not the latest settlement run
for each settlement period. ELEXON data typically has multiple settlement
runs (e.g., run 19, 20) with later runs being more accurate/final.

""")

    asyncio.run(cleanup_mixed_runs(dry_run=not args.execute))
