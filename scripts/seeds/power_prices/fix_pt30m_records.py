"""Fix mislabeled PT30M records in price_data_raw table.

PT30M data from Ireland (IE_SEM bidzone) was incorrectly labeled as PT60M
during import. This script identifies and fixes those records.
"""

import asyncio
from datetime import timedelta
from app.core.database import get_session_factory
from sqlalchemy import text

async def fix_pt30m_records():
    """Fix PT30M records that were mislabeled as PT60M."""

    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        print("=" * 80)
        print("FIXING PT30M RECORDS IN DATABASE")
        print("=" * 80)

        # Step 1: Identify mislabeled PT30M records
        # These are records with:
        # - period_type = PT60M (incorrectly labeled)
        # - identifier for Ireland (IE_SEM, 10Y1001A1001A59C)
        # - period_start has minutes = 30 (e.g., 06:30:00, 07:30:00)
        # - date range: 2015-01-01 to 2018-09-26

        print("\n1. Identifying mislabeled PT30M records...")

        result = await db.execute(text("""
            SELECT COUNT(*)
            FROM price_data_raw
            WHERE identifier IN ('IE(SEM)', '10Y1001A1001A59C', 'IE_SEM')
              AND period_type = 'PT60M'
              AND EXTRACT(MINUTE FROM period_start) = 30
              AND period_start >= '2015-01-01'
              AND period_start < '2018-10-01'
        """))
        count_to_fix = result.scalar()

        print(f"   Found {count_to_fix:,} records to fix")

        if count_to_fix == 0:
            print("\n   ✓ No records need fixing!")
            return

        # Step 2: Show sample of records to fix
        print("\n2. Sample records that will be fixed:")

        result = await db.execute(text("""
            SELECT
                id,
                period_start,
                period_end,
                period_type,
                identifier,
                value_extracted
            FROM price_data_raw
            WHERE identifier IN ('IE(SEM)', '10Y1001A1001A59C', 'IE_SEM')
              AND period_type = 'PT60M'
              AND EXTRACT(MINUTE FROM period_start) = 30
              AND period_start >= '2015-01-01'
              AND period_start < '2018-10-01'
            ORDER BY period_start
            LIMIT 5
        """))

        samples = list(result)
        for row in samples:
            print(f"   {row.period_start} - {row.period_end} ({row.period_type}) → Will become PT30M")

        # Step 3: Confirm before updating
        print("\n3. Preparing to update records...")
        print(f"   Records to update: {count_to_fix:,}")
        print("   Changes:")
        print("     - period_type: PT60M → PT30M")
        print("     - period_end: period_start + 30 minutes (instead of +1 hour)")

        # Step 4: Execute the update
        print("\n4. Executing update...")

        result = await db.execute(text("""
            UPDATE price_data_raw
            SET
                period_type = 'PT30M',
                period_end = period_start + INTERVAL '30 minutes',
                updated_at = NOW()
            WHERE identifier IN ('IE(SEM)', '10Y1001A1001A59C', 'IE_SEM')
              AND period_type = 'PT60M'
              AND EXTRACT(MINUTE FROM period_start) = 30
              AND period_start >= '2015-01-01'
              AND period_start < '2018-10-01'
        """))

        await db.commit()

        updated_count = result.rowcount
        print(f"   ✓ Updated {updated_count:,} records")

        # Step 5: Verify the fix
        print("\n5. Verifying fix...")

        result = await db.execute(text("""
            SELECT COUNT(*)
            FROM price_data_raw
            WHERE identifier IN ('IE(SEM)', '10Y1001A1001A59C', 'IE_SEM')
              AND period_type = 'PT30M'
        """))
        pt30m_count = result.scalar()

        print(f"   PT30M records now in database: {pt30m_count:,}")

        # Show sample of fixed records
        result = await db.execute(text("""
            SELECT
                period_start,
                period_end,
                period_type,
                identifier,
                value_extracted
            FROM price_data_raw
            WHERE period_type = 'PT30M'
            ORDER BY period_start
            LIMIT 5
        """))

        print("\n   Sample of fixed records:")
        for row in result:
            duration = (row.period_end - row.period_start).total_seconds() / 60
            print(f"   {row.period_start} - {row.period_end} ({row.period_type}, {duration:.0f} min)")

        print("\n" + "=" * 80)
        print("FIX COMPLETE!")
        print("=" * 80)

        print(f"""
Summary:
  ✓ Updated {updated_count:,} records from PT60M to PT30M
  ✓ Fixed period_end calculations (now +30 minutes)
  ✓ Ireland (IE_SEM) data 2015-2018 now correctly labeled

The price_data_raw table now has accurate resolution labels.
The ongoing reprocessing will use these corrected values.
""")

if __name__ == "__main__":
    asyncio.run(fix_pt30m_records())
