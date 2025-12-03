"""Fast bulk processing of raw price data to windfarm-level price_data table.

Uses SQL INSERT...SELECT for maximum performance - processes all windfarms
in a single SQL operation per bidzone instead of Python loops.

Usage:
    poetry run python scripts/seeds/power_prices/process_bulk_prices.py
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from sqlalchemy import text


async def process_bulk_prices():
    """Process all raw prices to windfarm-level data using bulk SQL operations."""
    print("=" * 60)
    print("Bulk Process Price Data to Windfarm-Level")
    print("=" * 60)

    start_time = datetime.now()
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Get all bidzone codes that have raw price data
        result = await db.execute(text("""
            SELECT DISTINCT pdr.identifier as bidzone_code, b.id as bidzone_id
            FROM price_data_raw pdr
            JOIN bidzones b ON b.code = pdr.identifier
            WHERE pdr.source = 'ENTSOE'
            ORDER BY bidzone_code
        """))
        bidzones = [(row[0], row[1]) for row in result.fetchall()]

        print(f"\nFound {len(bidzones)} bidzones with raw price data")

        # Get windfarms that need processing (don't have price_data yet)
        result = await db.execute(text("""
            SELECT w.id, w.name, b.code
            FROM windfarms w
            JOIN bidzones b ON w.bidzone_id = b.id
            WHERE w.bidzone_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM price_data pd WHERE pd.windfarm_id = w.id
              )
            ORDER BY b.code, w.name
        """))
        unprocessed = result.fetchall()
        print(f"Found {len(unprocessed)} windfarms that need processing")

        if not unprocessed:
            print("\nAll windfarms already have price data!")
            return

        # Group by bidzone for display
        by_bidzone = {}
        for row in unprocessed:
            code = row[2]
            if code not in by_bidzone:
                by_bidzone[code] = []
            by_bidzone[code].append(row[1])

        print(f"\nBidzones to process: {len(by_bidzone)}")
        for code, farms in list(by_bidzone.items())[:5]:
            print(f"  {code}: {len(farms)} windfarms")
        if len(by_bidzone) > 5:
            print(f"  ... and {len(by_bidzone) - 5} more")

        # Process each bidzone with bulk SQL
        total_inserted = 0
        for bidzone_code, bidzone_id in bidzones:
            if bidzone_code not in by_bidzone:
                continue

            farms_count = len(by_bidzone[bidzone_code])
            print(f"\nProcessing {bidzone_code} ({farms_count} windfarms)...")

            # Use INSERT...SELECT to bulk insert all windfarm prices for this bidzone
            # This is MUCH faster than Python loops
            # NOTE: DATE_TRUNC rounds 15-minute data (PT15M) to hourly boundaries
            # and AVG aggregates multiple values per hour
            result = await db.execute(text("""
                INSERT INTO price_data (
                    id, hour, windfarm_id, bidzone_id,
                    day_ahead_price, intraday_price, currency, source,
                    raw_data_ids, quality_flag, created_at, updated_at
                )
                SELECT
                    gen_random_uuid(),
                    DATE_TRUNC('hour', pdr.period_start) AS hour,
                    w.id,
                    w.bidzone_id,
                    AVG(CASE WHEN pdr.price_type = 'day_ahead' THEN pdr.value_extracted END),
                    AVG(CASE WHEN pdr.price_type = 'intraday' THEN pdr.value_extracted END),
                    'EUR',
                    'ENTSOE',
                    ARRAY_AGG(pdr.id),
                    CASE
                        WHEN MAX(CASE WHEN pdr.price_type = 'day_ahead' THEN 1 ELSE 0 END) = 1
                        THEN 'good'
                        ELSE 'partial'
                    END,
                    NOW(),
                    NOW()
                FROM price_data_raw pdr
                CROSS JOIN windfarms w
                WHERE pdr.source = 'ENTSOE'
                  AND pdr.identifier = :bidzone_code
                  AND w.bidzone_id = :bidzone_id
                  AND NOT EXISTS (
                      SELECT 1 FROM price_data pd
                      WHERE pd.windfarm_id = w.id
                  )
                GROUP BY DATE_TRUNC('hour', pdr.period_start), w.id, w.bidzone_id
                ON CONFLICT (hour, windfarm_id, source)
                DO UPDATE SET
                    day_ahead_price = EXCLUDED.day_ahead_price,
                    intraday_price = EXCLUDED.intraday_price,
                    raw_data_ids = EXCLUDED.raw_data_ids,
                    quality_flag = EXCLUDED.quality_flag,
                    updated_at = NOW()
            """), {"bidzone_code": bidzone_code, "bidzone_id": bidzone_id})

            await db.commit()

            # Get count of inserted records
            result = await db.execute(text("""
                SELECT COUNT(*) FROM price_data pd
                JOIN windfarms w ON pd.windfarm_id = w.id
                WHERE w.bidzone_id = :bidzone_id
            """), {"bidzone_id": bidzone_id})
            count = result.scalar()

            inserted = result.rowcount if hasattr(result, 'rowcount') and result.rowcount else count
            print(f"  Inserted/updated records for {bidzone_code}: {count:,}")
            total_inserted += inserted

        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "=" * 60)
        print("PROCESSING COMPLETE")
        print("=" * 60)

        # Get final counts
        result = await db.execute(text("SELECT COUNT(DISTINCT windfarm_id), COUNT(*) FROM price_data"))
        row = result.fetchone()

        print(f"Total windfarms with price data: {row[0]}")
        print(f"Total price_data records: {row[1]:,}")
        print(f"Duration: {duration:.1f} seconds")
        print("=" * 60)


if __name__ == '__main__':
    asyncio.run(process_bulk_prices())
