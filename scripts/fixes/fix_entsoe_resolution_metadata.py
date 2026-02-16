#!/usr/bin/env python3
"""
Fix ENTSOE Resolution Metadata

Updates generation_data_raw records that were stored with incorrect 'PT60M' metadata
when the actual data was PT15M (15-minute intervals). This happens because entsoe-py
doesn't include a resolution_code column — it was always defaulting to 'PT60M'.

Detection: if more than 1 record exists per (identifier, hour), the data is sub-hourly.

Usage:
    cd /Users/mdfaisal/Documents/energyexe/energyexe-core-backend
    poetry run python scripts/fixes/fix_entsoe_resolution_metadata.py
    poetry run python scripts/fixes/fix_entsoe_resolution_metadata.py --dry-run
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import text
from app.core.database import get_session_factory

import structlog
logger = structlog.get_logger()


async def fix_resolution_metadata(dry_run: bool = False):
    """Fix resolution metadata for ENTSOE raw records."""
    session_factory = get_session_factory()

    async with session_factory() as db:
        # Step 1: Find identifiers+hours with multiple records (sub-hourly data marked as PT60M)
        print("Step 1: Finding mis-labeled sub-hourly data...")

        count_stmt = text("""
            SELECT identifier,
                   date_trunc('hour', period_start) AS hour,
                   COUNT(*) AS records_per_hour
            FROM generation_data_raw
            WHERE source = 'ENTSOE'
              AND source_type = 'api'
              AND period_type = 'PT60M'
            GROUP BY identifier, date_trunc('hour', period_start)
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """)
        result = await db.execute(count_stmt)
        sample_rows = result.fetchall()

        if not sample_rows:
            print("No mis-labeled records found. Nothing to fix.")
            return

        print(f"Found sample groups with multiple records per hour:")
        for r in sample_rows[:5]:
            print(f"  {r.identifier} at {r.hour}: {r.records_per_hour} records")

        # Step 2: Count total affected records
        total_stmt = text("""
            SELECT COUNT(*) AS total
            FROM generation_data_raw gdr
            WHERE gdr.source = 'ENTSOE'
              AND gdr.source_type = 'api'
              AND gdr.period_type = 'PT60M'
              AND EXISTS (
                  SELECT 1
                  FROM generation_data_raw gdr2
                  WHERE gdr2.source = gdr.source
                    AND gdr2.source_type = gdr.source_type
                    AND gdr2.identifier = gdr.identifier
                    AND date_trunc('hour', gdr2.period_start) = date_trunc('hour', gdr.period_start)
                    AND gdr2.id != gdr.id
                    AND gdr2.period_type = 'PT60M'
              )
        """)
        total_result = await db.execute(total_stmt)
        total_row = total_result.fetchone()
        total_affected = total_row.total if total_row else 0

        print(f"\nTotal affected records: {total_affected:,}")

        if dry_run:
            print("\nDRY RUN — no changes made.")
            return

        # Step 3: Get distinct identifiers to batch process
        print("\nStep 3: Getting distinct identifiers...")

        id_stmt = text("""
            SELECT DISTINCT identifier
            FROM generation_data_raw
            WHERE source = 'ENTSOE'
              AND source_type = 'api'
              AND period_type = 'PT60M'
        """)
        id_result = await db.execute(id_stmt)
        identifiers = [row.identifier for row in id_result.fetchall()]
        print(f"Found {len(identifiers)} identifiers to check")

        total_pt15m = 0
        total_pt30m = 0

        for i, ident in enumerate(identifiers):
            # PT15M: hours with >= 3 records
            update_15m_stmt = text("""
                UPDATE generation_data_raw
                SET period_type = 'PT15M',
                    period_end = period_start + interval '15 minutes',
                    data = jsonb_set(
                        COALESCE(data, '{}'::jsonb),
                        '{resolution_code}',
                        '"PT15M"'
                    ),
                    updated_at = NOW()
                WHERE id IN (
                    SELECT gdr.id
                    FROM generation_data_raw gdr
                    JOIN (
                        SELECT identifier, date_trunc('hour', period_start) as hr
                        FROM generation_data_raw
                        WHERE source = 'ENTSOE' AND source_type = 'api'
                          AND period_type = 'PT60M' AND identifier = :ident
                        GROUP BY identifier, date_trunc('hour', period_start)
                        HAVING COUNT(*) >= 3
                    ) sub ON gdr.identifier = sub.identifier
                        AND date_trunc('hour', gdr.period_start) = sub.hr
                    WHERE gdr.source = 'ENTSOE' AND gdr.source_type = 'api'
                      AND gdr.period_type = 'PT60M' AND gdr.identifier = :ident
                )
            """)
            r15 = await db.execute(update_15m_stmt, {"ident": ident})
            total_pt15m += r15.rowcount

            # PT30M: hours with exactly 2 records
            update_30m_stmt = text("""
                UPDATE generation_data_raw
                SET period_type = 'PT30M',
                    period_end = period_start + interval '30 minutes',
                    data = jsonb_set(
                        COALESCE(data, '{}'::jsonb),
                        '{resolution_code}',
                        '"PT30M"'
                    ),
                    updated_at = NOW()
                WHERE id IN (
                    SELECT gdr.id
                    FROM generation_data_raw gdr
                    JOIN (
                        SELECT identifier, date_trunc('hour', period_start) as hr
                        FROM generation_data_raw
                        WHERE source = 'ENTSOE' AND source_type = 'api'
                          AND period_type = 'PT60M' AND identifier = :ident
                        GROUP BY identifier, date_trunc('hour', period_start)
                        HAVING COUNT(*) = 2
                    ) sub ON gdr.identifier = sub.identifier
                        AND date_trunc('hour', gdr.period_start) = sub.hr
                    WHERE gdr.source = 'ENTSOE' AND gdr.source_type = 'api'
                      AND gdr.period_type = 'PT60M' AND gdr.identifier = :ident
                )
            """)
            r30 = await db.execute(update_30m_stmt, {"ident": ident})
            total_pt30m += r30.rowcount

            if (i + 1) % 5 == 0 or i == len(identifiers) - 1:
                await db.commit()
                print(f"  Processed {i + 1}/{len(identifiers)} identifiers... (PT15M: {total_pt15m}, PT30M: {total_pt30m})")

        await db.commit()
        print(f"\nUpdated {total_pt15m:,} records to PT15M")
        print(f"Updated {total_pt30m:,} records to PT30M")

        print("\nDone! Next step: re-aggregate affected date ranges:")
        print("  poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py --source ENTSOE --date <DATE>")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix ENTSOE resolution metadata')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without modifying data')
    args = parser.parse_args()

    asyncio.run(fix_resolution_metadata(dry_run=args.dry_run))
