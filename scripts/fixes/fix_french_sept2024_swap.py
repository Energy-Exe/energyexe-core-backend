#!/usr/bin/env python3
"""
Fix French September 2024 Generation/Consumption Swap

RTE confirmed that ENTSOE labels are incorrect for French wind farms in September 2024:
'Actual Aggregated' (generation) and 'Actual Consumption' values are swapped.

This script swaps source_type between 'api' ↔ 'api_consumption' for affected records.
For Excel-imported records, it swaps the value_extracted values.

Depends on Issue 4 consumption infrastructure being in place first.

Usage:
    cd /Users/mdfaisal/Documents/energyexe/energyexe-core-backend
    poetry run python scripts/fixes/fix_french_sept2024_swap.py
    poetry run python scripts/fixes/fix_french_sept2024_swap.py --dry-run
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import text
from app.core.database import get_session_factory

import structlog
logger = structlog.get_logger()

# Affected period
SWAP_START = datetime(2024, 9, 1, tzinfo=timezone.utc)
SWAP_END = datetime(2024, 10, 1, tzinfo=timezone.utc)


async def fix_french_swap(dry_run: bool = False):
    """Swap generation and consumption source_types for French wind Sept 2024."""
    session_factory = get_session_factory()

    async with session_factory() as db:
        # Step 1: Find affected French ENTSOE records
        print("Step 1: Identifying affected French ENTSOE records...")

        count_stmt = text("""
            SELECT gdr.source_type, COUNT(*) AS cnt
            FROM generation_data_raw gdr
            JOIN generation_units gu ON gdr.identifier = gu.code AND gu.source = 'ENTSOE'
            JOIN windfarms wf ON gu.windfarm_id = wf.id
            JOIN countries c ON wf.country_id = c.id
            WHERE gdr.source = 'ENTSOE'
              AND c.code = 'FRA'
              AND gdr.period_start >= :start_date
              AND gdr.period_start < :end_date
              AND gdr.source_type IN ('api', 'api_consumption', 'excel', 'excel_consumption')
            GROUP BY gdr.source_type
        """)
        result = await db.execute(count_stmt, {"start_date": SWAP_START, "end_date": SWAP_END})
        rows = result.fetchall()

        if not rows:
            print("No affected records found. Nothing to fix.")
            return

        for r in rows:
            print(f"  {r.source_type}: {r.cnt} records")

        if dry_run:
            print("\nDRY RUN — no changes made.")
            return

        # Step 2: Swap API records: api ↔ api_consumption
        # Use a temp value to avoid unique constraint conflicts
        print("\nStep 2: Swapping API source_types...")

        # Mark api records as temp
        swap_1 = text("""
            UPDATE generation_data_raw gdr
            SET source_type = 'api_swap_temp',
                updated_at = NOW()
            FROM generation_units gu
            JOIN windfarms wf ON gu.windfarm_id = wf.id
            JOIN countries c ON wf.country_id = c.id
            WHERE gdr.identifier = gu.code
              AND gu.source = 'ENTSOE'
              AND gdr.source = 'ENTSOE'
              AND gdr.source_type = 'api'
              AND c.code = 'FRA'
              AND gdr.period_start >= :start_date
              AND gdr.period_start < :end_date
        """)
        r1 = await db.execute(swap_1, {"start_date": SWAP_START, "end_date": SWAP_END})
        print(f"  Marked {r1.rowcount} 'api' records as temp")

        # Rename api_consumption → api
        swap_2 = text("""
            UPDATE generation_data_raw gdr
            SET source_type = 'api',
                updated_at = NOW()
            FROM generation_units gu
            JOIN windfarms wf ON gu.windfarm_id = wf.id
            JOIN countries c ON wf.country_id = c.id
            WHERE gdr.identifier = gu.code
              AND gu.source = 'ENTSOE'
              AND gdr.source = 'ENTSOE'
              AND gdr.source_type = 'api_consumption'
              AND c.code = 'FRA'
              AND gdr.period_start >= :start_date
              AND gdr.period_start < :end_date
        """)
        r2 = await db.execute(swap_2, {"start_date": SWAP_START, "end_date": SWAP_END})
        print(f"  Swapped {r2.rowcount} 'api_consumption' → 'api'")

        # Rename temp → api_consumption
        swap_3 = text("""
            UPDATE generation_data_raw
            SET source_type = 'api_consumption',
                updated_at = NOW()
            WHERE source = 'ENTSOE'
              AND source_type = 'api_swap_temp'
        """)
        r3 = await db.execute(swap_3)
        print(f"  Swapped {r3.rowcount} temp → 'api_consumption'")

        await db.commit()

        # Step 2b: Swap Excel records: swap value_extracted with data->'actual_consumption_mw'
        # For Excel imports, value_extracted = ActualGenerationOutput (mislabeled as generation)
        # and data->actual_consumption_mw = ActualConsumption (mislabeled as consumption)
        # Since labels are swapped, we need to exchange these values
        print("\nStep 2b: Swapping Excel generation/consumption values...")

        swap_excel = text("""
            UPDATE generation_data_raw
            SET value_extracted = (data->>'actual_consumption_mw')::numeric,
                data = jsonb_set(
                    jsonb_set(
                        data,
                        '{actual_generation_output_mw}',
                        to_jsonb((data->>'actual_consumption_mw')::numeric)
                    ),
                    '{actual_consumption_mw}',
                    to_jsonb(COALESCE(value_extracted, 0::numeric))
                ),
                updated_at = NOW()
            WHERE id IN (
                SELECT gdr.id
                FROM generation_data_raw gdr
                JOIN generation_units gu ON gdr.identifier = gu.code AND gu.source = 'ENTSOE'
                JOIN windfarms wf ON gu.windfarm_id = wf.id
                JOIN countries c ON wf.country_id = c.id
                WHERE gdr.source = 'ENTSOE'
                  AND gdr.source_type = 'excel'
                  AND c.code = 'FRA'
                  AND gdr.period_start >= :start_date
                  AND gdr.period_start < :end_date
                  AND gdr.data IS NOT NULL
                  AND gdr.data->>'actual_consumption_mw' IS NOT NULL
                  AND (gdr.data->>'actual_consumption_mw')::numeric > 0
            )
        """)
        r_excel = await db.execute(swap_excel, {"start_date": SWAP_START, "end_date": SWAP_END})
        print(f"  Swapped {r_excel.rowcount} Excel records (value_extracted ↔ actual_consumption_mw)")

        await db.commit()

        # Step 3: Log the anomaly
        print("\nStep 3: Recording anomaly...")
        anomaly_stmt = text("""
            INSERT INTO data_anomalies (
                anomaly_type, severity, status,
                period_start, period_end,
                description, anomaly_metadata,
                detected_at, created_at, updated_at, is_active
            ) VALUES (
                'gen_consumption_swapped', 'high', 'resolved',
                :start_date, :end_date,
                'French ENTSOE wind data had generation and consumption labels swapped in September 2024 (confirmed by RTE)',
                :metadata,
                NOW(), NOW(), NOW(), true
            )
        """)
        import json
        await db.execute(anomaly_stmt, {
            "start_date": SWAP_START,
            "end_date": SWAP_END,
            "metadata": json.dumps({
                "country": "FR",
                "source": "ENTSOE",
                "fix_script": "fix_french_sept2024_swap.py",
                "records_swapped_api": r1.rowcount,
                "records_swapped_consumption": r2.rowcount,
                "records_swapped_excel": r_excel.rowcount,
            }),
        })
        await db.commit()

        print("\nDone! Next step: re-aggregate French data for September 2024")
        print("  poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py --source ENTSOE --date 2024-09-01")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix French Sept 2024 gen/consumption swap')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed')
    args = parser.parse_args()

    asyncio.run(fix_french_swap(dry_run=args.dry_run))
