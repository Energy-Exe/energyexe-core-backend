#!/usr/bin/env python3
"""
Re-import ELEXON B1610 data for days with incomplete settlement period coverage.
V2: Processes one day at a time directly (no subprocess), with progress tracking.

Usage:
    poetry run python scripts/seeds/raw_generation_data/elexon/reimport_incomplete_days_v2.py
    poetry run python scripts/seeds/raw_generation_data/elexon/reimport_incomplete_days_v2.py --dry-run
    poetry run python scripts/seeds/raw_generation_data/elexon/reimport_incomplete_days_v2.py --start-from 2025-06-01
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import argparse
import asyncpg

current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.config import get_settings


async def find_incomplete_days():
    """Find all settlement days with significant incomplete B1610 data."""
    settings = get_settings()
    dsn = str(settings.DATABASE_URL).replace('postgresql+asyncpg://', 'postgresql://')
    conn = await asyncpg.connect(dsn)

    rows = await conn.fetch('''
        WITH daily_bmu_sp AS (
            SELECT
                r.data->>'settlement_date' as sd,
                r.identifier,
                COUNT(DISTINCT (r.data->>'settlement_period')::int) as sp_count
            FROM generation_data_raw r
            WHERE r.source = 'ELEXON' AND r.source_type = 'api'
              AND r.period_start >= '2025-01-01' AND r.period_start < '2026-01-01'
              AND r.data->>'settlement_date' IS NOT NULL
            GROUP BY r.data->>'settlement_date', r.identifier
        ),
        daily_stats AS (
            SELECT
                sd,
                COUNT(*) FILTER (WHERE sp_count < 46) as incomplete_bmus
            FROM daily_bmu_sp
            GROUP BY sd
        )
        SELECT sd
        FROM daily_stats
        WHERE incomplete_bmus > 10
        ORDER BY sd
    ''')

    await conn.close()
    return [r['sd'] for r in rows if r['sd'] is not None]


async def import_single_day(day_str, bm_units, client, dry_run=False):
    """Import B1610 data for a single day using the existing fetch logic."""
    from app.core.database import get_session_factory
    from app.models.generation_data import GenerationDataRaw
    from sqlalchemy.dialects.postgresql import insert
    from decimal import Decimal
    import pandas as pd

    start = datetime.strptime(day_str, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, tzinfo=timezone.utc
    )
    end = start.replace(hour=23, minute=59, second=59)

    bm_unit_codes = [u['code'] for u in bm_units]

    # Fetch from API with retry
    max_retries = 3
    df = None
    for attempt in range(max_retries):
        try:
            df, metadata = await asyncio.wait_for(
                client.fetch_physical_data(
                    start=start,
                    end=end,
                    bm_units=bm_unit_codes,
                ),
                timeout=300,  # 5 minute timeout per API call
            )
            if not df.empty:
                break
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                await asyncio.sleep(5)
            else:
                return 0, "API timeout after 3 retries"
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(5)
            else:
                return 0, f"API error: {str(e)[:100]}"

    if df is None or df.empty:
        return 0, "No data from API"

    if dry_run:
        return len(df), None

    # Store in database
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        unit_map = {u['code']: u for u in bm_units}
        total_stored = 0

        for bm_unit_code in df['bm_unit'].unique() if 'bm_unit' in df.columns else []:
            if bm_unit_code not in unit_map:
                continue

            unit_df = df[df['bm_unit'] == bm_unit_code]
            if unit_df.empty:
                continue

            records = []
            for idx, row in unit_df.iterrows():
                timestamp = row.get("timestamp", idx)
                if not isinstance(timestamp, datetime):
                    timestamp = pd.to_datetime(timestamp)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)

                period_end = timestamp + timedelta(minutes=30)
                value = float(row.get("value", 0))

                settlement_date = row.get("settlement_date")
                if isinstance(settlement_date, datetime):
                    settlement_date = settlement_date.isoformat()

                data = {
                    "bm_unit": bm_unit_code,
                    "level_from": float(row["level_from"]) if "level_from" in row and pd.notna(row["level_from"]) else None,
                    "level_to": float(row["level_to"]) if "level_to" in row and pd.notna(row["level_to"]) else None,
                    "settlement_period": int(row["settlement_period"]) if "settlement_period" in row else None,
                    "settlement_date": settlement_date,
                    "import_metadata": {
                        "import_timestamp": datetime.now(timezone.utc).isoformat(),
                        "import_method": "api_script",
                        "import_script": "reimport_incomplete_days_v2.py",
                    },
                }

                records.append({
                    "source": "ELEXON",
                    "source_type": "api",
                    "identifier": bm_unit_code,
                    "period_start": timestamp,
                    "period_end": period_end,
                    "period_type": "PT30M",
                    "value_extracted": Decimal(str(value)),
                    "unit": "MWh",
                    "data": data,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                })

            if records:
                # Deduplicate
                seen = {}
                for record in records:
                    key = (record['source'], record['source_type'], record['identifier'], record['period_start'])
                    seen[key] = record
                unique_records = list(seen.values())

                # Bulk upsert in batches
                BATCH_SIZE = 2000
                for i in range(0, len(unique_records), BATCH_SIZE):
                    batch = unique_records[i:i + BATCH_SIZE]
                    stmt = insert(GenerationDataRaw).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['source', 'source_type', 'identifier', 'period_start'],
                        set_={
                            'value_extracted': stmt.excluded.value_extracted,
                            'data': stmt.excluded.data,
                            'updated_at': datetime.now(timezone.utc),
                            'period_end': stmt.excluded.period_end,
                            'period_type': stmt.excluded.period_type,
                            'unit': stmt.excluded.unit,
                        }
                    )
                    await db.execute(stmt)

                await db.commit()
                total_stored += len(unique_records)

    return total_stored, None


async def main(dry_run=False, start_from=None):
    from app.models.generation_unit import GenerationUnit
    from app.core.database import get_session_factory
    from app.services.elexon_client import ElexonClient
    from sqlalchemy import select

    print("=" * 80)
    print("ELEXON B1610 INCOMPLETE DAY RE-IMPORT (V2 - Direct)")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Find incomplete days
    print("\nFinding incomplete settlement days...", flush=True)
    incomplete_days = await find_incomplete_days()
    print(f"Found {len(incomplete_days)} incomplete days")

    if start_from:
        incomplete_days = [d for d in incomplete_days if d >= start_from]
        print(f"Starting from {start_from}: {len(incomplete_days)} days remaining")

    if dry_run:
        print("\nDRY RUN - would import these days:")
        for d in incomplete_days:
            print(f"  {d}")
        return

    # Get BM units
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        stmt = select(GenerationUnit).where(GenerationUnit.source == "ELEXON")
        result = await db.execute(stmt)
        units = result.scalars().all()
        bm_units = [
            {'id': u.id, 'code': u.code, 'name': u.name, 'windfarm_id': u.windfarm_id}
            for u in units if u.code and u.code != 'nan'
        ]
    print(f"BM Units: {len(bm_units)}")

    # Create client once
    client = ElexonClient()

    print(f"\nProcessing {len(incomplete_days)} days...", flush=True)
    print("=" * 80)

    start_time = datetime.now()
    success_count = 0
    fail_count = 0
    total_records = 0
    errors = []

    for i, day in enumerate(incomplete_days, 1):
        day_start = datetime.now()
        try:
            records, error = await import_single_day(day, bm_units, client)
            elapsed = (datetime.now() - day_start).total_seconds()

            if error:
                print(f"  [{i}/{len(incomplete_days)}] {day}: FAILED ({elapsed:.0f}s) - {error}", flush=True)
                fail_count += 1
                errors.append((day, error))
            else:
                total_records += records
                success_count += 1
                # Print progress every 5 days or on failures
                if i % 5 == 0 or i == len(incomplete_days):
                    eta_seconds = (datetime.now() - start_time).total_seconds() / i * (len(incomplete_days) - i)
                    print(f"  [{i}/{len(incomplete_days)}] {day}: {records:,} records ({elapsed:.0f}s) | Total: {total_records:,} | ETA: {eta_seconds/60:.0f}min", flush=True)
        except Exception as e:
            elapsed = (datetime.now() - day_start).total_seconds()
            print(f"  [{i}/{len(incomplete_days)}] {day}: ERROR ({elapsed:.0f}s) - {str(e)[:100]}", flush=True)
            fail_count += 1
            errors.append((day, str(e)[:100]))

        # Small delay between API calls
        await asyncio.sleep(1)

    total_elapsed = (datetime.now() - start_time).total_seconds()

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total days: {len(incomplete_days)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Total records: {total_records:,}")
    print(f"Duration: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")

    if errors:
        print(f"\nFailed days:")
        for day, error in errors:
            print(f"  {day}: {error}")

    print("\n" + "=" * 80)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Re-import incomplete ELEXON days (V2)')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--start-from', type=str, help='Start from this date (YYYY-MM-DD)')
    args = parser.parse_args()

    asyncio.run(main(dry_run=args.dry_run, start_from=args.start_from))
