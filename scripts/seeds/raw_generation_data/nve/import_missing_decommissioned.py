"""Import missing records for decommissioned NVE windfarms.

This script imports generation data that falls after the end_date of
decommissioned windfarms. These records were previously skipped by the
main import script because no operational phase matched their timestamp.

The 6 affected windfarms are:
- Code 22 (Vikna): 18,456 records after 2015-01-12
- Code 4 (Sandøy): 13,728 records after 2023-06-08
- Code 1 (Fjeldskår): 10,655 records after 2018-04-02
- Code 40 (Valsneset testpark): 8,640 records after 2015-10-06
- Code 23 (Kvalnes): 5,087 records after 2018-02-24
- Code 24 (Hovden Vesterålen): 3,600 records after 2015-09-20

Total: ~60,166 missing records

Usage:
    poetry run python scripts/seeds/raw_generation_data/nve/import_missing_decommissioned.py
"""

import asyncio
import pandas as pd
from datetime import datetime, date
from pathlib import Path
import sys
import time

sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from sqlalchemy import text

# The 6 decommissioned codes and their end dates
DECOMMISSIONED_CODES = {
    '22': date(2015, 1, 12),   # Vikna
    '4': date(2023, 6, 8),     # Sandøy
    '1': date(2018, 4, 2),     # Fjeldskår
    '40': date(2015, 10, 6),   # Valsneset testpark
    '23': date(2018, 2, 24),   # Kvalnes
    '24': date(2015, 9, 20),   # Hovden Vesterålen
}


async def get_last_phase_for_codes():
    """Get the last phase for each decommissioned code."""
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("""
            SELECT DISTINCT ON (code)
                code, id, name, end_date
            FROM generation_units
            WHERE source = 'NVE' AND code IN ('22', '4', '1', '40', '23', '24')
            ORDER BY code, start_date DESC
        """))
        return {row[0]: {'id': row[1], 'name': row[2], 'end_date': row[3]}
                for row in result.fetchall()}


async def import_missing_records():
    """Import missing records for decommissioned windfarms."""
    print("=" * 70)
    print(" " * 15 + "NVE DECOMMISSIONED DATA IMPORT")
    print("=" * 70)

    start_time = time.time()

    # Get last phase info for each code
    print("\n📊 Loading phase information for decommissioned codes...")
    phase_info = await get_last_phase_for_codes()

    if not phase_info:
        print("❌ No phase information found for decommissioned codes!")
        return

    print(f"   Found {len(phase_info)} codes with phase info:")
    for code, info in phase_info.items():
        print(f"   • Code {code}: {info['name']} (end: {info['end_date']})")

    # Read CSV
    csv_file = Path(__file__).parent / "data" / "vindprod2002-2024_kraftverk.csv"
    if not csv_file.exists():
        print(f"❌ CSV file not found: {csv_file}")
        return

    print(f"\n📁 Reading CSV file: {csv_file.name}")

    # Read data rows (skip first 3 rows: names, codes, header)
    df = pd.read_csv(csv_file, skiprows=3, header=None, low_memory=False)
    df[0] = pd.to_datetime(df[0])
    print(f"   Loaded {len(df):,} data rows")

    # Get column mapping (codes in row 1 of original CSV)
    codes_df = pd.read_csv(csv_file, header=None, nrows=2)
    col_to_code = {}
    for i, c in enumerate(codes_df.iloc[1, 1:], start=1):
        if pd.notna(c):
            code_str = str(int(c)) if isinstance(c, (int, float)) else str(c)
            if code_str in DECOMMISSIONED_CODES:
                col_to_code[i] = code_str

    print(f"   Mapped {len(col_to_code)} columns to decommissioned codes")

    # Collect records to import
    print("\n🔍 Finding missing records...")
    records = []
    records_by_code = {}

    for col_idx, code in col_to_code.items():
        if code not in phase_info:
            print(f"   ⚠️  Skipping code {code}: no phase info")
            continue

        end_date = DECOMMISSIONED_CODES[code]
        phase = phase_info[code]

        # Filter to records after end_date
        col_data = df.iloc[:, col_idx]
        timestamps = df.iloc[:, 0]
        mask = (timestamps.dt.date > end_date) & col_data.notna()

        code_count = 0
        for idx in df[mask].index:
            ts = timestamps.iloc[idx]
            value = col_data.iloc[idx]

            if ts.tzinfo is None:
                ts = ts.tz_localize('UTC')

            records.append({
                'period_start': ts,
                'period_end': ts + pd.Timedelta(hours=1),
                'period_type': 'hour',
                'source': 'NVE',
                'source_type': 'manual',
                'identifier': code,
                'value_extracted': float(value),
                'unit': 'MWh',
                'data': {
                    'generation_mwh': float(value),
                    'unit_code': code,
                    'unit_name': phase['name'],
                    'generation_unit_id': phase['id'],
                    'windfarm_id': None,
                    'match_type': 'fallback_after_decommission',
                    'timestamp': ts.isoformat()
                }
            })
            code_count += 1

        records_by_code[code] = code_count
        print(f"   Code {code}: {code_count:,} records after {end_date}")

    print(f"\n📊 Total records to import: {len(records):,}")

    if not records:
        print("   No records to import!")
        return

    # Insert into DB
    print(f"\n💾 Inserting records into database...")
    AsyncSessionLocal = get_session_factory()
    batch_size = 2000
    total = 0
    total_batches = (len(records) + batch_size - 1) // batch_size

    for batch_num, i in enumerate(range(0, len(records), batch_size), 1):
        batch = records[i:i + batch_size]
        async with AsyncSessionLocal() as db:
            try:
                db_records = [GenerationDataRaw(
                    period_start=r['period_start'],
                    period_end=r['period_end'],
                    period_type=r['period_type'],
                    source=r['source'],
                    source_type=r['source_type'],
                    identifier=r['identifier'],
                    value_extracted=r['value_extracted'],
                    unit=r['unit'],
                    data=r['data']
                ) for r in batch]
                db.add_all(db_records)
                await db.commit()
                total += len(batch)

                if batch_num % 10 == 0 or batch_num == total_batches:
                    pct = (batch_num / total_batches) * 100
                    print(f"   Batch {batch_num}/{total_batches} ({pct:.1f}%) - {total:,} records inserted")
            except Exception as e:
                print(f"   ❌ Error in batch {batch_num}: {e}")
                await db.rollback()

    # Summary
    elapsed = time.time() - start_time
    print(f"\n✅ Import complete!")
    print(f"   • Records imported: {total:,}")
    print(f"   • Time elapsed: {elapsed:.1f}s")
    print(f"   • Rate: {total/elapsed:.0f} records/sec")

    print("\n" + "=" * 70)
    print(" " * 15 + "IMPORT COMPLETED SUCCESSFULLY")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(import_missing_records())
