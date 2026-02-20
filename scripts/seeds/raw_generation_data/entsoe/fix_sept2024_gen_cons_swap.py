#!/usr/bin/env python3
"""
Fix September 2024 gen/cons swap for French offshore wind farms.

ENTSOE/RTE published September 2024 data with generation and consumption columns
swapped for Saint-Brieuc, Saint-Nazaire (Guerande), and partially for Fécamp.
A co-worker provided an edited xlsx with the corrected values.

This script:
  1. Reads the corrected xlsx
  2. Deletes existing Sept 2024 raw records (excel/excel_consumption) for the affected units
  3. Re-imports corrected data using the same format as import_parallel_optimized.py
  4. Deletes existing Sept 2024 aggregated records for the affected windfarms
  5. Triggers re-aggregation

Usage:
    poetry run python scripts/seeds/raw_generation_data/entsoe/fix_sept2024_gen_cons_swap.py \
        --edited-file /path/to/edited.xlsx \
        [--dry-run]

    # To re-aggregate after import:
    poetry run python scripts/seeds/aggregate_generation_data/reprocess_year_parallel.py \
        --year 2024 --months 9 --source ENTSOE --workers 1
"""

import asyncio
import argparse
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import asyncpg

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from dotenv import load_dotenv
import os

# Resolve project root whether run directly or via poetry from project dir
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent.parent.parent.parent
load_dotenv(_project_root / '.env')

# Column name mapping: v2 (r3) format → v1 (r2.1) format
COLUMN_MAP = {
    'DateTime(UTC)': 'DateTime (UTC)',
    'AreaMapCode': 'MapCode',
    'ActualGenerationOutput[MW]': 'ActualGenerationOutput(MW)',
    'ActualConsumption[MW]': 'ActualConsumption(MW)',
}

# Maximum valid MW (same as import_parallel_optimized.py)
MAX_VALID_MW = 1000

# Affected units and their windfarms
AFFECTED_UNITS = {
    '17W100P100P0842Y': {'name': 'Saint-Brieuc A1', 'windfarm_id': 7410},
    '17W100P100P3382R': {'name': 'Saint-Brieuc A2', 'windfarm_id': 7410},
    '17W0000014455651': {'name': 'Guerande 1', 'windfarm_id': 7411},
    '17W000001445567Y': {'name': 'Guerande 2', 'windfarm_id': 7411},
    '17W000001445569U': {'name': 'Fecamp 1', 'windfarm_id': 7372},
}

SEPT_START = datetime(2024, 9, 1, tzinfo=timezone.utc)
SEPT_END = datetime(2024, 10, 1, tzinfo=timezone.utc)


def get_db_url() -> str:
    url = os.getenv('DATABASE_URL', '')
    return url.replace('postgresql+asyncpg://', 'postgresql://')


def read_edited_file(path: str) -> pd.DataFrame:
    """Read the edited xlsx and normalize columns."""
    print(f"\n[1/5] Reading edited file: {path}")
    df = pd.read_excel(path, engine='openpyxl')

    # Normalize column names
    df = df.rename(columns=COLUMN_MAP)

    # Drop the co-worker's reference columns if present
    drop_cols = [c for c in df.columns if c.startswith('Unnamed') or c.startswith('original data') or c.startswith('OrigGen') or c.startswith('OrigCons')]
    # Also handle the .1 suffix columns from the rename
    drop_cols += [c for c in df.columns if c.endswith('.1')]
    if drop_cols:
        df = df.drop(columns=drop_cols, errors='ignore')

    # Filter to only the affected unit codes
    affected_codes = list(AFFECTED_UNITS.keys())
    df = df[df['GenerationUnitCode'].isin(affected_codes)].copy()

    print(f"  Rows after filtering to affected units: {len(df)}")
    for code in affected_codes:
        unit_rows = df[df['GenerationUnitCode'] == code]
        if len(unit_rows) > 0:
            name = unit_rows['GenerationUnitName'].iloc[0]
            print(f"    {code} ({name}): {len(unit_rows)} rows")
        else:
            print(f"    {code} ({AFFECTED_UNITS[code]['name']}): NOT in edited file")

    return df


def prepare_records(df: pd.DataFrame) -> list:
    """Transform the edited dataframe into records matching generation_data_raw schema.

    Uses the same logic as import_parallel_optimized.py: split gen/cons, deduplicate,
    filter outliers, build JSONB data column.
    """
    print(f"\n[2/5] Preparing records for import")

    gen_col = 'ActualGenerationOutput(MW)'
    cons_col = 'ActualConsumption(MW)'
    dt_col = 'DateTime (UTC)'

    # Parse datetime
    df[dt_col] = pd.to_datetime(df[dt_col])

    # Calculate period_end
    def calc_end(row):
        if row['ResolutionCode'] == 'PT15M':
            return row[dt_col] + pd.Timedelta(minutes=15)
        return row[dt_col] + pd.Timedelta(hours=1)

    df['period_start'] = df[dt_col].dt.tz_localize('UTC')
    df['period_end'] = df.apply(calc_end, axis=1)
    df['period_end'] = df['period_end'].dt.tz_localize('UTC')

    # Ensure consumption column exists
    if cons_col not in df.columns:
        df[cons_col] = np.nan
    if 'GenerationUnitInstalledCapacity(MW)' not in df.columns:
        df['GenerationUnitInstalledCapacity(MW)'] = np.nan

    # Split gen vs consumption-only (same logic as import_parallel_optimized.py)
    has_gen = df[gen_col].notna() & (df[gen_col] != 0)
    has_cons = df[cons_col].notna() & (df[cons_col] != 0)

    gen_df = df[has_gen].copy()
    gen_df['source_type'] = 'excel'
    gen_df['value_extracted'] = gen_df[gen_col]
    gen_df['_data_direction'] = 'generation'

    cons_df = df[has_cons & ~has_gen].copy()
    cons_df['source_type'] = 'excel_consumption'
    cons_df['value_extracted'] = cons_df[cons_col]
    cons_df['_data_direction'] = 'consumption'

    combined = pd.concat([gen_df, cons_df], ignore_index=True)
    print(f"  Generation rows: {len(gen_df)}, Consumption-only rows: {len(cons_df)}")

    if combined.empty:
        return []

    # Filter outliers
    outliers = combined['value_extracted'].abs() > MAX_VALID_MW
    if outliers.any():
        n = outliers.sum()
        print(f"  Dropping {n} outlier rows (value > {MAX_VALID_MW} MW)")
        combined = combined[~outliers]

    # Deduplicate (v2 CTA/BZN duplicates)
    before = len(combined)
    combined = combined.drop_duplicates(
        subset=['source_type', 'GenerationUnitCode', dt_col],
        keep='first'
    )
    if len(combined) < before:
        print(f"  Deduplication: {before} → {len(combined)} rows ({before - len(combined)} dropped)")

    # Build records
    records = []
    for _, row in combined.iterrows():
        data_json = json.dumps({
            'area_code': row.get('AreaCode', ''),
            'area_display_name': row.get('AreaDisplayName', ''),
            'area_type_code': row.get('AreaTypeCode', ''),
            'map_code': row.get('MapCode', ''),
            'generation_unit_code': row.get('GenerationUnitCode', ''),
            'generation_unit_name': row.get('GenerationUnitName', ''),
            'generation_unit_type': row.get('GenerationUnitType', ''),
            'actual_generation_output_mw': float(row.get(gen_col, 0)) if pd.notna(row.get(gen_col)) else None,
            'actual_consumption_mw': float(row.get(cons_col, 0)) if pd.notna(row.get(cons_col)) else None,
            'installed_capacity_mw': int(row.get('GenerationUnitInstalledCapacity(MW)', 0)) if pd.notna(row.get('GenerationUnitInstalledCapacity(MW)')) else None,
            'resolution_code': row.get('ResolutionCode', ''),
            'update_time': str(row.get('UpdateTime(UTC)', '')),
            'data_direction': row.get('_data_direction', 'generation'),
            'fix_note': 'sept2024_gen_cons_swap_fix',
        })

        records.append((
            'ENTSOE',                              # source
            row['source_type'],                    # source_type
            row['GenerationUnitCode'],             # identifier
            row['ResolutionCode'],                 # period_type
            row['period_start'],                   # period_start
            row['period_end'],                     # period_end
            float(row['value_extracted']) if pd.notna(row['value_extracted']) else None,  # value_extracted
            'MW',                                  # unit
            data_json,                             # data (JSONB)
        ))

    print(f"  Prepared {len(records)} records for import")
    return records


async def delete_old_raw(conn, dry_run: bool) -> int:
    """Delete existing Sept 2024 excel raw records for affected units."""
    print(f"\n[3/5] Deleting old raw records for Sept 2024")

    affected_codes = list(AFFECTED_UNITS.keys())

    # Count first
    count = await conn.fetchval("""
        SELECT count(*) FROM generation_data_raw
        WHERE source = 'ENTSOE'
          AND identifier = ANY($1)
          AND source_type IN ('excel', 'excel_consumption')
          AND period_start >= $2 AND period_start < $3
    """, affected_codes, SEPT_START, SEPT_END)

    print(f"  Found {count} existing raw records to delete")

    if dry_run:
        print("  [DRY RUN] Skipping delete")
        return 0

    result = await conn.execute("""
        DELETE FROM generation_data_raw
        WHERE source = 'ENTSOE'
          AND identifier = ANY($1)
          AND source_type IN ('excel', 'excel_consumption')
          AND period_start >= $2 AND period_start < $3
    """, affected_codes, SEPT_START, SEPT_END)

    deleted = int(result.split()[-1])
    print(f"  Deleted {deleted} raw records")
    return deleted


async def insert_new_raw(conn, records: list, dry_run: bool) -> int:
    """Insert corrected records using COPY."""
    print(f"\n[4/5] Inserting {len(records)} corrected raw records")

    if dry_run:
        print("  [DRY RUN] Skipping insert")
        return 0

    columns = ['source', 'source_type', 'identifier', 'period_type',
               'period_start', 'period_end', 'value_extracted', 'unit', 'data']

    await conn.copy_records_to_table(
        'generation_data_raw',
        records=records,
        columns=columns
    )

    print(f"  Inserted {len(records)} records")
    return len(records)


async def delete_old_aggregated(conn, dry_run: bool) -> int:
    """Delete existing Sept 2024 aggregated records for affected windfarms."""
    print(f"\n[5/5] Deleting old aggregated records for Sept 2024")

    affected_wf_ids = list(set(u['windfarm_id'] for u in AFFECTED_UNITS.values()))
    affected_unit_ids = []

    # Get generation_unit IDs
    rows = await conn.fetch("""
        SELECT id, code, name, windfarm_id FROM generation_units
        WHERE code = ANY($1)
    """, list(AFFECTED_UNITS.keys()))

    for r in rows:
        affected_unit_ids.append(r['id'])
        print(f"  Unit {r['id']} ({r['name']}) → windfarm {r['windfarm_id']}")

    count = await conn.fetchval("""
        SELECT count(*) FROM generation_data
        WHERE windfarm_id = ANY($1)
          AND generation_unit_id = ANY($2)
          AND hour >= $3 AND hour < $4
    """, affected_wf_ids, affected_unit_ids, SEPT_START, SEPT_END)

    print(f"  Found {count} aggregated records to delete")

    if dry_run:
        print("  [DRY RUN] Skipping delete")
        return 0

    result = await conn.execute("""
        DELETE FROM generation_data
        WHERE windfarm_id = ANY($1)
          AND generation_unit_id = ANY($2)
          AND hour >= $3 AND hour < $4
    """, affected_wf_ids, affected_unit_ids, SEPT_START, SEPT_END)

    deleted = int(result.split()[-1])
    print(f"  Deleted {deleted} aggregated records")
    return deleted


async def main():
    parser = argparse.ArgumentParser(description='Fix Sept 2024 gen/cons swap for French wind farms')
    parser.add_argument('--edited-file', required=True, help='Path to the corrected xlsx file')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    args = parser.parse_args()

    if not Path(args.edited_file).exists():
        print(f"ERROR: File not found: {args.edited_file}")
        sys.exit(1)

    if args.dry_run:
        print("=" * 80)
        print("  DRY RUN MODE — no changes will be made")
        print("=" * 80)

    # Step 1: Read edited file
    df = read_edited_file(args.edited_file)
    if df.empty:
        print("ERROR: No matching rows found in edited file")
        sys.exit(1)

    # Step 2: Prepare records
    records = prepare_records(df)
    if not records:
        print("ERROR: No records to import after processing")
        sys.exit(1)

    # Steps 3-5: Database operations
    db_url = get_db_url()
    conn = await asyncpg.connect(db_url)

    try:
        deleted_raw = await delete_old_raw(conn, args.dry_run)
        inserted = await insert_new_raw(conn, records, args.dry_run)
        deleted_agg = await delete_old_aggregated(conn, args.dry_run)
    finally:
        await conn.close()

    # Summary
    print(f"\n{'=' * 80}")
    print(f"  SUMMARY {'(DRY RUN)' if args.dry_run else ''}")
    print(f"{'=' * 80}")
    print(f"  Raw records deleted:    {deleted_raw}")
    print(f"  Raw records inserted:   {inserted}")
    print(f"  Aggregated records deleted: {deleted_agg}")
    print(f"\n  NOTE: Fecamp 2 (17W0000014455708) is NOT in the edited file.")
    print(f"  Its existing data is unchanged (32767 rogue values already cleaned separately).")

    if not args.dry_run:
        print(f"\n  Next step — re-aggregate Sept 2024:")
        print(f"  poetry run python scripts/seeds/aggregate_generation_data/reprocess_year_parallel.py \\")
        print(f"      --year 2024 --months 9 --source ENTSOE --workers 1")
    print()


if __name__ == '__main__':
    asyncio.run(main())
