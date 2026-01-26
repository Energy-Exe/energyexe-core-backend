"""Optimized parallel import script for NVE generation data."""

import asyncio
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import json
from multiprocessing import Pool, cpu_count
import psutil
import time
import argparse
from typing import List, Dict, Tuple, Optional
import logging
from io import StringIO
import numpy as np

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_unit import GenerationUnit
from app.models.generation_data import GenerationDataRaw
from app.models.windfarm import Windfarm
from sqlalchemy import select, text, func
from sqlalchemy.ext.asyncio import AsyncSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def find_operational_unit(units_list: List, timestamp: datetime, first_power_date=None):
    """Find which phase/unit was operational at the given timestamp.

    Uses first_power_date (if available) as the earliest date for data acceptance,
    instead of start_date (which is typically the commercial operational date).
    This allows importing pre-commercial testing/commissioning data.

    Args:
        units_list: List of generation units with the same code (different phases)
        timestamp: Timestamp of the data point
        first_power_date: Optional first power date from windfarm (earliest allowed date)

    Returns:
        The generation unit that was operational at that time, or None
    """
    # Convert timestamp to date for comparison
    check_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp

    # If we have a first_power_date from windfarm, use it as the absolute earliest date
    # This allows data from testing/commissioning period before commercial operation
    if first_power_date and check_date < first_power_date:
        return None

    for unit in units_list:
        # Check if this unit was operational at the timestamp
        # Use first_power_date if available, otherwise fall back to start_date
        earliest_date = first_power_date if first_power_date else unit.start_date

        if earliest_date and check_date < earliest_date:
            continue

        if unit.end_date and check_date > unit.end_date:
            continue

        # This unit is operational at this timestamp
        return unit

    # No matching unit found - but if we're after first_power_date and before the first unit's start_date,
    # return the first unit (for pre-commercial data)
    if first_power_date and units_list:
        first_unit = units_list[0]  # Units are sorted by start_date
        if first_unit.start_date and check_date < first_unit.start_date:
            # Data is in pre-commercial period, assign to the first unit
            return first_unit

    return None


async def get_nve_unit_mapping() -> Tuple[Dict[str, List], Dict[str, dict]]:
    """Get mapping between NVE codes and database units, plus windfarm info.

    Returns:
        Tuple of:
        - Dictionary mapping codes to lists of units (sorted by start_date)
        - Dictionary mapping codes to windfarm info (first_power_date, commercial_operational_date)
    """
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Get all NVE units from database with windfarm info via LEFT JOIN
        result = await db.execute(
            select(GenerationUnit, Windfarm)
            .outerjoin(Windfarm, GenerationUnit.windfarm_id == Windfarm.id)
            .where(GenerationUnit.source == 'NVE')
            .order_by(GenerationUnit.code, GenerationUnit.start_date)
        )
        rows = result.all()

        # Group units by code (multiple phases can have same code)
        units_by_code = {}
        windfarm_info_by_code = {}

        for unit, windfarm in rows:
            if unit.code not in units_by_code:
                units_by_code[unit.code] = []
            units_by_code[unit.code].append(unit)

            # Store windfarm info (first_power_date, commercial_operational_date) for each code
            # Use the first windfarm found for each code (they should all be the same)
            if unit.code not in windfarm_info_by_code and windfarm:
                windfarm_info_by_code[unit.code] = {
                    'first_power_date': windfarm.first_power_date,
                    'commercial_operational_date': windfarm.commercial_operational_date,
                    'windfarm_id': windfarm.id,
                    'windfarm_name': windfarm.name
                }

        logger.info(f"Found {len(rows)} NVE units across {len(units_by_code)} unique codes")
        logger.info(f"Found windfarm info for {len(windfarm_info_by_code)} codes")

        # Log multi-phase windfarms
        multi_phase = {code: len(units) for code, units in units_by_code.items() if len(units) > 1}
        if multi_phase:
            logger.info(f"Multi-phase windfarms: {len(multi_phase)} codes with multiple phases")

        # Log codes with first_power_date earlier than start_date
        early_power_codes = []
        for code, wf_info in windfarm_info_by_code.items():
            if wf_info['first_power_date'] and code in units_by_code:
                first_unit = units_by_code[code][0]
                if first_unit.start_date and wf_info['first_power_date'] < first_unit.start_date:
                    early_power_codes.append((code, wf_info['first_power_date'], first_unit.start_date))

        if early_power_codes:
            logger.info(f"Found {len(early_power_codes)} codes with first_power_date < start_date (pre-commercial data available)")
            for code, fpd, sd in early_power_codes[:5]:
                logger.info(f"  Code {code}: first_power_date={fpd}, start_date={sd}")

        return units_by_code, windfarm_info_by_code


def process_nve_chunk(args: Tuple[pd.DataFrame, Dict, int, int, Dict, Dict]) -> List[Dict]:
    """Process a chunk of NVE data with phase-aware unit selection.

    Uses first_power_date from windfarm info to allow pre-commercial data import.
    """
    chunk_df, unit_mapping_by_code, chunk_start, chunk_size, column_to_code, windfarm_info_by_code = args

    records = []

    # Process data rows
    # If chunk_start is 0, skip first two rows (headers), otherwise process all rows
    start_idx = 2 if chunk_start == 0 else 0

    for idx in range(start_idx, len(chunk_df)):
        row = chunk_df.iloc[idx]

        # Get timestamp from first column
        timestamp_value = row.iloc[0]

        # Skip if not a valid timestamp
        if pd.isna(timestamp_value):
            continue

        try:
            # Parse timestamp
            if isinstance(timestamp_value, str):
                timestamp = pd.to_datetime(timestamp_value)
            else:
                timestamp = pd.to_datetime(timestamp_value)

            # NVE data is in UTC (verified by DST transition analysis:
            # - All days have exactly 24 hours including DST transitions
            # - Spring forward days contain 02:00 which doesn't exist in local time
            # - Fall back days have only one 02:00 instead of two)
            if timestamp.tzinfo is None:
                # If naive, treat as UTC directly
                timestamp = timestamp.tz_localize('UTC')
            elif str(timestamp.tzinfo) != 'UTC':
                # If already timezone-aware but not UTC, convert to UTC
                timestamp = timestamp.tz_convert('UTC')

            # Process each wind farm column
            for col, code in column_to_code.items():
                value = row[col]

                # Skip NaN values
                if pd.isna(value):
                    continue

                # Get list of units for this code
                units_list = unit_mapping_by_code.get(code, [])
                if not units_list:
                    logger.debug(f"No units found for code {code}")
                    continue

                # Get windfarm info for this code (contains first_power_date)
                wf_info = windfarm_info_by_code.get(code, {})
                first_power_date = wf_info.get('first_power_date')

                # Find which phase was operational at this timestamp
                # Uses first_power_date to allow pre-commercial data
                operational_unit = find_operational_unit(units_list, timestamp, first_power_date)

                if not operational_unit:
                    logger.debug(f"No operational unit found for code {code} at {timestamp}")
                    continue

                # Create record with the correct phase
                # Timestamps are now in UTC after conversion above
                record = {
                    'period_start': timestamp.isoformat(),
                    'period_end': (timestamp + pd.Timedelta(hours=1)).isoformat(),
                    'period_type': 'hour',
                    'source': 'NVE',
                    'source_type': 'manual',
                    'identifier': code,  # Store code as identifier
                    'value_extracted': float(value),
                    'unit': 'MWh',
                    'data': json.dumps({
                        'generation_mwh': float(value),
                        'unit_code': code,
                        'unit_name': operational_unit.name,
                        'generation_unit_id': operational_unit.id,
                        'windfarm_id': operational_unit.windfarm_id,
                        'timestamp': timestamp.isoformat()
                    })
                }

                records.append(record)

        except Exception as e:
            logger.debug(f"Error processing row {chunk_start + idx}: {e}")
            continue

    logger.info(f"Processed chunk starting at row {chunk_start}: {len(records)} records")
    return records

async def clear_existing_nve_data():
    """Clear existing NVE data from the database."""
    import asyncpg
    import os
    from urllib.parse import urlparse
    from dotenv import load_dotenv

    print("\n🗑️  Clearing existing NVE records (this may take a few minutes)...")

    # Load .env file
    env_path = Path(__file__).parent.parent.parent.parent.parent / '.env'
    load_dotenv(env_path)

    # Get database URL from environment
    db_url = os.environ.get('DATABASE_URL', '')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set")

    # Parse the URL to extract connection parameters
    # Handle both postgresql:// and postgresql+asyncpg:// formats
    db_url = db_url.replace('postgresql+asyncpg://', 'postgresql://')
    parsed = urlparse(db_url)

    # Connect directly with asyncpg with longer timeout
    conn = await asyncpg.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip('/'),
        command_timeout=600,  # 10 minutes for deletion
    )

    try:
        # Delete all NVE records
        result = await conn.execute(
            "DELETE FROM generation_data_raw WHERE source = 'NVE'"
        )

        # Parse result to get count (format: "DELETE N")
        deleted_count = int(result.split()[1]) if result else 0

        if deleted_count > 0:
            print(f"   ✅ Cleared {deleted_count:,} records")
        else:
            print("   No existing NVE data to clear")
    finally:
        await conn.close()

    return deleted_count


async def get_last_imported_date():
    """Get the last imported date for NVE data to support resume."""
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(func.max(GenerationDataRaw.period_start))
            .where(GenerationDataRaw.source == 'NVE')
        )
        max_date = result.scalar()
        return max_date


async def import_nve_data(workers: int = 4, clean_first: bool = True, sample_size: Optional[int] = None, resume: bool = False):
    """Main import function for NVE data."""

    print("="*80)
    print(" "*20 + "🌊 NVE DATA IMPORT 🌊")
    print("="*80)
    print("Phase-aware import: Matches data to correct generation unit phase")

    start_time = time.time()
    resume_from_date = None

    # Check for resume
    if resume:
        resume_from_date = await get_last_imported_date()
        if resume_from_date:
            print(f"\n🔄 RESUME MODE: Continuing from {resume_from_date}", flush=True)
        else:
            print("\n🔄 RESUME MODE: No existing data found, starting fresh", flush=True)

    # Clear existing data if requested
    if clean_first:
        await clear_existing_nve_data()

    # Get unit mapping and windfarm info
    print("\n📊 Loading NVE unit mapping and windfarm info...")
    unit_mapping, windfarm_info_by_code = await get_nve_unit_mapping()

    # Load data file (CSV or Excel)
    csv_file = Path(__file__).parent / "data" / "vindprod2002-2024_kraftverk.csv"
    xlsx_file = Path(__file__).parent / "data" / "vindprod2002-2024_kraftverk.xlsx"

    # Prefer CSV if it exists (faster to load), otherwise use Excel
    if csv_file.exists():
        data_file = csv_file
        file_type = "csv"
    elif xlsx_file.exists():
        data_file = xlsx_file
        file_type = "xlsx"
    else:
        print("❌ No data file found! Expected either:")
        print(f"   - {csv_file}")
        print(f"   - {xlsx_file}")
        return

    print(f"\n📁 Reading NVE data file: {data_file.name}")
    print(f"   File size: {data_file.stat().st_size / 1024 / 1024:.2f} MB")

    # Read the data file
    print(f"\n⏳ Loading {file_type.upper()} file (this may take a moment)...")

    if sample_size:
        print(f"   📊 Sample mode: Processing first {sample_size:,} rows")
        if file_type == "csv":
            df = pd.read_csv(data_file, nrows=sample_size, encoding='utf-8-sig')
        else:
            df = pd.read_excel(data_file, nrows=sample_size)
    else:
        if file_type == "csv":
            df = pd.read_csv(data_file, encoding='utf-8-sig')
        else:
            df = pd.read_excel(data_file)

    print(f"   ✅ Loaded {len(df):,} rows with {len(df.columns)} columns")

    # Create column-to-code mapping from the first row (which contains unit codes)
    print("\n🔗 Creating column-to-code mapping...")

    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is None:
        print("   ❌ No data found in Excel file")
        return

    column_to_code = {}

    # Map each column to a code using the first row which contains unit codes
    for col in df.columns[1:]:  # Skip first column (timestamp/metadata)
        code_value = first_row[col]
        if pd.notna(code_value):
            code_str = str(int(code_value)) if isinstance(code_value, (int, float)) else str(code_value)

            # Check if this code exists in our mapping
            if code_str in unit_mapping:
                column_to_code[col] = code_str
            else:
                logger.debug(f"No mapping found for column {col} with code {code_str}")

    print(f"   ✅ Mapped {len(column_to_code)} columns to codes")

    # Filter dataframe for resume mode
    original_rows = len(df)
    if resume_from_date:
        print(f"\n🔍 Filtering data for resume (after {resume_from_date})...", flush=True)
        # The first column contains timestamps, rows 0 and 1 are headers
        # Parse timestamp column and filter
        timestamp_col = df.columns[0]
        # Skip header rows (0=codes, 1=timestamp label)
        data_start_idx = 2

        # Find the row index where we should start
        start_row = data_start_idx
        for idx in range(data_start_idx, len(df)):
            try:
                ts_val = df.iloc[idx, 0]
                if pd.notna(ts_val):
                    ts = pd.to_datetime(ts_val)
                    if ts.tzinfo is None:
                        ts = ts.tz_localize('UTC')
                    if ts > resume_from_date:
                        start_row = idx
                        break
            except:
                continue

        # Keep header rows + data rows after resume point
        if start_row > data_start_idx:
            # Keep rows 0, 1 (headers) and rows from start_row onwards
            df = pd.concat([df.iloc[:data_start_idx], df.iloc[start_row:]], ignore_index=True)
            print(f"   ✅ Filtered from {original_rows:,} to {len(df):,} rows (skipped {start_row - data_start_idx:,} already imported)", flush=True)
        else:
            print(f"   ℹ️  No rows to skip, processing all data", flush=True)

    # Calculate chunk size based on available memory
    available_memory = psutil.virtual_memory().available
    chunk_size = min(10000, len(df) // workers) if workers > 1 else len(df)
    
    print(f"\n🚀 Processing with {workers} workers (chunk size: {chunk_size:,} rows)...")
    
    # Prepare chunks for parallel processing
    chunks = []
    for i in range(0, len(df), chunk_size):
        chunk_end = min(i + chunk_size, len(df))
        chunk = df.iloc[i:chunk_end].copy()
        # Pass the pre-computed column_to_code mapping and windfarm info to each chunk
        chunks.append((chunk, unit_mapping, i, chunk_size, column_to_code, windfarm_info_by_code))
    
    print(f"   📦 Created {len(chunks)} chunks for processing")
    
    # Process chunks in parallel
    all_records = []
    
    if workers > 1:
        with Pool(processes=workers) as pool:
            results = pool.map(process_nve_chunk, chunks)
            for chunk_records in results:
                all_records.extend(chunk_records)
    else:
        for chunk_args in chunks:
            chunk_records = process_nve_chunk(chunk_args)
            all_records.extend(chunk_records)
    
    print(f"\n📊 Processing complete:")
    print(f"   • Total records to import: {len(all_records):,}")
    
    # Insert into database
    if all_records:
        print(f"\n💾 Inserting {len(all_records):,} records into database...", flush=True)

        AsyncSessionLocal = get_session_factory()

        # Batch insert with smaller batches for reliability
        batch_size = 2000
        total_batches = (len(all_records) + batch_size - 1) // batch_size
        total_inserted = 0
        failed_batches = 0

        for batch_num, i in enumerate(range(0, len(all_records), batch_size), 1):
            batch = all_records[i:i+batch_size]

            # Use a fresh session for each batch to avoid timeout issues
            async with AsyncSessionLocal() as db:
                try:
                    # Convert to GenerationDataRaw objects
                    db_records = []
                    for record in batch:
                        db_record = GenerationDataRaw(
                            period_start=datetime.fromisoformat(record['period_start']),
                            period_end=datetime.fromisoformat(record['period_end']),
                            period_type=record['period_type'],
                            source=record['source'],
                            source_type=record['source_type'],
                            identifier=record['identifier'],
                            value_extracted=record['value_extracted'],
                            unit=record['unit'],
                            data=json.loads(record['data'])
                        )
                        db_records.append(db_record)

                    db.add_all(db_records)
                    await db.commit()

                    total_inserted += len(batch)

                    # Print progress every 50 batches (100k records)
                    if batch_num % 50 == 0 or batch_num == total_batches:
                        pct = (batch_num / total_batches) * 100
                        print(f"   Batch {batch_num}/{total_batches} ({pct:.1f}%) - {total_inserted:,} records inserted", flush=True)

                except Exception as e:
                    logger.error(f"Error inserting batch {batch_num}: {e}")
                    failed_batches += 1
                    try:
                        await db.rollback()
                    except:
                        pass

        print(f"\n✅ Import complete!", flush=True)
        print(f"   • Records inserted: {total_inserted:,}", flush=True)
        if failed_batches > 0:
            print(f"   • Failed batches: {failed_batches}", flush=True)
    
    # Performance stats
    elapsed_time = time.time() - start_time
    print(f"\n⏱️  Performance:")
    print(f"   • Total time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
    if len(all_records) > 0:
        print(f"   • Processing rate: {len(all_records)/elapsed_time:.0f} records/second")
    
    # Summary by code
    if all_records:
        codes_summary = {}
        for record in all_records:
            code = record['identifier']
            if code not in codes_summary:
                codes_summary[code] = 0
            codes_summary[code] += 1

        print(f"\n📊 Records by code (top 10):")
        sorted_codes = sorted(codes_summary.items(), key=lambda x: x[1], reverse=True)[:10]
        for code, count in sorted_codes:
            print(f"   • Code {code}: {count:,} records")
    
    print("\n" + "="*80)
    print(" "*20 + "✨ NVE IMPORT COMPLETED ✨")
    print("="*80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Import NVE generation data')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--no-clean', action='store_true', help='Do not clean existing data before import')
    parser.add_argument('--resume', action='store_true', help='Resume from last imported date (skip existing data)')
    parser.add_argument('--sample', type=int, help='Process only first N rows (for testing)')

    args = parser.parse_args()

    # If resume is set, don't clean
    clean_first = not args.no_clean and not args.resume

    asyncio.run(import_nve_data(
        workers=args.workers,
        clean_first=clean_first,
        sample_size=args.sample,
        resume=args.resume
    ))


if __name__ == "__main__":
    main()