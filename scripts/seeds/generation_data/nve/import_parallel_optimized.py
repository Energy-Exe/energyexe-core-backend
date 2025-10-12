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
from sqlalchemy import select, text, func
from sqlalchemy.ext.asyncio import AsyncSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def find_operational_unit(units_list: List, timestamp: datetime):
    """Find which phase/unit was operational at the given timestamp.

    Args:
        units_list: List of generation units with the same code (different phases)
        timestamp: Timestamp of the data point

    Returns:
        The generation unit that was operational at that time, or None
    """
    # Convert timestamp to date for comparison
    check_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp

    for unit in units_list:
        # Check if this unit was operational at the timestamp
        if unit.start_date and check_date < unit.start_date:
            continue

        if unit.end_date and check_date > unit.end_date:
            continue

        # This unit is operational at this timestamp
        return unit

    # No matching unit found
    return None


async def get_nve_unit_mapping() -> Dict[str, List]:
    """Get mapping between NVE codes and database units.

    Returns:
        Dictionary mapping codes to lists of units (sorted by start_date)
    """
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Get all NVE units from database
        result = await db.execute(
            select(GenerationUnit)
            .where(GenerationUnit.source == 'NVE')
            .order_by(GenerationUnit.code, GenerationUnit.start_date)
        )
        units = result.scalars().all()

        # Group units by code (multiple phases can have same code)
        units_by_code = {}
        for unit in units:
            if unit.code not in units_by_code:
                units_by_code[unit.code] = []
            units_by_code[unit.code].append(unit)

        logger.info(f"Found {len(units)} NVE units across {len(units_by_code)} unique codes")

        # Log multi-phase windfarms
        multi_phase = {code: len(units) for code, units in units_by_code.items() if len(units) > 1}
        if multi_phase:
            logger.info(f"Multi-phase windfarms: {len(multi_phase)} codes with multiple phases")

        return units_by_code


def process_nve_chunk(args: Tuple[pd.DataFrame, Dict, int, int, Dict]) -> List[Dict]:
    """Process a chunk of NVE data with phase-aware unit selection."""
    chunk_df, unit_mapping_by_code, chunk_start, chunk_size, column_to_code = args

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

                # Find which phase was operational at this timestamp
                operational_unit = find_operational_unit(units_list, timestamp)

                if not operational_unit:
                    logger.debug(f"No operational unit found for code {code} at {timestamp}")
                    continue

                # Create record with the correct phase
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
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        # Count existing records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'NVE')
        )
        existing_count = result.scalar() or 0
        
        if existing_count > 0:
            print(f"\n🗑️  Clearing {existing_count:,} existing NVE records...")
            
            # Delete existing records
            await db.execute(
                text("DELETE FROM generation_data_raw WHERE source = 'NVE'")
            )
            await db.commit()
            
            print(f"   ✅ Cleared {existing_count:,} records")
        else:
            print("\n   No existing NVE data to clear")
    
    return existing_count


async def import_nve_data(workers: int = 4, clean_first: bool = True, sample_size: Optional[int] = None):
    """Main import function for NVE data."""

    print("="*80)
    print(" "*20 + "🌊 NVE DATA IMPORT 🌊")
    print("="*80)
    print("Phase-aware import: Matches data to correct generation unit phase")

    start_time = time.time()

    # Clear existing data if requested
    if clean_first:
        await clear_existing_nve_data()

    # Get unit mapping
    print("\n📊 Loading NVE unit mapping...")
    unit_mapping = await get_nve_unit_mapping()

    # Load Excel file
    data_file = Path(__file__).parent / "data" / "vindprod2002-2024_kraftverk.xlsx"
    print(f"\n📁 Reading NVE data file: {data_file.name}")
    print(f"   File size: {data_file.stat().st_size / 1024 / 1024:.2f} MB")

    # Read the Excel file
    print("\n⏳ Loading Excel file (this may take a moment)...")

    if sample_size:
        print(f"   📊 Sample mode: Processing first {sample_size:,} rows")
        df = pd.read_excel(data_file, nrows=sample_size)
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
    
    # Calculate chunk size based on available memory
    available_memory = psutil.virtual_memory().available
    chunk_size = min(10000, len(df) // workers) if workers > 1 else len(df)
    
    print(f"\n🚀 Processing with {workers} workers (chunk size: {chunk_size:,} rows)...")
    
    # Prepare chunks for parallel processing
    chunks = []
    for i in range(0, len(df), chunk_size):
        chunk_end = min(i + chunk_size, len(df))
        chunk = df.iloc[i:chunk_end].copy()
        # Pass the pre-computed column_to_code mapping to each chunk
        chunks.append((chunk, unit_mapping, i, chunk_size, column_to_code))
    
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
        print(f"\n💾 Inserting {len(all_records):,} records into database...")
        
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            # Check initial count
            result = await db.execute(
                select(func.count(GenerationDataRaw.id))
                .where(GenerationDataRaw.source == 'NVE')
            )
            initial_count = result.scalar() or 0
            
            # Batch insert
            batch_size = 10000
            total_batches = (len(all_records) + batch_size - 1) // batch_size
            
            for batch_num, i in enumerate(range(0, len(all_records), batch_size), 1):
                batch = all_records[i:i+batch_size]
                
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
                    
                    print(f"   Inserted batch {batch_num}/{total_batches} ({len(batch):,} records)")
                    
                except Exception as e:
                    logger.error(f"Error inserting batch {batch_num}: {e}")
                    await db.rollback()
            
            # Check final count
            result = await db.execute(
                select(func.count(GenerationDataRaw.id))
                .where(GenerationDataRaw.source == 'NVE')
            )
            final_count = result.scalar() or 0
            
            print(f"\n✅ Import complete!")
            print(f"   • Records added: {final_count - initial_count:,}")
            print(f"   • Total NVE records: {final_count:,}")
    
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
    parser.add_argument('--sample', type=int, help='Process only first N rows (for testing)')
    
    args = parser.parse_args()
    
    asyncio.run(import_nve_data(
        workers=args.workers,
        clean_first=not args.no_clean,
        sample_size=args.sample
    ))


if __name__ == "__main__":
    main()