"""Optimized parallel import script for Energistyrelsen monthly generation data."""

import asyncio
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
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


async def get_energistyrelsen_unit_mapping() -> Dict[str, Dict]:
    """Get mapping of configured Energistyrelsen units."""
    AsyncSessionLocal = get_session_factory()
    
    mapping = {}
    
    async with AsyncSessionLocal() as db:
        # Get all Energistyrelsen units from database
        result = await db.execute(
            select(GenerationUnit)
            .where(GenerationUnit.source == 'ENERGISTYRELSEN')
        )
        units = result.scalars().all()
        
        # Create mapping by code and name
        units_by_code = {unit.code: unit for unit in units}
        units_by_name = {unit.name: unit for unit in units}
        
        logger.info(f"Found {len(units)} ENERGISTYRELSEN units in database")
        
        return {
            'by_code': units_by_code,
            'by_name': units_by_name,
            'units': {unit.id: unit for unit in units}
        }


def process_energistyrelsen_chunk(args: Tuple[pd.DataFrame, Dict, int, int]) -> List[Dict]:
    """Process a chunk of Energistyrelsen monthly data."""
    chunk_df, unit_mapping, chunk_start, chunk_size = args
    
    records = []
    units_by_code = unit_mapping['by_code']
    units_by_name = unit_mapping['by_name']
    
    # Skip header rows (rows 0-6 contain metadata)
    data_start_row = 7
    
    if len(chunk_df) <= data_start_row:
        return records
    
    # Extract month columns (from column 17 onwards)
    # Columns 0-16 contain turbine metadata
    month_columns = chunk_df.columns[17:]
    
    # Process each turbine row
    for idx in range(data_start_row, len(chunk_df)):
        row = chunk_df.iloc[idx]
        
        # Get turbine GSRN (Grid System Registration Number)
        gsrn = row.iloc[1]  # Column 'Turbine data' contains GSRN
        
        if pd.isna(gsrn) or str(gsrn).lower() in ['turbine identifier (gsrn)', 'nan']:
            continue
        
        gsrn_str = str(int(gsrn)) if isinstance(gsrn, (int, float)) else str(gsrn)
        
        # Try to find unit by code (GSRN)
        unit = None
        if gsrn_str in units_by_code:
            unit = units_by_code[gsrn_str]
        
        # Skip if unit not configured
        if not unit:
            continue
        
        # Process each month column
        for col_idx, col in enumerate(month_columns):
            try:
                # Get month value from column header (row 1)
                month_str = chunk_df.iloc[1, col_idx + 17]
                
                # Skip invalid month headers
                if pd.isna(month_str) or 'Note' in str(month_str):
                    continue
                
                # Parse month date
                try:
                    month_date = pd.to_datetime(month_str)
                except:
                    continue
                
                # Get generation value
                value = row.iloc[col_idx + 17]
                
                # Skip NaN or invalid values
                if pd.isna(value) or str(value).lower() in ['nan', 'n/a', '-', '']:
                    continue
                
                # Convert to float (handle string values)
                try:
                    if isinstance(value, str):
                        value = value.replace(',', '').replace(' ', '')
                    generation_kwh = float(value)
                except:
                    continue
                
                # Skip zero or negative values
                if generation_kwh <= 0:
                    continue
                
                # Convert kWh to MWh
                generation_mwh = generation_kwh / 1000.0
                
                # Calculate period end (last day of month)
                if month_date.month == 12:
                    period_end = datetime(month_date.year + 1, 1, 1) - timedelta(seconds=1)
                else:
                    period_end = datetime(month_date.year, month_date.month + 1, 1) - timedelta(seconds=1)
                
                # Create record for monthly data
                record = {
                    'period_start': month_date.isoformat(),
                    'period_end': period_end.isoformat(),
                    'period_type': 'month',
                    'source': 'ENERGISTYRELSEN',
                    'source_type': 'manual',
                    'identifier': unit.code,
                    'value_extracted': generation_mwh,
                    'unit': 'MWh',
                    'data': json.dumps({
                        'generation_mwh': generation_mwh,
                        'generation_kwh': generation_kwh,
                        'unit_code': unit.code,
                        'unit_name': unit.name,
                        'generation_unit_id': unit.id,
                        'gsrn': gsrn_str,
                        'month': month_date.strftime('%Y-%m'),
                        'period_type': 'monthly_total'
                    })
                }
                
                records.append(record)
                
            except Exception as e:
                logger.debug(f"Error processing month column {col_idx} for turbine {gsrn_str}: {e}")
                continue
    
    logger.info(f"Processed chunk starting at row {chunk_start}: {len(records)} records")
    return records


async def clear_existing_energistyrelsen_data():
    """Clear existing Energistyrelsen data from the database."""
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        # Count existing records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
        )
        existing_count = result.scalar() or 0
        
        if existing_count > 0:
            print(f"\n🗑️  Clearing {existing_count:,} existing ENERGISTYRELSEN records...")
            
            # Delete existing records
            await db.execute(
                text("DELETE FROM generation_data_raw WHERE source = 'ENERGISTYRELSEN'")
            )
            await db.commit()
            
            print(f"   ✅ Cleared {existing_count:,} records")
        else:
            print("\n   No existing ENERGISTYRELSEN data to clear")
    
    return existing_count


async def import_energistyrelsen_data(workers: int = 4, clean_first: bool = True, sample_size: Optional[int] = None):
    """Main import function for Energistyrelsen monthly data."""
    
    print("="*80)
    print(" "*15 + "🇩🇰 ENERGISTYRELSEN MONTHLY DATA IMPORT 🇩🇰")
    print("="*80)
    
    start_time = time.time()
    
    # Clear existing data if requested
    if clean_first:
        await clear_existing_energistyrelsen_data()
    
    # Get unit mapping
    print("\n📊 Loading ENERGISTYRELSEN unit mapping...")
    unit_mapping = await get_energistyrelsen_unit_mapping()
    
    if not unit_mapping['by_code']:
        print("\n❌ No ENERGISTYRELSEN units found in database!")
        print("   Please configure units first.")
        return
    
    # Load Excel file
    data_file = Path(__file__).parent / "data" / "energistyrelsen_monthly_data_until_2025-01.xlsx"
    print(f"\n📁 Reading ENERGISTYRELSEN data file: {data_file.name}")
    print(f"   File size: {data_file.stat().st_size / 1024 / 1024:.2f} MB")
    
    # Read the Excel file
    print("\n⏳ Loading Excel file (this may take a moment)...")
    
    if sample_size:
        print(f"   📊 Sample mode: Processing first {sample_size:,} rows")
        df = pd.read_excel(data_file, sheet_name='kWh', nrows=sample_size)
    else:
        df = pd.read_excel(data_file, sheet_name='kWh')
    
    print(f"   ✅ Loaded {len(df):,} rows with {len(df.columns)} columns")
    
    # Show data range
    month_columns = df.columns[17:]
    if len(month_columns) > 0:
        first_month = df.iloc[1, 17]
        last_month = df.iloc[1, len(df.columns) - 1]
        print(f"   📅 Date range: {first_month} to {last_month}")
        print(f"   📅 Total months: {len(month_columns)}")
    
    # Calculate chunk size based on available memory
    available_memory = psutil.virtual_memory().available
    chunk_size = min(1000, (len(df) - 7) // workers) if workers > 1 else len(df)
    
    print(f"\n🚀 Processing with {workers} workers (chunk size: {chunk_size:,} rows)...")
    
    # Prepare chunks for parallel processing
    chunks = []
    data_start = 7  # Skip header rows
    
    for i in range(data_start, len(df), chunk_size):
        chunk_end = min(i + chunk_size, len(df))
        # Include header rows in each chunk for column mapping
        chunk = pd.concat([df.iloc[:data_start], df.iloc[i:chunk_end]]).copy()
        chunks.append((chunk, unit_mapping, i, chunk_size))
    
    print(f"   📦 Created {len(chunks)} chunks for processing")
    
    # Process chunks in parallel
    all_records = []
    
    if workers > 1:
        with Pool(processes=workers) as pool:
            results = pool.map(process_energistyrelsen_chunk, chunks)
            for chunk_records in results:
                all_records.extend(chunk_records)
    else:
        for chunk_args in chunks:
            chunk_records = process_energistyrelsen_chunk(chunk_args)
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
                .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
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
                .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
            )
            final_count = result.scalar() or 0
            
            print(f"\n✅ Import complete!")
            print(f"   • Records added: {final_count - initial_count:,}")
            print(f"   • Total ENERGISTYRELSEN records: {final_count:,}")
    else:
        print("\n⚠️  No records to import (no matching units found)")
    
    # Performance stats
    elapsed_time = time.time() - start_time
    print(f"\n⏱️  Performance:")
    print(f"   • Total time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
    if len(all_records) > 0:
        print(f"   • Processing rate: {len(all_records)/elapsed_time:.0f} records/second")
    
    # Summary by unit
    if all_records:
        units_summary = {}
        months_summary = {}
        
        for record in all_records:
            unit_id = record['identifier']
            month = json.loads(record['data'])['month']
            
            if unit_id not in units_summary:
                units_summary[unit_id] = 0
            units_summary[unit_id] += 1
            
            if month not in months_summary:
                months_summary[month] = 0
            months_summary[month] += 1
        
        print(f"\n📊 Records by unit (top 10):")
        sorted_units = sorted(units_summary.items(), key=lambda x: x[1], reverse=True)[:10]
        for unit_id, count in sorted_units:
            print(f"   • Unit {unit_id}: {count:,} records")
        
        print(f"\n📅 Month coverage:")
        print(f"   • Total unique months: {len(months_summary)}")
        sorted_months = sorted(months_summary.keys())
        if sorted_months:
            print(f"   • Date range: {sorted_months[0]} to {sorted_months[-1]}")
    
    print("\n" + "="*80)
    print(" "*15 + "✨ ENERGISTYRELSEN IMPORT COMPLETED ✨")
    print("="*80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Import Energistyrelsen monthly generation data')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--no-clean', action='store_true', help='Do not clean existing data before import')
    parser.add_argument('--sample', type=int, help='Process only first N rows (for testing)')
    
    args = parser.parse_args()
    
    asyncio.run(import_energistyrelsen_data(
        workers=args.workers,
        clean_first=not args.no_clean,
        sample_size=args.sample
    ))


if __name__ == "__main__":
    main()