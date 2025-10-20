"""Optimized parallel import script for Taipower generation data."""

import asyncio
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime
import json
from multiprocessing import Pool, cpu_count
import psutil
import time
import argparse
from typing import List, Dict, Tuple, Optional
import logging
from io import StringIO

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_unit import GenerationUnit
from app.models.generation_data import GenerationDataRaw
from sqlalchemy import select, text, func
from sqlalchemy.ext.asyncio import AsyncSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def get_configured_units() -> Dict[str, int]:
    """Get configured Taipower generation units from database."""
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(GenerationUnit.code, GenerationUnit.id)
            .where(GenerationUnit.source == 'TAIPOWER')
        )
        # Map both uppercase and lowercase for flexibility
        units = {}
        for row in result:
            code = row[0]
            unit_id = row[1]
            units[code] = unit_id
            units[code.upper()] = unit_id  # Also store uppercase version
            units[code.lower()] = unit_id  # And lowercase
        
        return units


def extract_unit_code_from_filename(filename: str) -> Optional[str]:
    """Extract unit code from filename and map to Chinese codes in database."""
    # Remove file extension and ' - For upload' suffix
    name = filename.replace('.xlsx', '').replace(' - For upload', '').strip()
    
    # Map English filenames to Chinese unit codes in database
    unit_code_map = {
        'Chang Kong': '彰工',  # Will handle multiple phases in processing
        'Changfang-Xidao FangEr': '芳二風',
        'Changfang-Xidao FangYi': '芳一風',
        'ChuangWei': '創維風',
        'Formosa 1 - HaiYang Zhunan': '海洋竹南',
        'Formosa 2 - HaiNeng': '海能風',
        'Greater ChanghuaSE - WoEr': '沃二風',  # Will handle phases
        'Greater ChanghuaSE - WoYi': '沃一風',  # Will handle phases
        'Guanwei-Guanyin and Taowei-Xinwu': '觀威觀音&桃威新屋',  # Will handle phases
        'Guanyuan': '觀園',
        'Luwei Changbin': '鹿威彰濱',  # Will handle phases
        'Mailiao': '雲麥',  # Will handle phases
        'Miaoli-Dapong': '苗栗大鵬',
        'Sihu': '四湖',
        'SinYuan-Lunbei': '新源崙背',
        'Taichung Port': '台中港',
        'Taipower Changhua Phase 1': '離岸一期',
        'Wanggong': '王功',
        'Yunlin YunHu': '允湖(註10)',
        'Yunlin YunSi': '允西(註10)',
        'Zhongneng': '中能風(註10)',
        'Zhongwei Da-an': '中威大安'  # Will handle phases
    }
    
    # Try to find matching key
    for key, code in unit_code_map.items():
        if key in name:
            return code
    
    logger.warning(f"No mapping found for filename: {filename}")
    return None


def process_excel_file(args: Tuple[str, Dict[str, int], int]) -> Tuple[int, int, List[Dict]]:
    """Process a single Excel file and return data for database insertion."""
    file_path, configured_units, file_idx = args
    file_path = Path(file_path)
    
    logger.info(f"[Worker {file_idx}] Processing {file_path.name}")
    
    records_to_insert = []
    filtered_count = 0
    
    try:
        # Read Excel file
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Extract unit code from filename
        unit_code = extract_unit_code_from_filename(file_path.name)
        
        if not unit_code:
            logger.warning(f"Could not map {file_path.name} to any unit code")
            return 0, len(df), []
        
        # Check if unit is configured
        if unit_code not in configured_units:
            logger.warning(f"Unit '{unit_code}' from {file_path.name} not found in configured units")
            return 0, len(df), []
        
        generation_unit_id = configured_units[unit_code]
        logger.info(f"Mapped {file_path.name} to unit '{unit_code}' (ID: {generation_unit_id})")
        
        # Process data
        for idx, row in df.iterrows():
            try:
                # Parse timestamp
                timestamp_str = row.get('Timestamp', '')
                if pd.isna(timestamp_str) or timestamp_str == '':
                    continue
                
                # Parse datetime (format: YYYY/M/D HH:MM)
                timestamp = pd.to_datetime(timestamp_str, format='%Y/%m/%d %H:%M')
                
                # Get generation value
                generation = row.get('Power generation', 0)
                if pd.isna(generation):
                    generation = 0
                
                # Get capacity
                capacity = row.get('Installed capacity', None)
                if pd.isna(capacity):
                    capacity = None
                
                # Get capacity factor
                capacity_factor = row.get('Capacity factor', None)
                if pd.isna(capacity_factor):
                    capacity_factor = None
                
                # Create record
                record = {
                    'period_start': timestamp.isoformat(),
                    'period_end': (timestamp + pd.Timedelta(hours=1)).isoformat(),  # Assume hourly data
                    'period_type': 'hour',
                    'source': 'TAIPOWER',
                    'identifier': unit_code,
                    'value_extracted': float(generation),
                    'unit': 'MW',
                    'data': json.dumps({
                        'generation_mw': float(generation),
                        'installed_capacity_mw': float(capacity) if capacity else None,
                        'capacity_factor': float(capacity_factor) if capacity_factor else None,
                        'unit_code': unit_code,
                        'generation_unit_id': generation_unit_id,
                        'file_source': file_path.name
                    })
                }
                
                records_to_insert.append(record)
                
            except Exception as e:
                logger.debug(f"Error processing row {idx}: {e}")
                continue
        
        logger.info(f"[Worker {file_idx}] Processed {file_path.name}: {len(records_to_insert)} records")
        
    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        return 0, 0, []
    
    return len(records_to_insert), filtered_count, records_to_insert


async def bulk_insert_records(records: List[Dict], db: AsyncSession):
    """Bulk insert records using PostgreSQL COPY."""
    if not records:
        return
    
    # Create CSV-like data for COPY
    csv_buffer = StringIO()
    
    for record in records:
        # Format: source, source_type, period_start, period_end, period_type, identifier, value_extracted, unit, data
        csv_buffer.write(f"{record['source']}\t")
        csv_buffer.write(f"manual\t")  # source_type
        csv_buffer.write(f"{record['period_start']}\t")
        csv_buffer.write(f"{record['period_end']}\t")
        csv_buffer.write(f"{record['period_type']}\t")
        csv_buffer.write(f"{record['identifier']}\t")
        csv_buffer.write(f"{record['value_extracted']}\t")
        csv_buffer.write(f"{record['unit']}\t")
        csv_buffer.write(f"{record['data']}\n")
    
    csv_buffer.seek(0)
    
    # Use raw SQL for COPY
    raw_conn = await db.connection()
    await raw_conn.execute(
        text("""
            COPY generation_data_raw (source, source_type, period_start, period_end, period_type, identifier, value_extracted, unit, data)
            FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')
        """)
    )
    
    # Write data
    await raw_conn.execute(text(csv_buffer.getvalue()))


async def clear_existing_taipower_data():
    """Clear existing Taipower data from the database."""
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        # Count existing records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'TAIPOWER')
        )
        existing_count = result.scalar() or 0
        
        if existing_count > 0:
            print(f"\n🗑️  Clearing {existing_count:,} existing Taipower records...")
            
            # Delete existing records
            await db.execute(
                text("DELETE FROM generation_data_raw WHERE source = 'Taipower'")
            )
            await db.commit()
            
            print(f"   ✅ Cleared {existing_count:,} records")
        else:
            print("\n   No existing Taipower data to clear")
    
    return existing_count


async def import_taipower_data(workers: int = 4, skip_duplicates: bool = False, clean_first: bool = True):
    """Main import function for Taipower data."""
    
    print("="*80)
    print(" "*20 + "⚡ TAIPOWER DATA IMPORT ⚡")
    print("="*80)
    
    start_time = time.time()
    
    # Clear existing data if requested
    if clean_first:
        await clear_existing_taipower_data()
    
    # Get configured units
    print("\n📊 Loading configured Taipower units...")
    configured_units = await get_configured_units()
    print(f"   Found {len(configured_units)} configured unit codes")
    
    # Get Excel files
    data_dir = Path(__file__).parent / "data"
    excel_files = sorted(data_dir.glob("*.xlsx"))
    print(f"\n📁 Found {len(excel_files)} Excel files to process")
    
    # Prepare arguments for parallel processing
    process_args = [
        (str(file_path), configured_units, idx)
        for idx, file_path in enumerate(excel_files)
    ]
    
    # Process files in parallel
    print(f"\n🚀 Processing with {workers} workers...")
    
    all_records = []
    total_processed = 0
    total_filtered = 0
    
    with Pool(processes=workers) as pool:
        results = pool.map(process_excel_file, process_args)
    
    # Collect results
    for processed, filtered, records in results:
        total_processed += processed
        total_filtered += filtered
        all_records.extend(records)
    
    print(f"\n📊 Processing complete:")
    print(f"   • Records to import: {len(all_records):,}")
    print(f"   • Records filtered: {total_filtered:,}")
    
    # Insert into database
    if all_records:
        print(f"\n💾 Inserting {len(all_records):,} records into database...")
        
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            # Check initial count
            result = await db.execute(
                select(text("COUNT(*)"))
                .select_from(GenerationDataRaw)
                .where(GenerationDataRaw.source == 'TAIPOWER')
            )
            initial_count = result.scalar()
            
            # Batch insert
            batch_size = 10000
            for i in range(0, len(all_records), batch_size):
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
                            source_type='manual',
                            identifier=record['identifier'],
                            value_extracted=record['value_extracted'],
                            unit=record['unit'],
                            data=json.loads(record['data'])
                        )
                        db_records.append(db_record)
                    
                    db.add_all(db_records)
                    await db.commit()
                    
                    print(f"   Inserted batch {i//batch_size + 1}/{(len(all_records) + batch_size - 1)//batch_size}")
                    
                except Exception as e:
                    logger.error(f"Error inserting batch: {e}")
                    await db.rollback()
            
            # Check final count
            result = await db.execute(
                select(text("COUNT(*)"))
                .select_from(GenerationDataRaw)
                .where(GenerationDataRaw.source == 'TAIPOWER')
            )
            final_count = result.scalar()
            
            print(f"\n✅ Import complete!")
            print(f"   • Records added: {final_count - initial_count:,}")
            print(f"   • Total Taipower records: {final_count:,}")
    
    # Performance stats
    elapsed_time = time.time() - start_time
    print(f"\n⏱️  Performance:")
    print(f"   • Total time: {elapsed_time:.1f} seconds")
    if total_processed > 0:
        print(f"   • Processing rate: {total_processed/elapsed_time:.0f} records/second")
    
    print("\n" + "="*80)
    print(" "*20 + "✨ IMPORT COMPLETED ✨")
    print("="*80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Import Taipower generation data')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--skip-duplicates', action='store_true', help='Skip duplicate checking')
    parser.add_argument('--no-clean', action='store_true', help='Do not clean existing data before import')
    
    args = parser.parse_args()
    
    asyncio.run(import_taipower_data(
        workers=args.workers,
        skip_duplicates=args.skip_duplicates,
        clean_first=not args.no_clean
    ))


if __name__ == "__main__":
    main()