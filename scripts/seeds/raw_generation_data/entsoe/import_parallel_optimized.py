"""Ultra-optimized parallel import of ENTSOE Excel files with all performance improvements."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from multiprocessing import Process, Queue, current_process, cpu_count
from typing import Dict, Any, List, Set, Optional
import signal
import time
import pickle
import psutil
from io import StringIO
import tempfile
import json

# Try to import optional faster libraries
try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False
    print("⚠️ openpyxl not installed. Using default Excel reader (slower)")

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.core.config import get_settings
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
import asyncpg

# Maximum valid MW value for a single generation unit.
# Any value above this is treated as corrupt source data (e.g. int16 overflow = 32767).
# No single offshore wind unit exceeds ~500 MW nameplate capacity.
MAX_VALID_MW = 1000

# Column name mapping: v2 (r3) format → v1 (r2.1) format
COLUMN_MAP = {
    'DateTime(UTC)': 'DateTime (UTC)',
    'AreaMapCode': 'MapCode',
    'ActualGenerationOutput[MW]': 'ActualGenerationOutput(MW)',
    'ActualConsumption[MW]': 'ActualConsumption(MW)',
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize v2 column names to v1 format so the rest of the code uses a single set."""
    return df.rename(columns=COLUMN_MAP)


def signal_handler(signum, frame):
    """Handle interrupt signal gracefully."""
    print(f"\n⚠️ Process {current_process().name} interrupted")
    sys.exit(0)


def get_optimal_chunk_size(file_size_mb: float) -> int:
    """Dynamically determine optimal chunk size based on available memory."""
    
    available_memory = psutil.virtual_memory().available
    
    # Use 10% of available memory for chunks
    memory_for_chunks = available_memory * 0.1
    
    # Estimate row size for ENTSOE data (about 500 bytes per row with all columns)
    estimated_row_size = 500
    optimal_chunk = int(memory_for_chunks / estimated_row_size)
    
    # Clamp between reasonable bounds
    min_chunk = 5000
    max_chunk = 50000  # Smaller than ELEXON due to Excel overhead
    
    chunk_size = max(min_chunk, min(optimal_chunk, max_chunk))
    
    print(f"[Optimization] Using chunk size: {chunk_size:,} (Available memory: {available_memory / 1e9:.1f}GB)")
    return chunk_size


async def get_relevant_unit_codes() -> Set[str]:
    """Fetch generation unit codes from database where source='ENTSOE'."""
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(GenerationUnit.code)
            .where(GenerationUnit.source == 'ENTSOE')
        )
        unit_codes = {row[0] for row in result}
    
    print(f"Found {len(unit_codes)} relevant ENTSOE generation units in the system")
    return unit_codes


async def import_with_copy(
    db_url: str,
    filtered_df: pd.DataFrame,
    table_name: str = 'generation_data_raw'
) -> int:
    """Use PostgreSQL COPY for ultra-fast bulk insert."""
    
    if filtered_df.empty:
        return 0
    
    # Prepare data for COPY
    output = StringIO()
    
    # Parse DateTime column and handle resolution
    filtered_df['DateTime (UTC)'] = pd.to_datetime(filtered_df['DateTime (UTC)'])
    
    # Calculate period_end based on resolution
    def calculate_period_end(row):
        if row['ResolutionCode'] == 'PT60M':
            return row['DateTime (UTC)'] + pd.Timedelta(hours=1)
        elif row['ResolutionCode'] == 'PT15M':
            return row['DateTime (UTC)'] + pd.Timedelta(minutes=15)
        else:
            return row['DateTime (UTC)'] + pd.Timedelta(hours=1)  # Default to hourly
    
    filtered_df['period_start'] = filtered_df['DateTime (UTC)']
    filtered_df['period_end'] = filtered_df.apply(calculate_period_end, axis=1)
    
    # Make timezone-aware (ENTSOE data is in UTC)
    filtered_df['period_start'] = filtered_df['period_start'].dt.tz_localize('UTC')
    filtered_df['period_end'] = filtered_df['period_end'].dt.tz_localize('UTC')
    
    # Columns that exist in generation_data_raw table
    columns = ['source', 'source_type', 'identifier', 'period_type', 'period_start', 'period_end', 
               'value_extracted', 'unit', 'data']
    
    # Ensure optional columns exist (v2 files may lack these)
    if 'ActualConsumption(MW)' not in filtered_df.columns:
        filtered_df['ActualConsumption(MW)'] = np.nan
    if 'GenerationUnitInstalledCapacity(MW)' not in filtered_df.columns:
        filtered_df['GenerationUnitInstalledCapacity(MW)'] = np.nan

    # Determine data direction per row
    has_gen = filtered_df['ActualGenerationOutput(MW)'].notna() & (filtered_df['ActualGenerationOutput(MW)'] != 0)
    has_cons = filtered_df['ActualConsumption(MW)'].notna() & (filtered_df['ActualConsumption(MW)'] != 0)

    # Generation rows: have generation value (regardless of consumption)
    gen_df = filtered_df[has_gen].copy()
    gen_df['source_type'] = 'excel'
    gen_df['value_extracted'] = gen_df['ActualGenerationOutput(MW)']
    gen_df['_data_direction'] = 'generation'

    # Consumption-only rows: have consumption but NO generation
    cons_df = filtered_df[has_cons & ~has_gen].copy()
    cons_df['source_type'] = 'excel_consumption'
    cons_df['value_extracted'] = cons_df['ActualConsumption(MW)']
    cons_df['_data_direction'] = 'consumption'

    # Combine and proceed
    filtered_df = pd.concat([gen_df, cons_df], ignore_index=True)

    if filtered_df.empty:
        return 0

    # Filter out outlier values (e.g. int16 overflow = 32767 MW)
    outliers = filtered_df['value_extracted'].abs() > MAX_VALID_MW
    if outliers.any():
        n_outliers = outliers.sum()
        sample = filtered_df.loc[outliers, ['GenerationUnitCode', 'DateTime (UTC)', 'value_extracted']].head(5)
        print(f"  ⚠️ Dropping {n_outliers} rows with value > {MAX_VALID_MW} MW:")
        for _, r in sample.iterrows():
            print(f"    {r['GenerationUnitCode']}  {r['DateTime (UTC)']}  {r['value_extracted']:.1f} MW")
        filtered_df = filtered_df[~outliers]

    if filtered_df.empty:
        return 0

    # Deduplicate: v2 files have both CTA and BZN rows for the same unit+timestamp.
    # Keep the first occurrence (same generation value, different area breakdown).
    before_dedup = len(filtered_df)
    filtered_df = filtered_df.drop_duplicates(
        subset=['source_type', 'GenerationUnitCode', 'DateTime (UTC)'],
        keep='first'
    )
    if len(filtered_df) < before_dedup:
        dropped = before_dedup - len(filtered_df)
        # Silently drop area-level duplicates (expected for v2 format)

    # Add remaining required columns
    filtered_df['source'] = 'ENTSOE'
    filtered_df['identifier'] = filtered_df['GenerationUnitCode']
    filtered_df['period_type'] = filtered_df['ResolutionCode']
    filtered_df['unit'] = 'MW'

    # Create JSONB data column with all original data (includes data_direction)
    filtered_df['data'] = filtered_df.apply(
        lambda row: json.dumps({
            'area_code': row.get('AreaCode', ''),
            'area_display_name': row.get('AreaDisplayName', ''),
            'area_type_code': row.get('AreaTypeCode', ''),
            'map_code': row.get('MapCode', ''),
            'generation_unit_code': row.get('GenerationUnitCode', ''),
            'generation_unit_name': row.get('GenerationUnitName', ''),
            'generation_unit_type': row.get('GenerationUnitType', ''),
            'actual_generation_output_mw': float(row.get('ActualGenerationOutput(MW)', 0)) if pd.notna(row.get('ActualGenerationOutput(MW)')) else None,
            'actual_consumption_mw': float(row.get('ActualConsumption(MW)', 0)) if pd.notna(row.get('ActualConsumption(MW)')) else None,
            'installed_capacity_mw': int(row.get('GenerationUnitInstalledCapacity(MW)', 0)) if pd.notna(row.get('GenerationUnitInstalledCapacity(MW)')) else None,
            'resolution_code': row.get('ResolutionCode', ''),
            'update_time': str(row.get('UpdateTime(UTC)', '')),
            'data_direction': row.get('_data_direction', 'generation'),
        }), axis=1
    )
    
    # Connect directly with asyncpg for COPY
    try:
        conn = await asyncpg.connect(db_url)
        
        # Convert DataFrame to list of tuples for copy_records_to_table
        records = []
        for _, row in filtered_df.iterrows():
            # Ensure proper data types for each column
            record = (
                row['source'],
                row['source_type'],
                row['identifier'],
                row['period_type'],
                row['period_start'],  # Already datetime
                row['period_end'],    # Already datetime
                float(row['value_extracted']) if pd.notna(row['value_extracted']) else None,
                row['unit'],
                row['data']  # JSON string
            )
            records.append(record)
        
        # Use copy_records_to_table which is the correct asyncpg method
        result = await conn.copy_records_to_table(
            table_name,
            records=records,
            columns=columns
        )
        
        await conn.close()
        return len(records)
        
    except Exception as e:
        print(f"COPY failed: {e}, using fallback bulk insert...")
        
        # Fallback: Use SQLAlchemy bulk insert
        try:
            from app.core.database import get_session_factory
            from app.models.generation_data import GenerationDataRaw
            
            # Convert DataFrame to list of dicts for bulk insert
            records_dict = filtered_df[columns].to_dict('records')
            
            # Use sync operation in async context
            async def bulk_insert():
                AsyncSessionLocal = get_session_factory()
                async with AsyncSessionLocal() as db:
                    # Convert to GenerationDataRaw objects
                    objects = [
                        GenerationDataRaw(**record)
                        for record in records_dict
                    ]
                    db.add_all(objects)
                    await db.commit()
                    return len(objects)
            
            return await bulk_insert()
            
        except Exception as e2:
            print(f"Bulk insert also failed: {e2}")
            return 0


def convert_excel_to_csv_optimized(excel_path: str, csv_path: str) -> int:
    """Convert Excel to CSV for faster processing."""

    # Read Excel with optimizations
    if HAS_OPENPYXL:
        df = pd.read_excel(excel_path, engine='openpyxl')
    else:
        df = pd.read_excel(excel_path)

    # Normalize v2 column names to v1 format
    df = normalize_columns(df)

    # Save as CSV for faster reading later
    df.to_csv(csv_path, index=False)

    return len(df)


def import_single_file_worker_optimized(
    file_path: str,
    worker_id: int,
    relevant_unit_codes: Set[str],
    db_url: str,
    skip_duplicates: bool = True,
    use_copy: bool = True,
    batch_size: int = 5,
    progress_queue: Queue = None,
    status_queue: Queue = None
) -> Dict[str, Any]:
    """Optimized worker that processes an Excel file with filtering."""
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    file_name = os.path.basename(file_path)
    worker_name = f"Worker-{worker_id}"
    
    # Send status update
    if status_queue:
        status_queue.put({
            'worker': worker_id,
            'status': 'starting',
            'file': file_name
        })
    
    print(f"\n[{worker_name}] 🚀 Starting: {file_name}")
    
    # Get file size for optimization
    file_size_mb = os.path.getsize(file_path) / 1024 / 1024
    print(f"[{worker_name}] 📁 File size: {file_size_mb:.2f} MB")
    
    # Convert Excel to CSV for faster processing
    temp_csv = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
    temp_csv_path = temp_csv.name
    temp_csv.close()
    
    try:
        print(f"[{worker_name}] 📄 Converting Excel to CSV for faster processing...")
        total_rows = convert_excel_to_csv_optimized(file_path, temp_csv_path)
        print(f"[{worker_name}] 📊 Total rows: {total_rows:,}")
        
        if total_rows == 0:
            return {
                'file': file_name,
                'worker': worker_id,
                'total_rows': 0,
                'records_imported': 0,
                'records_filtered': 0
            }
        
        total_imported = 0
        total_filtered = 0
        start_time = datetime.now()
        
        # Get optimal chunk size
        chunk_size = get_optimal_chunk_size(file_size_mb)
        
        # Read CSV in chunks
        print(f"[{worker_name}] 📖 Reading CSV in chunks of {chunk_size:,} rows...")
        
        # Build dtype dict from columns that actually exist in the file
        # (v2 files may lack some columns like GenerationUnitInstalledCapacity)
        all_dtypes = {
            'GenerationUnitCode': str,
            'GenerationUnitName': str,
            'GenerationUnitType': str,
            'ResolutionCode': str,
            'AreaCode': str,
            'AreaDisplayName': str,
            'MapCode': str,
            'ActualGenerationOutput(MW)': float,
            'ActualConsumption(MW)': float,
            'GenerationUnitInstalledCapacity(MW)': float,
        }
        csv_columns = set(pd.read_csv(temp_csv_path, nrows=0).columns)
        active_dtypes = {k: v for k, v in all_dtypes.items() if k in csv_columns}

        chunk_iterator = pd.read_csv(
            temp_csv_path,
            chunksize=chunk_size,
            dtype=active_dtypes,
        )
        
        pbar = tqdm(
            total=total_rows,
            desc=f"[W{worker_id}] {file_name[:30]}",
            unit="rows",
            position=worker_id,
            leave=True,
            colour='green',
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )
        
        # Accumulate batches for bulk processing
        accumulated_dfs = []
        
        for chunk_df in chunk_iterator:
            original_size = len(chunk_df)
            
            # Filter for relevant generation units
            filtered_df = chunk_df[chunk_df['GenerationUnitCode'].isin(relevant_unit_codes)]
            
            filtered_out = original_size - len(filtered_df)
            total_filtered += filtered_out
            
            pbar.update(original_size)
            
            if filtered_df.empty:
                continue
            
            # Accumulate for batch processing
            accumulated_dfs.append(filtered_df)
            
            # Process accumulated batches
            if len(accumulated_dfs) >= batch_size:
                # Combine batches
                combined_df = pd.concat(accumulated_dfs, ignore_index=True)
                
                # Import batch
                if use_copy and not combined_df.empty:
                    records = asyncio.run(import_with_copy(db_url, combined_df))
                    total_imported += records
                
                accumulated_dfs = []
                
                pbar.set_postfix({
                    '✅': f'{total_imported:,}',
                    '🚫': f'{total_filtered:,}',
                    '📊': f'{(total_imported/(total_imported+total_filtered)*100):.1f}%' if (total_imported+total_filtered) > 0 else '0%'
                })
        
        # Process remaining accumulated data
        if accumulated_dfs:
            combined_df = pd.concat(accumulated_dfs, ignore_index=True)
            
            if use_copy and not combined_df.empty:
                records = asyncio.run(import_with_copy(db_url, combined_df))
                total_imported += records
        
        pbar.close()
        
    except Exception as e:
        print(f"\n[{worker_name}] Error: {str(e)}")
        return {
            'file': file_name,
            'worker': worker_id,
            'error': str(e)
        }
    finally:
        # Clean up temp file
        if os.path.exists(temp_csv_path):
            os.unlink(temp_csv_path)
    
    duration = (datetime.now() - start_time).total_seconds()
    filter_rate = (total_filtered / total_rows * 100) if total_rows > 0 else 0
    import_rate = total_imported/duration if duration > 0 else 0
    
    # Send completion status
    if status_queue:
        status_queue.put({
            'worker': worker_id,
            'status': 'completed',
            'imported': total_imported,
            'filtered': total_filtered,
            'duration': duration
        })
    
    print(f"\n[{worker_name}] 🎉 Completed {file_name}:")
    print(f"  📊 Statistics:")
    print(f"     • Total rows processed: {total_rows:,}")
    print(f"     • Rows imported: {total_imported:,} ({100-filter_rate:.1f}%)")
    print(f"     • Rows filtered: {total_filtered:,} ({filter_rate:.1f}%)")
    print(f"  ⏱️  Performance:")
    print(f"     • Duration: {duration/60:.1f} minutes")
    print(f"     • Import rate: {import_rate:.0f} records/second")
    print(f"     • Processing speed: {total_rows/duration:.0f} rows/second" if duration > 0 else "")
    
    return {
        'file': file_name,
        'worker': worker_id,
        'total_rows': total_rows,
        'records_imported': total_imported,
        'records_filtered': total_filtered,
        'filter_rate': filter_rate,
        'duration_seconds': duration
    }


def run_optimized_worker(
    file_path: str,
    worker_id: int,
    unit_codes_file: str,
    db_url: str,
    skip_duplicates: bool,
    use_copy: bool,
    result_queue: Queue,
    progress_queue: Queue,
    status_queue: Queue = None
):
    """Run the optimized worker in a separate process."""
    try:
        # Load unit codes
        with open(unit_codes_file, 'rb') as f:
            relevant_unit_codes = pickle.load(f)
        
        result = import_single_file_worker_optimized(
            file_path,
            worker_id,
            relevant_unit_codes,
            db_url,
            skip_duplicates,
            use_copy,
            batch_size=5,
            progress_queue=progress_queue,
            status_queue=status_queue
        )
        result_queue.put(result)
        
    except KeyboardInterrupt:
        print(f"\nWorker {worker_id} interrupted")
        result_queue.put({
            'file': os.path.basename(file_path),
            'worker': worker_id,
            'error': 'Interrupted'
        })
    except Exception as e:
        print(f"\nWorker {worker_id} crashed: {e}")
        result_queue.put({
            'file': os.path.basename(file_path),
            'worker': worker_id,
            'error': str(e)
        })


async def get_database_stats() -> Dict[str, Any]:
    """Get current database statistics using fast estimation."""
    try:
        AsyncSessionLocal = get_session_factory()

        async with AsyncSessionLocal() as db:
            # Use fast estimate (avoids slow COUNT on large tables)
            stats_result = await db.execute(
                text("""
                    SELECT
                        reltuples::BIGINT as estimated_count,
                        pg_size_pretty(pg_total_relation_size('generation_data_raw')) as table_size
                    FROM pg_class
                    WHERE relname = 'generation_data_raw'
                """)
            )
            stats = stats_result.first()

            return {
                'entsoe_count': stats.estimated_count if stats else 0,
                'total_count': stats.estimated_count if stats else 0,
                'table_size': stats.table_size if stats else 'Unknown'
            }
    except Exception as e:
        print(f"  Warning: Could not fetch stats: {e}")
        return {'entsoe_count': 0, 'total_count': 0, 'table_size': 'Unknown'}


async def run_all_async_operations(
    skip_duplicates: bool = True,
    use_copy: bool = True
):
    """Run all async operations in a single event loop."""
    
    # Get relevant unit codes
    print("\n🔍 Fetching relevant ENTSOE generation unit codes...")
    relevant_unit_codes = await get_relevant_unit_codes()
    
    if not relevant_unit_codes:
        print("\n⚠️ No generation units found with source='ENTSOE'")
        return None
    
    print(f"Will filter for {len(relevant_unit_codes)} generation unit codes")
    
    # Get initial stats
    print("\n📊 Checking database...")
    initial_stats = await get_database_stats()
    print(f"Current ENTSOE records: {initial_stats['entsoe_count']:,}")
    print(f"Total table size: {initial_stats['table_size']}")
    
    return {
        'relevant_unit_codes': relevant_unit_codes,
        'initial_stats': initial_stats
    }


async def delete_existing_excel_records(db_url: str, xlsx_files: List[Path]) -> int:
    """Delete existing ENTSOE excel/excel_consumption raw records for the date range covered by xlsx files.

    Parses YYYY_MM from file names to determine the date range, then deletes
    all rows with source_type IN ('excel', 'excel_consumption') in that range.
    """
    import re

    # Parse YYYY_MM from filenames like "2023_01_Actual..."
    months = []
    for f in xlsx_files:
        m = re.match(r'(\d{4})_(\d{2})_', f.name)
        if m:
            months.append((int(m.group(1)), int(m.group(2))))

    if not months:
        print("Could not parse date range from file names — skipping delete")
        return 0

    months.sort()
    first_year, first_month = months[0]
    last_year, last_month = months[-1]

    range_start = datetime(first_year, first_month, 1)
    # End is the first day of the month AFTER the last file
    if last_month == 12:
        range_end = datetime(last_year + 1, 1, 1)
    else:
        range_end = datetime(last_year, last_month + 1, 1)

    print(f"\n🗑️  Deleting existing ENTSOE excel records: {range_start:%Y-%m-%d} to {range_end:%Y-%m-%d}")

    conn = await asyncpg.connect(db_url)
    try:
        result = await conn.execute(
            """
            DELETE FROM generation_data_raw
            WHERE source = 'ENTSOE'
              AND source_type IN ('excel', 'excel_consumption')
              AND period_start >= $1
              AND period_start < $2
            """,
            range_start, range_end
        )
        deleted = int(result.split()[-1])  # "DELETE <count>"
        print(f"   Deleted {deleted:,} records")
        return deleted
    finally:
        await conn.close()


def import_parallel_optimized(
    num_workers: int = 4,
    skip_duplicates: bool = True,
    use_copy: bool = True,
    max_files: Optional[int] = None,
    data_dir: Optional[str] = None,
    delete_existing: bool = False,
):
    """Ultra-optimized parallel import with all performance improvements."""
    
    print("\n" + "="*80)
    print(" "*20 + "🚀 ENTSOE PARALLEL IMPORT 🚀")
    print("="*80)
    
    # Show optimization status
    print("\n📋 Optimizations enabled:")
    print(f"  {'✅' if HAS_OPENPYXL else '⚠️'} Openpyxl Excel reading: {'Yes' if HAS_OPENPYXL else 'No (install openpyxl for faster Excel reading)'}") 
    print(f"  ✅ Excel to CSV conversion: Yes")
    print(f"  {'✅' if use_copy else '❌'} PostgreSQL COPY: {'Yes' if use_copy else 'No'}")
    print(f"  {'✅' if not skip_duplicates else '⚠️'} Memory duplicate check: {'No (faster)' if not skip_duplicates else 'Yes'}")
    print(f"  ✅ Dynamic chunk sizing: Yes")
    print(f"  ✅ Batch accumulation: Yes")
    print(f"  ✅ Parallel processing: {num_workers} workers")
    
    # Run all async operations in a single event loop
    async_result = asyncio.run(run_all_async_operations(skip_duplicates, use_copy))
    
    if not async_result:
        return
    
    relevant_unit_codes = async_result['relevant_unit_codes']
    initial_stats = async_result['initial_stats']
    
    # Save unit codes for workers
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pkl')
    with open(temp_file.name, 'wb') as f:
        pickle.dump(relevant_unit_codes, f)
    unit_codes_file = temp_file.name
    
    # Get database URL
    settings = get_settings()
    # Get the base PostgreSQL URL for asyncpg (without the +asyncpg suffix)
    db_url = settings.database_url_async.replace('+asyncpg', '')
    
    # Get Excel files
    data_folder = Path(data_dir) if data_dir else Path(__file__).parent / "data"
    excel_files = sorted(data_folder.glob("*.xlsx"))
    
    if not excel_files:
        print(f"No Excel files found in {data_folder}")
        os.unlink(unit_codes_file)
        return
    
    if max_files:
        excel_files = excel_files[:max_files]
    
    print(f"\n📁 Found {len(excel_files)} Excel files")
    
    # Show file size summary
    total_size = sum(os.path.getsize(f) for f in excel_files)
    print(f"📊 Total data size: {total_size / 1024 / 1024:.2f} MB")
    
    if delete_existing:
        print(f"\n⚠️  --delete-existing is set. This will DELETE all ENTSOE excel/excel_consumption")
        print(f"   raw records in the date range covered by these {len(excel_files)} files BEFORE importing.")

    # Confirm
    response = input("\nProceed with optimized import? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled")
        os.unlink(unit_codes_file)
        return

    # Delete existing records if requested
    if delete_existing:
        asyncio.run(delete_existing_excel_records(db_url, excel_files))

    # Start workers
    actual_workers = min(num_workers, len(excel_files))
    print(f"\n🚀 Starting {actual_workers} workers...")
    
    result_queue = Queue()
    progress_queue = Queue()
    status_queue = Queue()
    
    workers = []
    start_time = datetime.now()
    
    print("\n" + "="*80)
    print(" "*25 + "📊 IMPORT PROGRESS 📊")
    print("="*80)
    
    # Add spacing for progress bars
    for _ in range(actual_workers):
        print()
    
    # Start workers
    for i in range(actual_workers):
        if i < len(excel_files):
            p = Process(
                target=run_optimized_worker,
                args=(
                    str(excel_files[i]),
                    i,
                    unit_codes_file,
                    db_url,
                    skip_duplicates,
                    use_copy,
                    result_queue,
                    progress_queue,
                    status_queue
                ),
                name=f"Worker-{i}"
            )
            p.start()
            workers.append((p, i))
            time.sleep(0.2)
    
    print(f"\n📈 Processing with {actual_workers} workers...\n")
    
    # Monitor progress and assign new files to completed workers
    results = []
    next_file_index = actual_workers
    active_workers = dict(workers)
    
    try:
        while active_workers or next_file_index < len(excel_files):
            # Check for completed workers
            for p, worker_id in list(active_workers.items()):
                if not p.is_alive():
                    # Worker completed
                    if not result_queue.empty():
                        result = result_queue.get()
                        results.append(result)
                    
                    del active_workers[p]
                    
                    # Assign next file if available
                    if next_file_index < len(excel_files):
                        new_p = Process(
                            target=run_optimized_worker,
                            args=(
                                str(excel_files[next_file_index]),
                                worker_id,
                                unit_codes_file,
                                db_url,
                                skip_duplicates,
                                use_copy,
                                result_queue,
                                progress_queue,
                                status_queue
                            ),
                            name=f"Worker-{worker_id}"
                        )
                        new_p.start()
                        active_workers[new_p] = worker_id
                        next_file_index += 1
                        time.sleep(0.2)
            
            # Collect any remaining results
            while not result_queue.empty():
                result = result_queue.get()
                results.append(result)
            
            time.sleep(1)
                
    except KeyboardInterrupt:
        print("\n\nInterrupted! Stopping workers...")
        for p, _ in active_workers.items():
            if p.is_alive():
                p.terminate()
                p.join()
        os.unlink(unit_codes_file)
        return
    finally:
        if os.path.exists(unit_codes_file):
            os.unlink(unit_codes_file)
    
    total_duration = (datetime.now() - start_time).total_seconds()
    
    # Summary
    print("\n" + "="*80)
    print(" "*30 + "📈 IMPORT SUMMARY 📈")
    print("="*80)
    
    total_imported = 0
    total_filtered = 0
    total_rows_processed = 0
    
    for result in sorted(results, key=lambda x: x.get('file', '')):
        if 'error' not in result:
            print(f"\n📁 {result['file']}")
            print(f"  ✅ Imported: {result.get('records_imported', 0):,}")
            print(f"  🚫 Filtered: {result.get('records_filtered', 0):,} ({result.get('filter_rate', 0):.1f}%)")
            
            total_imported += result.get('records_imported', 0)
            total_filtered += result.get('records_filtered', 0)
            total_rows_processed += result.get('total_rows', 0)
    
    print(f"\n📊 Final Statistics:")
    print(f"  • Total files processed: {len(results)}")
    print(f"  • Total rows processed: {total_rows_processed:,}")
    print(f"  • Total records imported: {total_imported:,}")
    print(f"  • Total records filtered: {total_filtered:,}")
    print(f"  • Success rate: {total_imported/(total_imported+total_filtered)*100:.1f}%" if (total_imported+total_filtered) > 0 else "0%")
    print(f"\n⏱️  Performance:")
    print(f"  • Total time: {total_duration/60:.1f} minutes")
    print(f"  • Overall rate: {total_imported/total_duration:.0f} records/second" if total_duration > 0 else "")
    print(f"  • Processing speed: {total_rows_processed/total_duration:.0f} rows/second" if total_duration > 0 else "")
    print(f"\n💾 Database:")
    print(f"  • Initial ENTSOE records: {initial_stats['entsoe_count']:,}")
    print(f"  • Records added: {total_imported:,}")
    
    print("\n" + "="*80)
    print(" "*20 + "✨ ENTSOE IMPORT COMPLETED ✨")
    print("="*80)
    print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Optimized ENTSOE parallel import')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers')
    parser.add_argument('--no-copy', action='store_true', help='Disable COPY optimization')
    parser.add_argument('--skip-duplicates', action='store_true', help='Check for duplicates')
    parser.add_argument('--max-files', type=int, help='Maximum number of files to process (for testing)')
    parser.add_argument('--data-dir', type=str, help='Path to directory containing xlsx files (default: data/)')
    parser.add_argument('--delete-existing', action='store_true',
                        help='Delete existing ENTSOE excel raw records in the date range before importing')

    args = parser.parse_args()

    # Check system
    print(f"System: {cpu_count()} CPUs, {psutil.virtual_memory().total/1e9:.1f}GB RAM")

    if not HAS_OPENPYXL:
        print("\n💡 TIP: Install openpyxl for faster Excel reading:")
        print("   poetry add openpyxl")

    import_parallel_optimized(
        num_workers=args.workers,
        skip_duplicates=args.skip_duplicates,
        use_copy=not args.no_copy,
        max_files=args.max_files,
        data_dir=args.data_dir,
        delete_existing=args.delete_existing,
    )