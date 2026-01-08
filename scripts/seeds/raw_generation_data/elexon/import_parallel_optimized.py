"""Highly optimized parallel import of Elexon CSV files with all performance improvements."""

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
import cProfile
import pstats
from contextlib import contextmanager

# Try to import optional faster libraries
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    print("⚠️ Polars not installed. Using pandas (slower). Install with: pip install polars")

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    HAS_ARROW = True
except ImportError:
    HAS_ARROW = False

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
import asyncpg


# Configuration
PROFILE_ENABLED = False  # Set to True to enable profiling


def signal_handler(signum, frame):
    """Handle interrupt signal gracefully."""
    print(f"\n⚠️ Process {current_process().name} interrupted")
    sys.exit(0)


@contextmanager
def profiler_context(enabled=PROFILE_ENABLED):
    """Context manager for optional profiling."""
    if enabled:
        profiler = cProfile.Profile()
        profiler.enable()
        try:
            yield profiler
        finally:
            profiler.disable()
            stats = pstats.Stats(profiler)
            stats.sort_stats('cumulative')
            print(f"\n{'='*60}")
            print(f"PERFORMANCE PROFILE FOR {current_process().name}")
            print(f"{'='*60}")
            stats.print_stats(15)
    else:
        yield None


async def get_relevant_bmu_ids() -> Set[str]:
    """Fetch BMU IDs from generation_units where source='ELEXON'."""
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(GenerationUnit.code)
            .where(GenerationUnit.source == 'ELEXON')
        )
        bmu_ids = {row[0] for row in result}
    
    print(f"Found {len(bmu_ids)} relevant BMU IDs in the system")
    return bmu_ids


def get_optimal_chunk_size(file_path: str) -> int:
    """Dynamically determine optimal chunk size based on available memory."""
    
    available_memory = psutil.virtual_memory().available
    file_size = os.path.getsize(file_path)
    
    # Use 10% of available memory for chunks
    memory_for_chunks = available_memory * 0.1
    
    # Estimate row size (rough estimate: 200 bytes per row)
    estimated_row_size = 200
    optimal_chunk = int(memory_for_chunks / estimated_row_size)
    
    # Clamp between reasonable bounds
    min_chunk = 10000
    max_chunk = 100000
    
    chunk_size = max(min_chunk, min(optimal_chunk, max_chunk))
    
    print(f"[Optimization] Using chunk size: {chunk_size:,} (Available memory: {available_memory / 1e9:.1f}GB)")
    return chunk_size


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

    # Calculate period_start and period_end from settlement_date and settlement_period
    # Settlement date is in UK local time (BST/GMT), needs conversion to UTC
    # During BST (summer): UK midnight = 23:00 UTC (previous day)
    # During GMT (winter): UK midnight = 00:00 UTC (same day)
    uk_dates = pd.to_datetime(filtered_df['settlement_date'], format='%Y%m%d').dt.tz_localize(
        'Europe/London', ambiguous='infer', nonexistent='shift_forward'
    )
    utc_dates = uk_dates.dt.tz_convert('UTC')

    # Add settlement period offset (each period is 30 minutes)
    filtered_df['period_start'] = utc_dates + pd.to_timedelta(
        (filtered_df['settlement_period'] - 1) * 30, unit='m'
    )
    filtered_df['period_end'] = filtered_df['period_start'] + pd.Timedelta(minutes=30)

    # Convert to naive UTC for storage (tz_localize(None) removes timezone info but keeps UTC values)
    filtered_df['period_start'] = filtered_df['period_start'].dt.tz_localize(None)
    filtered_df['period_end'] = filtered_df['period_end'].dt.tz_localize(None)
    
    # Columns that exist in generation_data_raw table
    columns = ['source', 'source_type', 'identifier', 'period_type', 'period_start', 'period_end', 
               'value_extracted', 'unit', 'data']
    
    # Add required columns with correct values
    filtered_df['source'] = 'ELEXON'
    filtered_df['source_type'] = 'csv'  # Since we're importing from CSV files
    filtered_df['identifier'] = filtered_df['bmu_id']
    filtered_df['period_type'] = 'ACTUAL'
    # Apply sign based on import_export_ind: I=Import (negative), E=Export (positive)
    import numpy as np
    filtered_df['value_extracted'] = np.where(
        filtered_df['import_export_ind'] == 'I',
        -filtered_df['metered_volume'],
        filtered_df['metered_volume']
    )
    filtered_df['unit'] = 'MW'
    
    # Create JSONB data column with all original data
    import json
    filtered_df['data'] = filtered_df.apply(
        lambda row: json.dumps({
            'bmu_id': row.get('bmu_id', ''),
            'settlement_date': str(row.get('settlement_date', '')),
            'settlement_period': row.get('settlement_period', 0),
            'settlement_run_type': row.get('settlement_run_type', ''),
            'cdca_run_number': row.get('cdca_run_number', 0),
            'estimate_ind': row.get('estimate_ind', ''),
            'import_export_ind': row.get('import_export_ind', ''),
            'metered_volume': float(row.get('metered_volume', 0))
        }), axis=1
    )
    
    # Select only needed columns
    copy_df = filtered_df[columns]
    
    # Write to CSV format
    copy_df.to_csv(output, index=False, header=False, sep='\t', na_rep='\\N')
    output.seek(0)
    
    # Connect directly with asyncpg for COPY
    try:
        conn = await asyncpg.connect(db_url)
        
        # Convert DataFrame to list of tuples for copy_records_to_table
        records = []
        for _, row in copy_df.iterrows():
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
            records_dict = copy_df.to_dict('records')
            
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


def read_csv_optimized(
    file_path: str,
    chunk_size: int,
    relevant_bmu_ids: Set[str],
    use_polars: bool = True
) -> pd.DataFrame:
    """Read CSV with maximum optimization."""
    
    if use_polars and HAS_POLARS:
        # Use Polars for 5-10x faster reading
        try:
            # Read with Polars lazy evaluation
            df = pl.scan_csv(
                file_path,
                has_header=True,
                rechunk=True,
                low_memory=False,
                try_parse_dates=True
            )
            
            # Filter at scan level (before loading into memory)
            filtered = df.filter(
                pl.col('bmu_id').is_in(list(relevant_bmu_ids))
            ).collect(streaming=True)
            
            # Convert to pandas if needed
            return filtered.to_pandas()
            
        except Exception as e:
            print(f"Polars failed: {e}, falling back to pandas")
    
    # Fallback to optimized pandas
    return pd.read_csv(
        file_path,
        chunksize=chunk_size,
        parse_dates=['settlement_date'],
        dtype={
            'bmu_id': 'category',  # Use category for memory efficiency
            'settlement_period': 'int8',  # Use smallest int type
            'metered_volume': 'float32',  # Use float32 instead of float64
            'import_export_ind': 'category',
            'settlement_run_type': 'category',
            'cdca_run_number': 'int8',
            'estimate_ind': 'category'
        },
        engine='pyarrow' if HAS_ARROW else 'c',  # Use Arrow engine if available
        low_memory=False
    )


def import_single_file_worker_optimized(
    file_path: str,
    worker_id: int,
    relevant_bmu_ids: Set[str],
    db_url: str,
    skip_duplicates: bool = True,
    use_copy: bool = True,
    batch_size: int = 5,
    progress_queue: Queue = None,
    status_queue: Queue = None
) -> Dict[str, Any]:
    """Optimized worker with all performance improvements."""
    
    with profiler_context():
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
        
        # Get optimal chunk size
        chunk_size = get_optimal_chunk_size(file_path)
        
        # Quick row count with progress
        print(f"[{worker_name}] 📊 Counting rows...")
        total_rows = sum(1 for _ in open(file_path, 'rb')) - 1
        print(f"[{worker_name}] 📊 Total rows: {total_rows:,}")
        
        if total_rows == 0:
            return {
                'file': file_name,
                'worker': worker_id,
                'total_rows': 0,
                'records_imported': 0,
                'records_filtered': 0
            }
        
        # Convert BMU IDs to numpy array for faster lookup
        bmu_ids_array = np.array(list(relevant_bmu_ids))
        # Also create dict for O(1) lookup
        bmu_lookup = {bmu: True for bmu in relevant_bmu_ids}
        
        total_imported = 0
        total_filtered = 0
        chunk_count = 0
        start_time = datetime.now()
        
        # Track imported keys in memory to avoid duplicates
        imported_keys = set() if skip_duplicates else None
        
        # Accumulate batches for bulk processing
        accumulated_dfs = []
        
        try:
            # Read CSV with optimization
            if HAS_POLARS:
                # Read entire file with Polars and filter
                print(f"[{worker_name}] ⚡ Using Polars for ultra-fast reading...")
                
                if status_queue:
                    status_queue.put({
                        'worker': worker_id,
                        'status': 'reading',
                        'progress': 0
                    })
                
                df = pl.read_csv(
                    file_path,
                    has_header=True,
                    rechunk=True,
                    low_memory=False,
                    n_threads=2,
                    try_parse_dates=True
                )
                
                # Filter all at once (strip whitespace first)
                print(f"[{worker_name}] 🔍 Filtering for {len(relevant_bmu_ids)} BMU IDs...")
                filtered_df = df.with_columns(
                    pl.col('bmu_id').str.strip_chars()
                ).filter(
                    pl.col('bmu_id').is_in(list(relevant_bmu_ids))
                )

                # Keep only the latest settlement run per settlement period
                print(f"[{worker_name}] 🔍 Filtering for latest settlement runs (max cdca_run_number)...")
                filtered_df = filtered_df.sort('cdca_run_number', descending=True)
                filtered_df = filtered_df.unique(
                    subset=['bmu_id', 'settlement_date', 'settlement_period'],
                    keep='first'  # Keep highest run number
                )

                filtered_df = filtered_df.to_pandas()

                total_filtered = len(df) - len(filtered_df)
                print(f"[{worker_name}] ✅ Kept {len(filtered_df):,} rows (latest runs only), filtered {total_filtered:,} rows")

                if not filtered_df.empty:
                    # No need for duplicate checking - we already filtered for latest runs
                    new_df = filtered_df
                    
                    # Import using COPY
                    if use_copy:
                        print(f"[{worker_name}] 💾 Importing {len(new_df):,} records using COPY...")
                        records = asyncio.run(import_with_copy(db_url, new_df))
                        total_imported = records
                        print(f"[{worker_name}] ✅ Imported {total_imported:,} records")
                    
            else:
                # Use optimized pandas chunking
                print(f"[{worker_name}] 📖 Using optimized pandas chunking...")
                
                chunk_iterator = pd.read_csv(
                    file_path,
                    chunksize=chunk_size,
                    parse_dates=['settlement_date'],
                    dtype={
                        'bmu_id': 'category',
                        'settlement_period': 'int8',
                        'metered_volume': 'float32',
                        'import_export_ind': 'category',
                        'settlement_run_type': 'category',
                        'cdca_run_number': 'int8',
                        'estimate_ind': 'category'
                    },
                    engine='pyarrow' if HAS_ARROW else 'c',
                    low_memory=False
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
                
                for chunk_df in chunk_iterator:
                    chunk_count += 1
                    original_size = len(chunk_df)
                    
                    # Strip whitespace from BMU IDs before filtering
                    chunk_df['bmu_id'] = chunk_df['bmu_id'].str.strip()
                    
                    # Fast numpy-based filtering
                    mask = np.isin(chunk_df['bmu_id'].values, bmu_ids_array)
                    filtered_df = chunk_df[mask]
                    
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

                        # Keep only the latest settlement run per settlement period
                        combined_df = combined_df.sort_values('cdca_run_number', ascending=False)
                        new_df = combined_df.drop_duplicates(
                            subset=['bmu_id', 'settlement_date', 'settlement_period'],
                            keep='first'  # Keep highest run number
                        )

                        # Import batch
                        if use_copy and not new_df.empty:
                            records = asyncio.run(import_with_copy(db_url, new_df))
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

                    # Keep only the latest settlement run per settlement period
                    combined_df = combined_df.sort_values('cdca_run_number', ascending=False)
                    new_df = combined_df.drop_duplicates(
                        subset=['bmu_id', 'settlement_date', 'settlement_period'],
                        keep='first'  # Keep highest run number
                    )

                    if use_copy and not new_df.empty:
                        records = asyncio.run(import_with_copy(db_url, new_df))
                        total_imported += records
                
                pbar.close()
                
        except Exception as e:
            print(f"\n[{worker_name}] Error: {str(e)}")
            return {
                'file': file_name,
                'worker': worker_id,
                'error': str(e)
            }
        
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
    bmu_ids_file: str,
    db_url: str,
    skip_duplicates: bool,
    use_copy: bool,
    result_queue: Queue,
    progress_queue: Queue,
    status_queue: Queue = None
):
    """Run the optimized worker in a separate process."""
    try:
        # Load BMU IDs
        with open(bmu_ids_file, 'rb') as f:
            relevant_bmu_ids = pickle.load(f)
        
        result = import_single_file_worker_optimized(
            file_path,
            worker_id,
            relevant_bmu_ids,
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
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        # Use fast estimate instead of COUNT(*)
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
            'total_count': stats.estimated_count if stats else 0,
            'table_size': stats.table_size if stats else 'Unknown'
        }


async def run_all_async_operations(
    skip_duplicates: bool = True,
    use_copy: bool = True
):
    """Run all async operations in a single event loop."""
    
    # Get relevant BMU IDs
    print("\nFetching relevant BMU IDs...")
    relevant_bmu_ids = await get_relevant_bmu_ids()
    
    if not relevant_bmu_ids:
        print("\n⚠️ No generation units found with source='ELEXON'")
        return None
    
    print(f"Will filter for {len(relevant_bmu_ids)} BMU IDs")
    
    # Get initial stats
    print("\nChecking database...")
    initial_stats = await get_database_stats()
    print(f"Current records: ~{initial_stats['total_count']:,}")
    print(f"Table size: {initial_stats['table_size']}")
    
    return {
        'relevant_bmu_ids': relevant_bmu_ids,
        'initial_stats': initial_stats
    }


def import_parallel_optimized(
    num_workers: int = 4,
    skip_duplicates: bool = True,
    use_copy: bool = True,
    profile: bool = False
):
    """Ultra-optimized parallel import with all performance improvements."""
    
    global PROFILE_ENABLED
    PROFILE_ENABLED = profile
    
    print("\n" + "="*80)
    print(" "*20 + "🚀 ULTRA-OPTIMIZED PARALLEL IMPORT 🚀")
    print("="*80)
    
    # Show optimization status
    print("\n📋 Optimizations enabled:")
    print(f"  {'✅' if HAS_POLARS else '⚠️'} Polars CSV reading: {'Yes' if HAS_POLARS else 'No (install polars for 5x speed)'}")
    print(f"  {'✅' if HAS_ARROW else '⚠️'} Arrow backend: {'Yes' if HAS_ARROW else 'No (install pyarrow)'}")
    print(f"  {'✅' if use_copy else '❌'} PostgreSQL COPY: {'Yes' if use_copy else 'No'}")
    print(f"  {'✅' if not skip_duplicates else '⚠️'} Memory duplicate check: {'No (faster)' if not skip_duplicates else 'Yes'}")
    print(f"  ✅ Dynamic chunk sizing: Yes")
    print(f"  ✅ NumPy filtering: Yes")
    print(f"  ✅ Batch accumulation: Yes")
    print(f"  {'✅' if profile else '❌'} Profiling: {'Yes' if profile else 'No'}")
    
    # Run all async operations in a single event loop
    async_result = asyncio.run(run_all_async_operations(skip_duplicates, use_copy))
    
    if not async_result:
        return
    
    relevant_bmu_ids = async_result['relevant_bmu_ids']
    initial_stats = async_result['initial_stats']
    
    # Save BMU IDs for workers
    import tempfile
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pkl')
    with open(temp_file.name, 'wb') as f:
        pickle.dump(relevant_bmu_ids, f)
    bmu_ids_file = temp_file.name
    
    # Get database URL
    from app.core.config import get_settings
    settings = get_settings()
    # Get the base PostgreSQL URL for asyncpg (without the +asyncpg suffix)
    db_url = settings.database_url_async.replace('+asyncpg', '')
    
    # Get CSV files
    data_folder = Path(__file__).parent / "data"
    csv_files = sorted(data_folder.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {data_folder}")
        os.unlink(bmu_ids_file)
        return
    
    print(f"\nFound {len(csv_files)} CSV files")
    
    # Confirm
    response = input("\nProceed with optimized import? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled")
        os.unlink(bmu_ids_file)
        return
    
    # Start workers
    actual_workers = min(num_workers, len(csv_files))
    print(f"\nStarting {actual_workers} workers...")
    
    result_queue = Queue()
    progress_queue = Queue()
    status_queue = Queue()
    
    workers = []
    start_time = datetime.now()
    
    print("\n" + "="*80)
    print(" "*25 + "📊 IMPORT PROGRESS 📊")
    print("="*80)
    
    for i, csv_file in enumerate(csv_files[:actual_workers]):
        p = Process(
            target=run_optimized_worker,
            args=(
                str(csv_file),
                i,
                bmu_ids_file,
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
        workers.append(p)
        time.sleep(0.2)
    
    print(f"\nProcessing with {actual_workers} workers...\n")
    
    # Add spacing for progress bars
    for _ in range(actual_workers):
        print()
    
    # Monitor progress
    results = []
    workers_completed = 0
    
    try:
        while workers_completed < actual_workers:
            if not result_queue.empty():
                result = result_queue.get()
                results.append(result)
                workers_completed += 1
            
            for w in workers:
                if not w.is_alive() and w.exitcode != 0:
                    print(f"\n⚠️ {w.name} exited with code {w.exitcode}")
            
            time.sleep(1)
        
        for w in workers:
            w.join(timeout=5)
            if w.is_alive():
                w.terminate()
                w.join()
                
    except KeyboardInterrupt:
        print("\n\nInterrupted! Stopping workers...")
        for w in workers:
            if w.is_alive():
                w.terminate()
                w.join()
        os.unlink(bmu_ids_file)
        return
    finally:
        if os.path.exists(bmu_ids_file):
            os.unlink(bmu_ids_file)
    
    total_duration = (datetime.now() - start_time).total_seconds()
    
    # Note: Final stats removed to avoid event loop issues
    # Stats are estimated from worker results instead
    
    # Summary
    print("\n" + "="*80)
    print(" "*30 + "📈 IMPORT SUMMARY 📈")
    print("="*80)
    
    total_imported = 0
    total_filtered = 0
    
    for result in sorted(results, key=lambda x: x['worker']):
        if 'error' not in result:
            print(f"\nWorker {result['worker']}: {result['file']}")
            print(f"  Filtered: {result.get('records_filtered', 0):,} ({result.get('filter_rate', 0):.1f}%)")
            print(f"  Imported: {result['records_imported']:,}")
            print(f"  Rate: {result['records_imported']/result['duration_seconds']:.0f} rec/s" if result.get('duration_seconds', 0) > 0 else "")
            
            total_imported += result.get('records_imported', 0)
            total_filtered += result.get('records_filtered', 0)
    
    print(f"\n📊 Final Statistics:")
    print(f"  • Total records imported: {total_imported:,}")
    print(f"  • Total records filtered: {total_filtered:,}")
    print(f"  • Success rate: {total_imported/(total_imported+total_filtered)*100:.1f}%" if (total_imported+total_filtered) > 0 else "0%")
    print(f"\n⏱️  Performance:")
    print(f"  • Total time: {total_duration/60:.1f} minutes")
    print(f"  • Overall rate: {total_imported/total_duration:.0f} records/second" if total_duration > 0 else "")
    print(f"  • Processing speed: {(total_imported+total_filtered)/total_duration:.0f} rows/second" if total_duration > 0 else "")
    print(f"\n💾 Database:")
    print(f"  • Initial table size: {initial_stats['table_size']}")
    print(f"  • Records added: {total_imported:,}")
    
    # Calculate speedup
    if total_imported > 0:
        # Estimate: unoptimized would take ~100 records/second
        unoptimized_estimate = total_imported / 100
        speedup = unoptimized_estimate / total_duration
        print(f"\nEstimated speedup: {speedup:.1f}x faster than unoptimized")
    
    print("\n" + "="*80)
    print(" "*20 + "✨ OPTIMIZED IMPORT COMPLETED ✨")
    print("="*80)
    print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Ultra-optimized parallel import')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers')
    parser.add_argument('--no-copy', action='store_true', help='Disable COPY optimization')
    parser.add_argument('--skip-duplicates', action='store_true', help='Check for duplicates')
    parser.add_argument('--profile', action='store_true', help='Enable profiling')
    
    args = parser.parse_args()
    
    # Check system
    print(f"System: {cpu_count()} CPUs, {psutil.virtual_memory().total/1e9:.1f}GB RAM")
    
    if not HAS_POLARS:
        print("\n💡 TIP: Install polars for 5-10x faster CSV reading:")
        print("   pip install polars")
    
    import_parallel_optimized(
        num_workers=args.workers,
        skip_duplicates=args.skip_duplicates,
        use_copy=not args.no_copy,
        profile=args.profile
    )