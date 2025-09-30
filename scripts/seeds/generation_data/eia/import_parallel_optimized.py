"""Optimized parallel import script for EIA monthly wind generation data."""

import asyncio
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import json
from multiprocessing import Process, Queue, current_process, cpu_count
import psutil
import time
import argparse
from typing import List, Dict, Set, Any, Optional
import logging
from tqdm import tqdm
import os
import pickle
import tempfile
from dateutil.relativedelta import relativedelta
import numpy as np

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_unit import GenerationUnit
from app.models.generation_data import GenerationDataRaw
from sqlalchemy import select, text, func
from sqlalchemy.ext.asyncio import AsyncSession
import asyncpg

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def get_eia_plant_ids() -> Set[str]:
    """Get set of configured EIA plant IDs (generation_unit.code where source='EIA')."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Get all EIA units from database
        result = await db.execute(
            select(GenerationUnit.code)
            .where(GenerationUnit.source == 'EIA')
        )
        plant_ids = {str(row[0]) for row in result}

        logger.info(f"Found {len(plant_ids)} EIA generation units in database")
        return plant_ids


async def clear_existing_eia_data():
    """Clear existing EIA data from the database."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Count existing records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'EIA')
        )
        existing_count = result.scalar() or 0

        if existing_count > 0:
            print(f"\n🗑️  Clearing {existing_count:,} existing EIA records...")

            # Delete existing records
            await db.execute(
                text("DELETE FROM generation_data_raw WHERE source = 'EIA'")
            )
            await db.commit()

            print(f"   ✅ Cleared {existing_count:,} records")
        else:
            print("\n   No existing EIA data to clear")

    return existing_count


async def import_with_copy_bulk(db_url: str, records: List[Dict]) -> int:
    """Use PostgreSQL COPY for ultra-fast bulk insert."""

    if not records:
        return 0

    try:
        # Connect with asyncpg
        conn = await asyncpg.connect(db_url)

        # Prepare records for COPY
        copy_records = []
        for record in records:
            copy_record = (
                record['source'],
                record['source_type'],
                record['identifier'],
                record['period_type'],
                datetime.fromisoformat(record['period_start']),
                datetime.fromisoformat(record['period_end']),
                float(record['value_extracted']),
                record['unit'],
                record['data']  # Already JSON string
            )
            copy_records.append(copy_record)

        # Use COPY
        columns = ['source', 'source_type', 'identifier', 'period_type',
                   'period_start', 'period_end', 'value_extracted', 'unit', 'data']

        await conn.copy_records_to_table(
            'generation_data_raw',
            records=copy_records,
            columns=columns
        )

        await conn.close()
        return len(copy_records)

    except Exception as e:
        logger.error(f"COPY failed: {e}")
        return 0


def process_eia_file(
    file_path: str,
    plant_ids: Set[str],
    worker_id: int,
    db_url: str,
    result_queue: Queue
) -> Dict[str, Any]:
    """Process a single EIA Excel file."""

    file_name = os.path.basename(file_path)
    worker_name = f"Worker-{worker_id}"

    logger.info(f"[{worker_name}] 🚀 Starting: {file_name}")

    try:
        # Read Excel file (skip first 5 header rows)
        df = pd.read_excel(file_path, skiprows=5)

        total_rows = len(df)
        logger.info(f"[{worker_name}] 📊 Total rows: {total_rows:,}")

        # Clean column names (remove newlines)
        df.columns = [str(col).replace('\n', ' ') for col in df.columns]

        # Filter for Wind data only (fuel_type = 'WND')
        df = df[df['Reported Fuel Type Code'] == 'WND'].copy()
        wind_rows = len(df)

        logger.info(f"[{worker_name}] 🌬️  Wind rows: {wind_rows:,} ({wind_rows/total_rows*100:.1f}%)")

        if wind_rows == 0:
            logger.info(f"[{worker_name}] ⚠️  No wind data in {file_name}")
            return {
                'file': file_name,
                'worker': worker_id,
                'total_rows': total_rows,
                'wind_rows': 0,
                'records_imported': 0
            }

        # Convert Plant Id to string and filter for configured plants
        df['Plant Id'] = df['Plant Id'].astype(str)
        df = df[df['Plant Id'].isin(plant_ids)].copy()

        configured_rows = len(df)
        logger.info(f"[{worker_name}] ✅ Configured plants: {configured_rows:,}")

        if configured_rows == 0:
            logger.info(f"[{worker_name}] ⚠️  No configured plants in {file_name}")
            return {
                'file': file_name,
                'worker': worker_id,
                'total_rows': total_rows,
                'wind_rows': wind_rows,
                'records_imported': 0
            }

        # Identify month columns (columns with 'Netgen' in name)
        month_columns = [col for col in df.columns if 'Netgen' in str(col)]

        # Extract month names from column headers
        month_names = []
        for col in month_columns:
            # Extract month name (e.g., "Netgen January" -> "January")
            month_name = col.replace('Netgen ', '').strip()
            month_names.append(month_name)

        # Determine which year column to use
        # Some files have 'YEAR' with wrong data and 'Year' with correct data
        if 'Year' in df.columns:
            year_col = 'Year'
        else:
            year_col = 'YEAR'

        # Prepare data for melting
        id_vars = ['Plant Id', 'Plant Name', 'Reported Fuel Type Code', year_col]

        logger.info(f"[{worker_name}] 📅 Melting {len(month_columns)} month columns...")

        # Melt from wide to long format
        melted = df.melt(
            id_vars=id_vars,
            value_vars=month_columns,
            var_name='month_col',
            value_name='generation_mwh'
        )

        # Extract month name from column name
        melted['month_name'] = melted['month_col'].str.replace('Netgen ', '').str.strip()

        # Convert generation to float and filter out NaN/zero values
        melted['generation_mwh'] = pd.to_numeric(melted['generation_mwh'], errors='coerce')
        melted = melted[melted['generation_mwh'].notna() & (melted['generation_mwh'] > 0)].copy()

        logger.info(f"[{worker_name}] 📊 Records after melting: {len(melted):,}")

        # Create period_start (first day of month)
        melted['period_start'] = pd.to_datetime(
            melted[year_col].astype(str) + '-' + melted['month_name'],
            format='%Y-%B',
            errors='coerce'
        )

        # Remove rows with invalid dates
        melted = melted[melted['period_start'].notna()].copy()

        # Create period_end (first day of next month) using pandas DateOffset
        melted['period_end'] = melted['period_start'] + pd.DateOffset(months=1)

        # Prepare records for import
        records = []
        for _, row in melted.iterrows():
            record = {
                'source': 'EIA',
                'source_type': 'excel',
                'identifier': str(row['Plant Id']),
                'period_type': 'month',
                'period_start': row['period_start'].isoformat(),
                'period_end': row['period_end'].isoformat(),
                'value_extracted': float(row['generation_mwh']),
                'unit': 'MWh',
                'data': json.dumps({
                    'plant_id': int(row['Plant Id']),
                    'plant_name': str(row['Plant Name']),
                    'fuel_type': 'WND',
                    'month': row['month_name'],
                    'year': int(row[year_col]),
                    'generation_mwh': float(row['generation_mwh'])
                })
            }
            records.append(record)

        logger.info(f"[{worker_name}] 💾 Importing {len(records):,} records...")

        # Import using COPY
        imported = asyncio.run(import_with_copy_bulk(db_url, records))

        logger.info(f"[{worker_name}] ✅ Completed {file_name}: {imported:,} records")

        return {
            'file': file_name,
            'worker': worker_id,
            'total_rows': total_rows,
            'wind_rows': wind_rows,
            'configured_rows': configured_rows,
            'records_imported': imported
        }

    except Exception as e:
        logger.error(f"[{worker_name}] ❌ Error processing {file_name}: {e}")
        return {
            'file': file_name,
            'worker': worker_id,
            'error': str(e)
        }


def run_worker(
    file_path: str,
    worker_id: int,
    plant_ids_file: str,
    db_url: str,
    result_queue: Queue
):
    """Worker process wrapper."""
    try:
        # Load plant IDs
        with open(plant_ids_file, 'rb') as f:
            plant_ids = pickle.load(f)

        result = process_eia_file(file_path, plant_ids, worker_id, db_url, result_queue)
        result_queue.put(result)

    except Exception as e:
        logger.error(f"Worker {worker_id} crashed: {e}")
        result_queue.put({
            'file': os.path.basename(file_path),
            'worker': worker_id,
            'error': str(e)
        })


async def get_database_stats() -> Dict[str, Any]:
    """Get current database statistics."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Count EIA records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'EIA')
        )
        eia_count = result.scalar() or 0

        # Get date range
        date_range_result = await db.execute(
            select(
                func.min(GenerationDataRaw.period_start),
                func.max(GenerationDataRaw.period_start)
            )
            .where(GenerationDataRaw.source == 'EIA')
        )
        date_range = date_range_result.first()

        return {
            'eia_count': eia_count,
            'min_date': date_range[0] if date_range else None,
            'max_date': date_range[1] if date_range else None
        }


async def run_all_async_operations(clean: bool = True):
    """Run all async operations."""

    # Get plant IDs
    print("\n🔍 Fetching EIA plant IDs...")
    plant_ids = await get_eia_plant_ids()

    if not plant_ids:
        print("\n⚠️ No generation units found with source='EIA'")
        print("   Please configure EIA generation units first")
        return None

    print(f"   Found {len(plant_ids)} configured EIA plants")

    # Clear existing data if requested
    if clean:
        await clear_existing_eia_data()

    # Get initial stats
    print("\n📊 Initial database stats:")
    initial_stats = await get_database_stats()
    print(f"   Current EIA records: {initial_stats['eia_count']:,}")

    return {
        'plant_ids': plant_ids,
        'initial_stats': initial_stats
    }


def import_parallel_optimized(
    num_workers: int = 4,
    clean: bool = True,
    sample: Optional[int] = None
):
    """Main import function with parallel processing."""

    print("\n" + "="*80)
    print(" "*25 + "🌬️  EIA WIND DATA IMPORT 🌬️")
    print("="*80)

    # Run async operations
    async_result = asyncio.run(run_all_async_operations(clean=clean))

    if not async_result:
        return

    plant_ids = async_result['plant_ids']
    initial_stats = async_result['initial_stats']

    # Save plant IDs for workers
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pkl')
    with open(temp_file.name, 'wb') as f:
        pickle.dump(plant_ids, f)
    plant_ids_file = temp_file.name

    # Get database URL
    from app.core.config import get_settings
    settings = get_settings()
    db_url = settings.database_url_async.replace('+asyncpg', '')

    # Get Excel files
    data_folder = Path(__file__).parent / "data"
    excel_files = sorted(data_folder.glob("*.xlsx"))

    if not excel_files:
        print(f"❌ No Excel files found in {data_folder}")
        os.unlink(plant_ids_file)
        return

    # Limit to sample if requested
    if sample:
        excel_files = excel_files[:sample]

    print(f"\n📁 Found {len(excel_files)} Excel files")

    # Show file years
    years = [f.stem.split()[-1] for f in excel_files]
    print(f"   Years: {years[0]} - {years[-1]}")

    # Confirm
    response = input("\n❓ Proceed with import? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled")
        os.unlink(plant_ids_file)
        return

    # Start workers
    actual_workers = min(num_workers, len(excel_files))
    print(f"\n🚀 Starting {actual_workers} workers...")

    result_queue = Queue()
    workers = []
    start_time = datetime.now()

    print("\n" + "="*80)
    print(" "*30 + "📊 IMPORT PROGRESS 📊")
    print("="*80 + "\n")

    # Start workers
    next_file_index = 0
    active_workers = {}

    # Start initial batch of workers
    for i in range(min(actual_workers, len(excel_files))):
        p = Process(
            target=run_worker,
            args=(
                str(excel_files[next_file_index]),
                i,
                plant_ids_file,
                db_url,
                result_queue
            ),
            name=f"Worker-{i}"
        )
        p.start()
        active_workers[p] = i
        next_file_index += 1
        time.sleep(0.2)

    # Monitor and assign new files
    results = []

    try:
        with tqdm(total=len(excel_files), desc="Overall Progress", unit="file") as pbar:
            while active_workers or next_file_index < len(excel_files):
                # Check for completed workers
                for p, worker_id in list(active_workers.items()):
                    if not p.is_alive():
                        # Worker completed
                        if not result_queue.empty():
                            result = result_queue.get()
                            results.append(result)
                            pbar.update(1)

                        del active_workers[p]

                        # Assign next file if available
                        if next_file_index < len(excel_files):
                            new_p = Process(
                                target=run_worker,
                                args=(
                                    str(excel_files[next_file_index]),
                                    worker_id,
                                    plant_ids_file,
                                    db_url,
                                    result_queue
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
                    pbar.update(1)

                time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted! Stopping workers...")
        for p in active_workers.keys():
            if p.is_alive():
                p.terminate()
                p.join()
        os.unlink(plant_ids_file)
        return
    finally:
        if os.path.exists(plant_ids_file):
            os.unlink(plant_ids_file)

    total_duration = (datetime.now() - start_time).total_seconds()

    # Summary
    print("\n" + "="*80)
    print(" "*30 + "📈 IMPORT SUMMARY 📈")
    print("="*80)

    total_imported = 0
    total_wind_rows = 0

    for result in sorted(results, key=lambda x: x.get('file', '')):
        if 'error' not in result:
            print(f"\n📁 {result['file']}")
            print(f"   Wind rows: {result.get('wind_rows', 0):,}")
            print(f"   Imported: {result.get('records_imported', 0):,}")

            total_imported += result.get('records_imported', 0)
            total_wind_rows += result.get('wind_rows', 0)
        else:
            print(f"\n❌ {result['file']}: {result['error']}")

    print(f"\n📊 Final Statistics:")
    print(f"   Files processed: {len(results)}")
    print(f"   Total wind rows: {total_wind_rows:,}")
    print(f"   Total records imported: {total_imported:,}")
    print(f"\n⏱️  Performance:")
    print(f"   Duration: {total_duration/60:.1f} minutes")
    print(f"   Import rate: {total_imported/total_duration:.0f} records/second" if total_duration > 0 else "")

    # Get final stats
    print("\n📊 Final database stats:")
    try:
        final_stats = asyncio.run(get_database_stats())
        print(f"   Total EIA records: {final_stats['eia_count']:,}")
        if final_stats['min_date'] and final_stats['max_date']:
            print(f"   Date range: {final_stats['min_date'].strftime('%Y-%m')} to {final_stats['max_date'].strftime('%Y-%m')}")
    except Exception as e:
        logger.warning(f"Could not get final stats: {e}")

    print("\n" + "="*80)
    print(" "*25 + "✨ EIA IMPORT COMPLETED ✨")
    print("="*80)
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='EIA wind generation data import')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--no-clean', action='store_true', help='Do not clear existing data')
    parser.add_argument('--sample', type=int, help='Process only first N files (for testing)')

    args = parser.parse_args()

    # Check system
    print(f"💻 System: {cpu_count()} CPUs, {psutil.virtual_memory().total/1e9:.1f}GB RAM")

    import_parallel_optimized(
        num_workers=args.workers,
        clean=not args.no_clean,
        sample=args.sample
    )
