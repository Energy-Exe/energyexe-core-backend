#!/usr/bin/env python3
"""
ELEXON API Data Import Script

Fetches generation data from ELEXON Insights API and stores in generation_data_raw table.
ELEXON provides 30-minute settlement period data for UK BM Units.

Usage:
    # Fetch single day for all ELEXON windfarms
    poetry run python scripts/seeds/raw_generation_data/elexon/import_from_api.py \
        --start 2025-10-11 --end 2025-10-11

    # Fetch date range
    poetry run python scripts/seeds/raw_generation_data/elexon/import_from_api.py \
        --start 2025-10-01 --end 2025-10-07

    # Dry run (see what would be fetched)
    poetry run python scripts/seeds/raw_generation_data/elexon/import_from_api.py \
        --start 2025-10-11 --end 2025-10-11 --dry-run

Note: Requires ELEXON_API_KEY in .env file
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import argparse
from typing import List, Dict
import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.core.config import get_settings
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from app.services.elexon_client import ElexonClient
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()


async def get_elexon_bm_units() -> List[Dict]:
    """
    Get all ELEXON BM units from database.

    Returns:
        List of dicts with unit info
    """
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        stmt = select(GenerationUnit).where(GenerationUnit.source == "ELEXON")
        result = await db.execute(stmt)
        units = result.scalars().all()

        return [
            {
                'id': u.id,
                'code': u.code,
                'name': u.name,
                'windfarm_id': u.windfarm_id
            }
            for u in units if u.code and u.code != 'nan'
        ]


async def fetch_and_store_elexon_data(
    bm_units: List[Dict],
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False
) -> Dict:
    """
    Fetch ELEXON data for all BM units and store in database.

    Args:
        bm_units: List of BM unit dicts
        start_date: Start date
        end_date: End date
        dry_run: If True, don't actually store data

    Returns:
        Dict with results
    """
    result = {
        'total_units': len(bm_units),
        'api_calls': 0,
        'records_stored': 0,
        'records_updated': 0,
        'errors': []
    }

    try:
        logger.info(f"Fetching ELEXON data for {len(bm_units)} BM units")

        # Create ELEXON client
        client = ElexonClient()

        # Extract BM unit codes
        bm_unit_codes = [u['code'] for u in bm_units]

        # Fetch data for all BM units (ONE API call)
        df, metadata = await client.fetch_physical_data(
            start=start_date,
            end=end_date,
            bm_units=bm_unit_codes,
        )

        result['api_calls'] = 1

        if df.empty:
            logger.warning("No data returned from ELEXON API")
            result['errors'].append("No data available from API")
            return result

        logger.info(f"Received {len(df)} records from ELEXON API")

        if dry_run:
            result['records_stored'] = len(df)
            logger.info(f"DRY RUN: Would store {len(df)} records")
            return result

        # Store data for each unit
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            # Map BM unit codes to unit objects for quick lookup
            unit_map = {u['code']: u for u in bm_units}

            # Group data by BM unit
            for bm_unit_code in df['bm_unit'].unique() if 'bm_unit' in df.columns else []:
                if bm_unit_code not in unit_map:
                    continue

                unit = unit_map[bm_unit_code]

                # Filter for this BM unit
                unit_df = df[df['bm_unit'] == bm_unit_code]

                if unit_df.empty:
                    continue

                # Prepare records for bulk upsert
                records_to_insert = []

                for idx, row in unit_df.iterrows():
                    # Extract timestamp
                    timestamp = row.get("timestamp", idx)
                    if not isinstance(timestamp, datetime):
                        import pandas as pd
                        timestamp = pd.to_datetime(timestamp)

                    # Ensure timezone-aware
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=timezone.utc)

                    # ELEXON uses 30-minute settlement periods
                    period_end = timestamp + timedelta(minutes=30)
                    period_type = "PT30M"

                    # Extract value
                    value = float(row.get("value", 0))

                    # Build data JSONB
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
                            "import_script": "import_from_api.py",
                        },
                    }

                    records_to_insert.append({
                        "source": "ELEXON",
                        "source_type": "api",
                        "identifier": bm_unit_code,
                        "period_start": timestamp,
                        "period_end": period_end,
                        "period_type": period_type,
                        "value_extracted": Decimal(str(value)),
                        "unit": "MWh",
                        "data": data,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    })

                if records_to_insert:
                    # Deduplicate records within this batch
                    # Keep last occurrence for each (source, identifier, period_start)
                    seen = {}
                    unique_records = []

                    for record in records_to_insert:
                        key = (record['source'], record['identifier'], record['period_start'])
                        # Keep the last one (overwrite if already seen)
                        seen[key] = record

                    unique_records = list(seen.values())

                    if len(unique_records) < len(records_to_insert):
                        logger.warning(
                            f"Deduped {len(records_to_insert) - len(unique_records)} duplicates "
                            f"within batch for unit {bm_unit_code}"
                        )

                    # Bulk upsert with deduplicated records
                    stmt = insert(GenerationDataRaw).values(unique_records)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['source', 'identifier', 'period_start'],
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

                    result['records_stored'] += len(unique_records)
                    logger.info(f"Stored {len(unique_records)} records for BM unit {bm_unit_code}")

        logger.info(f"Completed: {result['records_stored']} total records stored")

    except Exception as e:
        error_msg = f"Error processing ELEXON data: {str(e)}"
        logger.error(error_msg)
        result['errors'].append(error_msg)

    return result


async def main(start_date: str, end_date: str, dry_run: bool = False, chunk_days: int = 7):
    """
    Main import function with automatic chunking for large date ranges.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        dry_run: If True, don't actually store data
        chunk_days: Number of days per chunk (default: 7)
    """
    print("="*80)
    print(" " * 25 + "ELEXON API DATA IMPORT")
    print("="*80)
    print(f"Start Date: {start_date}")
    print(f"End Date: {end_date}")
    print(f"Dry Run: {dry_run}")
    print()

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)

    # Calculate total days
    total_days = (end - start).days + 1

    # Get BM units once
    print("Fetching ELEXON BM units from database...")
    bm_units = await get_elexon_bm_units()
    print(f"Found {len(bm_units)} BM units")

    # Determine if chunking is needed
    if total_days > chunk_days:
        num_chunks = (total_days + chunk_days - 1) // chunk_days
        print(f"\n⚠️  Large date range ({total_days} days) - will process in {num_chunks} chunks of {chunk_days} days")
    else:
        print(f"\nProcessing {total_days} day(s) in one batch")

    print("\n" + "="*80)
    print("Starting data fetch...")
    print("="*80 + "\n")

    # Process in chunks
    current_start = start
    chunk_results = []
    total_records = 0
    total_errors = 0
    chunk_num = 1

    while current_start <= end:
        # Calculate chunk end
        chunk_end = min(
            current_start + timedelta(days=chunk_days - 1, hours=23, minutes=59, seconds=59),
            end
        )

        chunk_days_actual = (chunk_end - current_start).days + 1

        print(f"\nChunk {chunk_num}: {current_start.date()} to {chunk_end.date()} ({chunk_days_actual} days)")
        print("-" * 60)

        # Fetch and store this chunk with retry
        max_retries = 3
        retry_count = 0
        result = None

        while retry_count < max_retries:
            try:
                result = await fetch_and_store_elexon_data(bm_units, current_start, chunk_end, dry_run)

                # Success - break retry loop
                break

            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"  ⚠️  Chunk failed (attempt {retry_count}/{max_retries}): {str(e)}")
                    print(f"     Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    print(f"  ❌ Chunk failed after {max_retries} attempts: {str(e)}")
                    # Create error result
                    result = {
                        'total_units': len(bm_units),
                        'api_calls': 0,
                        'records_stored': 0,
                        'records_updated': 0,
                        'errors': [f"Failed after {max_retries} retries: {str(e)}"]
                    }

        chunk_results.append(result)
        total_records += result['records_stored']
        total_errors += len(result['errors'])

        if result['errors']:
            print(f"  ⚠️  Chunk had {len(result['errors'])} error(s)")
            # Log failed chunk details for manual retry
            print(f"     To retry this chunk manually:")
            print(f"     poetry run python scripts/seeds/raw_generation_data/elexon/import_from_api.py \\")
            print(f"       --start {current_start.date()} --end {chunk_end.date()}")
        else:
            print(f"  ✅ Chunk completed: {result['records_stored']:,} records")

        # Move to next chunk
        current_start = chunk_end + timedelta(seconds=1)
        chunk_num += 1

        # Rate limiting between chunks
        if current_start <= end:
            await asyncio.sleep(2)

    # Print summary
    print("\n" + "="*80)
    print(" " * 30 + "SUMMARY")
    print("="*80)

    print(f"\nBM Units: {len(bm_units)}")
    print(f"Total Chunks: {len(chunk_results)}")
    print(f"Total API Calls: {sum(r['api_calls'] for r in chunk_results)}")
    print(f"Total Records Stored: {total_records:,}")
    print(f"Total Errors: {total_errors}")

    if total_errors > 0:
        print(f"\nChunks with errors:")
        for i, r in enumerate(chunk_results, 1):
            if r['errors']:
                print(f"  Chunk {i}: {len(r['errors'])} error(s)")
                for error in r['errors']:
                    print(f"    - {error}")

    print("\n" + "="*80)

    if dry_run:
        print("\n⚠️  DRY RUN - No data was actually stored")
    else:
        print("\n✅ Import completed!")

    if total_records > 0 and not dry_run:
        print("\n💡 Next step: Run aggregation to process raw data into generation_data table")
        print(f"   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \\")
        print(f"     --source ELEXON --start {start_date} --end {end_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import ELEXON data from API')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be fetched without storing')

    args = parser.parse_args()

    try:
        asyncio.run(main(
            start_date=args.start,
            end_date=args.end,
            dry_run=args.dry_run
        ))
    except KeyboardInterrupt:
        print("\n\n⚠️ Import cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
