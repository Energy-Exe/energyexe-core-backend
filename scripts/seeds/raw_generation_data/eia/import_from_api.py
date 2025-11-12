#!/usr/bin/env python3
"""
EIA API Data Import Script

Fetches monthly generation data from EIA API and stores in generation_data_raw table.
EIA provides monthly aggregated data for wind farms in the United States.

Features:
- Processes 1,537 plants in batches of 10 per API call (154 batches)
- Smart retry logic: Automatically splits failing batches into smaller sizes
- Bulk upsert to avoid duplicates

Usage:
    # Fetch recent months (e.g., first half of 2025)
    poetry run python scripts/seeds/raw_generation_data/eia/import_from_api.py \
        --start-year 2025 --start-month 1 --end-year 2025 --end-month 6

    # Fetch specific date range
    poetry run python scripts/seeds/raw_generation_data/eia/import_from_api.py \
        --start-year 2024 --start-month 12 --end-year 2025 --end-month 7

    # Dry run (see what would be fetched)
    poetry run python scripts/seeds/raw_generation_data/eia/import_from_api.py \
        --start-year 2025 --start-month 1 --end-year 2025 --end-month 6 --dry-run

Note:
- Requires EIA_API_KEY in .env file
- EIA data has 1-2 month publication lag (use historical dates)
- Some batches may show 500 errors - the retry logic handles this automatically
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
import argparse
from typing import List, Dict
import structlog
import pandas as pd

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.core.config import get_settings
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from app.services.eia_client import EIAClient
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()


async def get_eia_plant_codes() -> List[Dict]:
    """
    Get all unique EIA plant codes from database.

    Note: Multiple generation units may share the same code (phases/repowering).
    We deduplicate by code to avoid fetching the same plant data multiple times.

    Returns:
        List of dicts with unique plant codes
    """
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        stmt = select(GenerationUnit).where(GenerationUnit.source == "EIA")
        result = await db.execute(stmt)
        units = result.scalars().all()

        all_units = [
            {
                'id': u.id,
                'code': u.code,
                'name': u.name,
                'windfarm_id': u.windfarm_id
            }
            for u in units if u.code and u.code != 'nan'
        ]

        # Deduplicate by code (multiple phases may share same code)
        seen_codes = set()
        unique_units = []

        for unit in all_units:
            if unit['code'] not in seen_codes:
                unique_units.append(unit)
                seen_codes.add(unit['code'])

        logger.info(f"Total EIA units in database: {len(all_units)}")
        logger.info(f"Unique plant codes: {len(unique_units)}")
        logger.info(f"Duplicate codes (phases): {len(all_units) - len(unique_units)}")

        return unique_units


async def fetch_and_store_eia_data(
    plant_codes: List[Dict],
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    dry_run: bool = False,
    batch_size: int = 10
) -> Dict:
    """
    Fetch EIA data for all plant codes and store in database.
    Processes in batches to avoid URL length limit and API errors.

    Args:
        plant_codes: List of plant code dicts
        start_year: Start year
        start_month: Start month (1-12)
        end_year: End year
        end_month: End month (1-12)
        dry_run: If True, don't actually store data
        batch_size: Number of plant codes per API call (default 10)

    Returns:
        Dict with results
    """
    result = {
        'total_plants': len(plant_codes),
        'api_calls': 0,
        'records_stored': 0,
        'records_updated': 0,
        'errors': []
    }

    try:
        logger.info(f"Fetching EIA data for {len(plant_codes)} plants in batches of {batch_size}")

        # Create EIA client
        client = EIAClient()

        # Extract plant codes
        codes = [u['code'] for u in plant_codes]

        # Split into batches to avoid URL length limit
        num_batches = (len(codes) + batch_size - 1) // batch_size
        logger.info(f"Processing {num_batches} batches")

        async def fetch_with_retry(batch_codes, batch_label, current_size):
            """Fetch data with automatic retry on smaller batch sizes."""
            # Try fetching this batch
            df, metadata = await client.fetch_monthly_generation_data(
                plant_codes=batch_codes,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
            )

            result['api_calls'] += 1

            # If successful, return
            if metadata.get('success', False):
                return df

            # If failed and batch size > 1, retry with smaller batches
            if len(batch_codes) > 1:
                logger.warning(f"{batch_label}: Failed with {len(batch_codes)} codes, retrying with smaller batches")

                # Split in half and retry
                mid = len(batch_codes) // 2
                sub_batch_1 = batch_codes[:mid]
                sub_batch_2 = batch_codes[mid:]

                df1 = await fetch_with_retry(sub_batch_1, f"{batch_label}a", current_size // 2)
                df2 = await fetch_with_retry(sub_batch_2, f"{batch_label}b", current_size // 2)

                # Combine results
                dfs_to_combine = [df for df in [df1, df2] if not df.empty]
                if dfs_to_combine:
                    return pd.concat(dfs_to_combine, ignore_index=True)
                return pd.DataFrame()

            # If failed with single code, just log and continue
            error_msg = f"{batch_label}: Failed for single plant code {batch_codes[0]}"
            logger.warning(error_msg)
            result['errors'].append(error_msg)
            return pd.DataFrame()

        all_dfs = []
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(codes))
            batch_codes = codes[start_idx:end_idx]

            logger.info(f"Batch {batch_idx + 1}/{num_batches}: Fetching {len(batch_codes)} plant codes")

            # Fetch with automatic retry
            df = await fetch_with_retry(batch_codes, f"Batch {batch_idx + 1}", len(batch_codes))

            if not df.empty:
                logger.info(f"Batch {batch_idx + 1}: Received {len(df)} records")
                all_dfs.append(df)
            else:
                logger.warning(f"Batch {batch_idx + 1}: No data returned")

        # Combine all batches
        if all_dfs:
            df = pd.concat(all_dfs, ignore_index=True)
            logger.info(f"Total records from all batches: {len(df)}")
        else:
            logger.warning("No data returned from any EIA API batches")
            result['errors'].append("No data available from API")
            return result

        if dry_run:
            result['records_stored'] = len(df)
            logger.info(f"DRY RUN: Would store {len(df)} records")
            return result

        # Store data in database
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            # Map plant codes to plant objects for quick lookup
            plant_map = {u['code']: u for u in plant_codes}

            # Prepare records for bulk upsert
            records_to_insert = []

            for idx, row in df.iterrows():
                plant_code = str(row.get("plantCode", ""))

                if plant_code not in plant_map:
                    continue

                # Extract period (format: "YYYY-MM")
                period_str = row.get("period", "")
                if not period_str:
                    continue

                # Parse period
                try:
                    period_date = datetime.strptime(period_str, "%Y-%m")
                    period_date = period_date.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid period format: {period_str}")
                    continue

                # Calculate period_end (first day of next month)
                if period_date.month == 12:
                    period_end = period_date.replace(year=period_date.year + 1, month=1)
                else:
                    period_end = period_date.replace(month=period_date.month + 1)

                # Extract value
                value = float(row.get("generation", 0))

                # Skip zero or negative values
                if value <= 0:
                    continue

                # Build data JSONB
                data = {
                    "plant_code": plant_code,
                    "plant_name": str(row.get("plantName", "")),
                    "period": period_str,
                    "fuel_type": str(row.get("fuel2002", "WND")),
                    "state": str(row.get("state", "")),
                    "generation_unit": str(row.get("generationUnit", "megawatthours")),
                    "import_metadata": {
                        "import_timestamp": datetime.now(timezone.utc).isoformat(),
                        "import_method": "api_script",
                        "import_script": "import_from_api.py",
                    },
                }

                records_to_insert.append({
                    "source": "EIA",
                    "source_type": "api",
                    "identifier": plant_code,
                    "period_start": period_date,
                    "period_end": period_end,
                    "period_type": "month",
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
                        f"within batch"
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
                logger.info(f"Stored {len(unique_records)} records")

        logger.info(f"Completed: {result['records_stored']} total records stored")

    except Exception as e:
        error_msg = f"Error processing EIA data: {str(e)}"
        logger.error(error_msg)
        result['errors'].append(error_msg)

    return result


async def main(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    dry_run: bool = False
):
    """
    Main import function.

    Args:
        start_year: Start year
        start_month: Start month (1-12)
        end_year: End year
        end_month: End month (1-12)
        dry_run: If True, don't actually store data
    """
    print("="*80)
    print(" " * 25 + "EIA API DATA IMPORT")
    print("="*80)
    print(f"Start Date: {start_year}-{start_month:02d}")
    print(f"End Date: {end_year}-{end_month:02d}")
    print(f"Dry Run: {dry_run}")
    print()

    # Validate dates
    if start_year < 2000 or start_year > 2100:
        print(f"❌ Invalid start year: {start_year}")
        sys.exit(1)
    if start_month < 1 or start_month > 12:
        print(f"❌ Invalid start month: {start_month}")
        sys.exit(1)
    if end_year < 2000 or end_year > 2100:
        print(f"❌ Invalid end year: {end_year}")
        sys.exit(1)
    if end_month < 1 or end_month > 12:
        print(f"❌ Invalid end month: {end_month}")
        sys.exit(1)

    # Calculate total months
    total_months = (end_year - start_year) * 12 + (end_month - start_month) + 1

    # Get plant codes once
    print("Fetching EIA plant codes from database...")
    plant_codes = await get_eia_plant_codes()
    print(f"Found {len(plant_codes)} EIA plants")

    if not plant_codes:
        print("\n⚠️ No generation units found with source='EIA'")
        print("   Please configure EIA generation units first")
        return

    print(f"\nProcessing {total_months} month(s)")

    print("\n" + "="*80)
    print("Starting data fetch...")
    print("="*80 + "\n")

    # Fetch and store data
    result = await fetch_and_store_eia_data(
        plant_codes,
        start_year,
        start_month,
        end_year,
        end_month,
        dry_run
    )

    # Print summary
    print("\n" + "="*80)
    print(" " * 30 + "SUMMARY")
    print("="*80)

    print(f"\nPlant Codes: {len(plant_codes)}")
    print(f"Total API Calls: {result['api_calls']}")
    print(f"Total Records Stored: {result['records_stored']:,}")
    print(f"Total Errors: {len(result['errors'])}")

    if result['errors']:
        print(f"\nErrors:")
        for error in result['errors']:
            print(f"  - {error}")

    print("\n" + "="*80)

    if dry_run:
        print("\n⚠️  DRY RUN - No data was actually stored")
    else:
        print("\n✅ Import completed!")

    if result['records_stored'] > 0 and not dry_run:
        print("\n💡 Next step: Run aggregation to process raw data into generation_data table")
        print(f"   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py \\")
        print(f"     --source EIA --start {start_year}-{start_month:02d} --end {end_year}-{end_month:02d}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import EIA data from API')
    parser.add_argument('--start-year', type=int, required=True, help='Start year (YYYY)')
    parser.add_argument('--start-month', type=int, required=True, help='Start month (1-12)')
    parser.add_argument('--end-year', type=int, required=True, help='End year (YYYY)')
    parser.add_argument('--end-month', type=int, required=True, help='End month (1-12)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be fetched without storing')

    args = parser.parse_args()

    try:
        asyncio.run(main(
            start_year=args.start_year,
            start_month=args.start_month,
            end_year=args.end_year,
            end_month=args.end_month,
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
