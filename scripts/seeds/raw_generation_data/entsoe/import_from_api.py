#!/usr/bin/env python3
"""
ENTSOE API Data Import Script

Fetches generation data from ENTSOE Transparency Platform API and stores in generation_data_raw table.
Uses optimized bidding zone grouping to minimize API calls.

Usage:
    # Fetch single day for all ENTSOE windfarms
    poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
        --start 2025-10-11 --end 2025-10-11

    # Fetch date range
    poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
        --start 2025-10-01 --end 2025-10-07

    # Fetch specific control areas only
    poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
        --start 2025-10-11 --end 2025-10-11 --zones BE FR

    # Dry run (see what would be fetched)
    poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
        --start 2025-10-11 --end 2025-10-11 --dry-run
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import argparse
from typing import List, Dict, Set, Optional
import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.core.config import get_settings
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm
from app.models.bidzone import Bidzone
from app.services.entsoe_client import ENTSOEClient
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()


async def get_entsoe_units_by_control_area() -> Dict[str, Dict]:
    """
    Get all ENTSOE generation units grouped by control area.

    Returns:
        Dict mapping control_area_code to {control_area_name, windfarms, units, eic_codes}
    """
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Import ControlArea model
        from app.models.control_area import ControlArea

        # Get all windfarms with ENTSOE units
        stmt = (
            select(Windfarm)
            .options(selectinload(Windfarm.generation_units))
            .join(GenerationUnit, GenerationUnit.windfarm_id == Windfarm.id)
            .where(GenerationUnit.source == "ENTSOE")
            .distinct()
        )
        result = await db.execute(stmt)
        windfarms = result.scalars().all()

        # Group by control area
        control_area_groups = {}

        for windfarm in windfarms:
            # Get ENTSOE units
            entsoe_units = [u for u in windfarm.generation_units if u.source == "ENTSOE"]
            if not entsoe_units:
                continue

            # Get control area
            if not windfarm.control_area_id:
                logger.warning(f"Windfarm {windfarm.name} has no control area - skipping")
                continue

            ca_stmt = select(ControlArea).where(ControlArea.id == windfarm.control_area_id)
            ca_result = await db.execute(ca_stmt)
            control_area = ca_result.scalar_one_or_none()

            if not control_area or not control_area.code:
                logger.warning(f"Windfarm {windfarm.name} has invalid control area - skipping")
                continue

            # Initialize control area group
            if control_area.code not in control_area_groups:
                control_area_groups[control_area.code] = {
                    'control_area_name': control_area.name,
                    'windfarms': [],
                    'units': [],
                    'eic_codes': []
                }

            # Add to group
            control_area_groups[control_area.code]['windfarms'].append(windfarm)
            control_area_groups[control_area.code]['units'].extend(entsoe_units)
            control_area_groups[control_area.code]['eic_codes'].extend([
                u.code for u in entsoe_units if u.code and u.code != 'nan'
            ])

    return control_area_groups


async def fetch_and_store_control_area_data(
    control_area_code: str,
    area_data: Dict,
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False
) -> Dict:
    """
    Fetch data for a single control area and store in database.

    Args:
        control_area_code: EIC code of control area (e.g., '10Y1001A1001A796' for DK)
        area_data: Dict with windfarms, units, eic_codes
        start_date: Start date
        end_date: End date
        dry_run: If True, don't actually store data

    Returns:
        Dict with results
    """
    result = {
        'control_area': area_data['control_area_name'],
        'control_area_code': control_area_code,
        'windfarms': len(area_data['windfarms']),
        'units': len(area_data['units']),
        'api_calls': 0,
        'records_stored': 0,
        'records_updated': 0,
        'errors': []
    }

    try:
        logger.info(
            f"Processing {area_data['control_area_name']} "
            f"({len(area_data['windfarms'])} windfarms, {len(area_data['units'])} units)"
        )

        # Create ENTSOE client
        client = ENTSOEClient()

        # Convert to naive UTC
        start_naive = start_date.replace(tzinfo=None) if start_date.tzinfo else start_date
        end_naive = end_date.replace(tzinfo=None) if end_date.tzinfo else end_date

        # Fetch data for entire control area (ONE API call)
        df, metadata = await client.fetch_generation_per_unit(
            start=start_naive,
            end=end_naive,
            area_code=control_area_code,
            eic_codes=area_data['eic_codes'],
            production_types=["wind"],
        )

        result['api_calls'] = 1

        if df.empty:
            logger.warning(f"No data returned for {area_data['control_area_name']}")
            result['errors'].append("No data available from API")
            return result

        logger.info(f"Received {len(df)} records for {area_data['control_area_name']}")

        if dry_run:
            result['records_stored'] = len(df)
            logger.info(f"DRY RUN: Would store {len(df)} records")
            return result

        # Store data for each unit
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            for unit in area_data['units']:
                # Filter for this unit's EIC code
                unit_df = df[df.get('eic_code', df.get('unit_code', '')) == unit.code]

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

                    # Determine period type
                    resolution = row.get("resolution_code", "PT60M")
                    if resolution == "PT15M":
                        period_end = timestamp + timedelta(minutes=15)
                        period_type = "PT15M"
                    elif resolution == "PT60M":
                        period_end = timestamp + timedelta(hours=1)
                        period_type = "PT60M"
                    else:
                        period_end = timestamp + timedelta(hours=1)
                        period_type = "PT60M"

                    # Extract value
                    value = float(row.get("value", 0))

                    # Build data JSONB
                    data = {
                        "eic_code": unit.code,
                        "area_code": control_area_code,
                        "production_type": row.get("production_type", "wind"),
                        "resolution_code": resolution,
                        "installed_capacity_mw": float(row["installed_capacity_mw"])
                            if "installed_capacity_mw" in row and pd.notna(row["installed_capacity_mw"])
                            else None,
                        "import_metadata": {
                            "import_timestamp": datetime.now(timezone.utc).isoformat(),
                            "import_method": "api_script",
                            "import_script": "import_from_api.py",
                        },
                    }

                    records_to_insert.append({
                        "source": "ENTSOE",
                        "source_type": "api",
                        "identifier": unit.code,
                        "period_start": timestamp,
                        "period_end": period_end,
                        "period_type": period_type,
                        "value_extracted": Decimal(str(value)),
                        "unit": "MW",
                        "data": data,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    })

                if records_to_insert:
                    # Bulk upsert
                    stmt = insert(GenerationDataRaw).values(records_to_insert)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['source', 'source_type', 'identifier', 'period_start'],
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

                    result['records_stored'] += len(records_to_insert)
                    logger.info(f"Stored {len(records_to_insert)} records for unit {unit.code}")

        logger.info(
            f"Completed {area_data['control_area_name']}: "
            f"{result['records_stored']} records stored"
        )

    except Exception as e:
        error_msg = f"Error processing {area_data['control_area_name']}: {str(e)}"
        logger.error(error_msg)
        result['errors'].append(error_msg)

    return result


async def main(start_date: str, end_date: str, zones: Optional[List[str]] = None, dry_run: bool = False, chunk_days: int = 7):
    """
    Main import function with automatic chunking for large date ranges.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        zones: Optional list of control area names to process (e.g., ['BE', 'FR', 'DK'])
        dry_run: If True, don't actually store data
        chunk_days: Number of days per chunk (default: 7)
    """
    print("="*80)
    print(" " * 25 + "ENTSOE API DATA IMPORT")
    print("="*80)
    print(f"Start Date: {start_date}")
    print(f"End Date: {end_date}")
    if zones:
        print(f"Zones: {', '.join(zones)}")
    else:
        print("Zones: All available")
    print(f"Dry Run: {dry_run}")
    print()

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)

    # Calculate total days
    total_days = (end - start).days + 1

    # Get units grouped by control area once
    print("Fetching ENTSOE units from database...")
    control_area_groups = await get_entsoe_units_by_control_area()

    # Filter by specified zones if provided
    if zones:
        control_area_groups = {
            code: data for code, data in control_area_groups.items()
            if data['control_area_name'] in zones
        }

    print(f"\nFound {len(control_area_groups)} control areas to process:")
    for code, data in control_area_groups.items():
        print(f"  {data['control_area_name']:15} ({code}): {len(data['windfarms'])} windfarms, {len(data['units'])} units")

    # Determine if chunking is needed
    if total_days > chunk_days:
        num_chunks = (total_days + chunk_days - 1) // chunk_days
        print(f"\n⚠️  Large date range ({total_days} days) - will process in {num_chunks} chunks of {chunk_days} days")
    else:
        print(f"\nProcessing {total_days} day(s) in one batch")

    print("\n" + "="*80)
    print("Starting data fetch...")
    print("="*80 + "\n")

    # Process in date chunks
    current_start = start
    all_chunk_results = []
    chunk_num = 1

    while current_start <= end:
        # Calculate chunk end
        chunk_end = min(
            current_start + timedelta(days=chunk_days - 1, hours=23, minutes=59, seconds=59),
            end
        )

        chunk_days_actual = (chunk_end - current_start).days + 1

        if total_days > chunk_days:
            print(f"\n{'='*70}")
            print(f"Chunk {chunk_num}: {current_start.date()} to {chunk_end.date()} ({chunk_days_actual} days)")
            print('='*70)

        # Process each bidding zone for this chunk
        chunk_results = []

        for control_area_code, zone_data in control_area_groups.items():
            # Retry logic for each zone
            max_retries = 3
            retry_count = 0
            result = None

            while retry_count < max_retries:
                try:
                    result = await fetch_and_store_control_area_data(
                        control_area_code,
                        zone_data,
                        current_start,
                        chunk_end,
                        dry_run
                    )
                    break  # Success

                except Exception as e:
                    error_str = str(e)

                    # Check if it's InvalidBusinessParameterError (control area doesn't support per-unit data)
                    if "InvalidBusinessParameterError" in error_str or "units_found: []" in error_str:
                        logger.warning(
                            f"⚠️  Control Area {area_data['control_area_name']} ({control_area_code}) does not support per-unit generation data API. "
                            f"This control area may not provide detailed per-unit data through ENTSOE API."
                        )
                        # Create skipped result (don't retry)
                        result = {
                            'control_area': area_data['control_area_name'],
                            'control_area_code': control_area_code,
                            'windfarms': len(area_data['windfarms']),
                            'units': len(area_data['units']),
                            'api_calls': 0,
                            'records_stored': 0,
                            'records_updated': 0,
                            'errors': [f"Control area does not support per-unit data API (InvalidBusinessParameterError)"]
                        }
                        break  # Don't retry for this type of error

                    # For other errors, retry
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"Control Area {area_data['control_area_name']} failed (attempt {retry_count}), retrying...")
                        await asyncio.sleep(5)
                    else:
                        logger.error(f"Control Area {area_data['control_area_name']} failed after {max_retries} attempts")
                        # Create error result
                        result = {
                            'control_area': area_data['control_area_name'],
                            'control_area_code': control_area_code,
                            'windfarms': len(area_data['windfarms']),
                            'units': len(area_data['units']),
                            'api_calls': 0,
                            'records_stored': 0,
                            'records_updated': 0,
                            'errors': [f"Failed after {max_retries} retries: {str(e)}"]
                        }

            chunk_results.append(result)

            # Rate limiting between zones
            await asyncio.sleep(1)

        all_chunk_results.extend(chunk_results)

        # Show chunk summary if chunking
        if total_days > chunk_days:
            chunk_total = sum(r['records_stored'] for r in chunk_results)
            chunk_errors = sum(len(r['errors']) for r in chunk_results)
            print(f"\n  Chunk {chunk_num} summary: {chunk_total:,} records, {chunk_errors} errors")

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

    total_api_calls = sum(r['api_calls'] for r in all_chunk_results)
    total_records = sum(r['records_stored'] for r in all_chunk_results)
    total_errors = sum(len(r['errors']) for r in all_chunk_results)

    print(f"\nTotal API Calls: {total_api_calls}")
    print(f"Total Records Stored: {total_records:,}")
    print(f"Total Errors: {total_errors}")

    # Group results by control area across all chunks
    area_totals = {}
    for r in all_chunk_results:
        area_key = r['control_area']
        if area_key not in area_totals:
            area_totals[area_key] = {
                'records': 0,
                'api_calls': 0,
                'errors': []
            }
        area_totals[area_key]['records'] += r['records_stored']
        area_totals[area_key]['api_calls'] += r['api_calls']
        area_totals[area_key]['errors'].extend(r['errors'])

    print("\n\nResults by Control Area:")
    print("-"*80)

    for area_name, totals in sorted(area_totals.items()):
        status = "✅" if totals['records'] > 0 else "❌"
        print(f"\n{status} {area_name}")
        print(f"   API Calls: {totals['api_calls']}")
        print(f"   Records: {totals['records']:,}")

        if totals['errors']:
            print(f"   Errors: {len(totals['errors'])}")

    print("\n" + "="*80)

    if dry_run:
        print("\n⚠️  DRY RUN - No data was actually stored")
    else:
        print("\n✅ Import completed!")

    if total_records > 0 and not dry_run:
        print("\n💡 Next step: Run aggregation to process raw data into generation_data table")
        print(f"   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \\")
        print(f"     --source ENTSOE --start {start_date} --end {end_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import ENTSOE data from API')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--zones', nargs='+', help='Specific control areas to fetch (e.g., BE FR DK1)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be fetched without storing')

    args = parser.parse_args()

    try:
        asyncio.run(main(
            start_date=args.start,
            end_date=args.end,
            zones=args.zones,
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
