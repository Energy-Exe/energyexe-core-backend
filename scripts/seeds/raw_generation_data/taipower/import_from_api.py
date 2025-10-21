#!/usr/bin/env python3
"""
Taipower API Data Import Script

Fetches live generation data from Taipower API and stores in generation_data_raw table.

NOTE: Taipower API only provides CURRENT/LIVE data (not historical).
      Run this script hourly/daily to build historical data over time.

Usage:
    # Fetch and store current snapshot
    poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py

    # Dry run (see what would be fetched)
    poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py --dry-run

    # Specify units to import (by Chinese code)
    poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py --units "彰工" "海能風"
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
import argparse
from typing import List, Dict, Optional
import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from app.services.taipower_client import TaipowerClient
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()


async def get_configured_units() -> Dict[str, GenerationUnit]:
    """Get all configured Taipower generation units from database."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(GenerationUnit)
            .where(GenerationUnit.source == "Taipower")
            .order_by(GenerationUnit.name)
        )
        units = result.scalars().all()

        # Map by code
        units_by_code = {unit.code: unit for unit in units}

        logger.info(f"Found {len(units)} configured Taipower units in database")

        return units_by_code


async def fetch_and_store_taipower_data(
    unit_filter: Optional[List[str]] = None,
    dry_run: bool = False
) -> Dict:
    """
    Fetch current Taipower data and store in database.

    Args:
        unit_filter: Optional list of unit codes to import (filters the API response)
        dry_run: If True, don't actually store data

    Returns:
        Dict with import results
    """
    result = {
        'success': True,
        'timestamp': None,
        'units_in_api': 0,
        'units_matched': 0,
        'records_stored': 0,
        'records_updated': 0,
        'errors': []
    }

    try:
        # Get configured units
        configured_units = await get_configured_units()

        if not configured_units:
            logger.warning("No Taipower units configured in database")
            result['errors'].append("No Taipower units configured in database")
            result['success'] = False
            return result

        # Create client and fetch live data
        client = TaipowerClient()
        api_data, metadata = await client.fetch_live_data()

        if not metadata.get('success') or not api_data:
            logger.error("Failed to fetch Taipower data")
            result['errors'].extend(metadata.get('errors', []))
            result['success'] = False
            return result

        result['timestamp'] = api_data.datetime
        result['units_in_api'] = len(api_data.generation_units)

        logger.info(
            f"Fetched {len(api_data.generation_units)} units from Taipower API "
            f"at {api_data.datetime}"
        )

        # Filter for wind generation units only
        wind_units = [
            u for u in api_data.generation_units
            if u.generation_type == '風力'
        ]

        logger.info(f"Filtered to {len(wind_units)} wind generation units")

        if dry_run:
            result['records_stored'] = len(wind_units)
            logger.info(f"DRY RUN: Would store {len(wind_units)} records")

            # Show which units would be imported
            print("\nUnits that would be imported:")
            for unit in wind_units:
                # Try to match to configured units
                matched = unit.unit_name in configured_units
                status = "✅" if matched else "❌"
                print(f"  {status} {unit.unit_name}: {unit.net_generation_mw} MW")

            return result

        # Store data for each wind unit
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            records_to_upsert = []

            for unit_data in wind_units:
                # Map to configured unit
                unit_code = unit_data.unit_name
                configured_unit = configured_units.get(unit_code)

                if not configured_unit:
                    logger.debug(f"Unit '{unit_code}' not in configured units - skipping")
                    continue

                # Apply unit filter if specified
                if unit_filter and configured_unit.code not in unit_filter:
                    logger.debug(f"Unit '{configured_unit.code}' not in filter - skipping")
                    continue

                result['units_matched'] += 1

                # Create record
                record = {
                    'source': 'Taipower',
                    'source_type': 'api',
                    'identifier': configured_unit.code,
                    'period_start': api_data.datetime,
                    'period_end': api_data.datetime,  # Snapshot - start == end
                    'period_type': 'snapshot',
                    'value_extracted': Decimal(str(unit_data.net_generation_mw)),
                    'unit': 'MW',
                    'data': {
                        'generation_mw': unit_data.net_generation_mw,
                        'installed_capacity_mw': unit_data.installed_capacity_mw,
                        'capacity_factor': unit_data.capacity_utilization_percent,
                        'generation_type': unit_data.generation_type,
                        'unit_code': configured_unit.code,
                        'unit_name': configured_unit.name,
                        'generation_unit_id': configured_unit.id,
                        'windfarm_id': configured_unit.windfarm_id,
                        'notes': unit_data.notes,
                        'api_timestamp': api_data.datetime.isoformat(),
                        'import_metadata': {
                            'import_timestamp': datetime.now(timezone.utc).isoformat(),
                            'import_method': 'api_script',
                            'import_script': 'import_from_api.py'
                        }
                    }
                }
                records_to_upsert.append(record)

            if not records_to_upsert:
                logger.warning("No matching units found to import")
                result['errors'].append("No matching units found")
                return result

            # Bulk upsert using PostgreSQL INSERT...ON CONFLICT
            stmt = insert(GenerationDataRaw).values(records_to_upsert)

            # On conflict (same source, identifier, period_start), update the values
            stmt = stmt.on_conflict_do_update(
                index_elements=['source', 'identifier', 'period_start'],
                set_={
                    'value_extracted': stmt.excluded.value_extracted,
                    'data': stmt.excluded.data,
                    'updated_at': datetime.now(timezone.utc)
                }
            )

            await db.execute(stmt)
            await db.commit()

            result['records_stored'] = len(records_to_upsert)

            logger.info(
                f"Stored {len(records_to_upsert)} Taipower records for {api_data.datetime}"
            )

    except Exception as e:
        error_msg = f"Error processing Taipower data: {str(e)}"
        logger.error(error_msg)
        result['errors'].append(error_msg)
        result['success'] = False

    return result


async def main(units: Optional[List[str]] = None, dry_run: bool = False):
    """
    Main import function.

    Args:
        units: Optional list of unit codes to import (Chinese codes)
        dry_run: If True, don't actually store data
    """
    print("="*80)
    print(" "*25 + "TAIPOWER API IMPORT")
    print("="*80)

    if dry_run:
        print("\n⚠️  DRY RUN MODE - No data will be stored")

    if units:
        print(f"\n🎯 Filtering for units: {', '.join(units)}")

    print("\n" + "="*80)
    print("Fetching live data from Taipower API...")
    print("="*80)

    result = await fetch_and_store_taipower_data(
        unit_filter=units,
        dry_run=dry_run
    )

    # Print summary
    print("\n" + "="*80)
    print(" "*30 + "SUMMARY")
    print("="*80)

    if result['timestamp']:
        print(f"\nAPI Timestamp: {result['timestamp']}")

    print(f"Units in API Response: {result['units_in_api']}")
    print(f"Units Matched to Database: {result['units_matched']}")
    print(f"Records Stored: {result['records_stored']}")

    if result['errors']:
        print(f"\n❌ Errors ({len(result['errors'])}):")
        for error in result['errors']:
            print(f"  - {error}")

    print("\n" + "="*80)

    if result['success'] and result['records_stored'] > 0:
        print("\n✅ Import completed successfully!")

        if not dry_run:
            print("\n💡 Next step: Run aggregation to process into generation_data table")
            print("   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \\")
            print("     --source TAIPOWER --start 2025-10-21 --end 2025-10-21")
    elif result['success'] and result['records_stored'] == 0:
        print("\n⚠️  No data imported (no matching units)")
    else:
        print("\n❌ Import failed - see errors above")

    print("\n" + "="*80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Import live data from Taipower API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch and store current snapshot
  poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py

  # Dry run to see what would be imported
  poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py --dry-run

  # Import specific units only
  poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py --units "彰工" "海能風"

Note: Taipower API only provides current/live data. Run this script periodically
      (hourly or daily) to build historical data over time.
        """
    )

    parser.add_argument(
        '--units',
        nargs='+',
        help='Optional: Filter to specific unit codes (Chinese codes, e.g., "彰工")'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform dry run without storing data'
    )

    args = parser.parse_args()

    asyncio.run(main(
        units=args.units,
        dry_run=args.dry_run
    ))
