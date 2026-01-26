#!/usr/bin/env python3
"""
ELEXON BOAV (Bid-Offer Acceptance Volumes) Import Script

Fetches bid/offer acceptance volumes from ELEXON BOAV API and stores in generation_data_raw table.
BOAV data captures curtailment (accepted bids) and dispatch instructions (accepted offers).

Key Concepts:
- Accepted Bids: Generator paid to REDUCE output (curtailment) - stored as negative values
- Accepted Offers: Generator paid to INCREASE output - stored as positive values

For calculating actual production:
    Actual Production = Metered Generation (B1610) + abs(Curtailed Volume from Bids)

Usage:
    # Fetch single day for all ELEXON windfarms (both bids and offers)
    poetry run python scripts/seeds/raw_generation_data/elexon/import_acceptance_volumes.py \
        --start 2025-10-11 --end 2025-10-11

    # Fetch bids only (curtailment data)
    poetry run python scripts/seeds/raw_generation_data/elexon/import_acceptance_volumes.py \
        --start 2025-10-11 --end 2025-10-11 --bid-offer bid

    # Fetch offers only
    poetry run python scripts/seeds/raw_generation_data/elexon/import_acceptance_volumes.py \
        --start 2025-10-11 --end 2025-10-11 --bid-offer offer

    # Dry run (see what would be fetched)
    poetry run python scripts/seeds/raw_generation_data/elexon/import_acceptance_volumes.py \
        --start 2025-10-11 --end 2025-10-11 --dry-run

Note: Requires ELEXON_API_KEY in .env file
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
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


async def fetch_and_store_boav_data(
    bm_units: List[Dict],
    settlement_date: date,
    bid_offer: str,
    dry_run: bool = False
) -> Dict:
    """
    Fetch BOAV data for a specific date and store in database.

    Args:
        bm_units: List of BM unit dicts
        settlement_date: Date to fetch
        bid_offer: 'bid' or 'offer'
        dry_run: If True, don't actually store data

    Returns:
        Dict with results
    """
    import pandas as pd

    result = {
        'settlement_date': str(settlement_date),
        'bid_offer': bid_offer,
        'total_units': len(bm_units),
        'api_calls': 0,
        'records_stored': 0,
        'records_updated': 0,
        'errors': []
    }

    try:
        logger.info(
            f"Fetching BOAV {bid_offer} data for {settlement_date}",
            bm_units_count=len(bm_units)
        )

        # Create ELEXON client
        client = ElexonClient()

        # Extract BM unit codes for filtering
        bm_unit_codes = [u['code'] for u in bm_units]

        # Fetch BOAV data
        df, metadata = await client.fetch_acceptance_volumes(
            settlement_date=settlement_date,
            bid_offer=bid_offer,
            bm_units=bm_unit_codes,
        )

        result['api_calls'] = 1

        if df.empty:
            logger.info(f"No BOAV {bid_offer} data for {settlement_date}")
            return result

        logger.info(
            f"Received {len(df)} BOAV {bid_offer} records",
            bm_units_found=len(metadata.get('bm_units_found', set()))
        )

        if dry_run:
            result['records_stored'] = len(df)
            logger.info(f"DRY RUN: Would store {len(df)} records")
            return result

        # Store data
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            # Map BM unit codes for quick lookup
            unit_map = {u['code']: u for u in bm_units}

            # Prepare records for bulk upsert
            records_to_insert = []

            for idx, row in df.iterrows():
                bm_unit_code = row.get("bm_unit")

                # Skip if not in our unit list
                if bm_unit_code not in unit_map:
                    continue

                # Extract timestamp
                timestamp_str = row.get("timestamp")
                if timestamp_str:
                    timestamp = datetime.fromisoformat(timestamp_str)
                else:
                    continue

                # Ensure timezone-aware
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)

                # BOAV uses 30-minute settlement periods
                period_end = timestamp + timedelta(minutes=30)
                period_type = "PT30M"

                # Extract value (negative for bids, positive for offers)
                value = float(row.get("total_volume_accepted", 0))

                # Build data JSONB with all available fields
                data = {
                    "bm_unit": bm_unit_code,
                    "acceptance_id": int(row.get("acceptance_id")) if pd.notna(row.get("acceptance_id")) else None,
                    "acceptance_duration": row.get("acceptance_duration"),
                    "total_volume_accepted": value,
                    "pair_volumes": row.get("pair_volumes"),
                    "settlement_date": str(row.get("settlement_date")),
                    "settlement_period": int(row.get("settlement_period")) if pd.notna(row.get("settlement_period")) else None,
                    "bid_offer": bid_offer,
                    "import_metadata": {
                        "import_timestamp": datetime.now(timezone.utc).isoformat(),
                        "import_method": "api_script",
                        "import_script": "import_acceptance_volumes.py",
                    },
                }

                # Use source_type to distinguish BOAV data from B1610
                source_type = f"boav_{bid_offer}"

                records_to_insert.append({
                    "source": "ELEXON",
                    "source_type": source_type,
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
                # Aggregate records for same (source, source_type, identifier, period_start)
                # Multiple acceptance IDs can exist for the same settlement period
                # We sum their volumes and store individual acceptance details in the JSON
                seen = {}
                for record in records_to_insert:
                    key = (
                        record['source'],
                        record['source_type'],
                        record['identifier'],
                        record['period_start']
                    )
                    if key not in seen:
                        # First record for this period - wrap acceptance in a list
                        record['data']['acceptances'] = [{
                            'acceptance_id': record['data'].get('acceptance_id'),
                            'total_volume_accepted': record['data'].get('total_volume_accepted'),
                            'acceptance_duration': record['data'].get('acceptance_duration'),
                            'pair_volumes': record['data'].get('pair_volumes'),
                        }]
                        record['data']['acceptance_count'] = 1
                        seen[key] = record
                    else:
                        # Additional acceptance for same period - sum value and append details
                        existing = seen[key]
                        existing_val = float(existing['value_extracted'])
                        new_val = float(record['value_extracted'])
                        summed = existing_val + new_val
                        existing['value_extracted'] = Decimal(str(summed))
                        existing['data']['total_volume_accepted'] = summed
                        # Clear top-level acceptance_id since it's now ambiguous
                        existing['data']['acceptance_id'] = None
                        existing['data']['acceptance_duration'] = None
                        existing['data']['acceptances'].append({
                            'acceptance_id': record['data'].get('acceptance_id'),
                            'total_volume_accepted': record['data'].get('total_volume_accepted'),
                            'acceptance_duration': record['data'].get('acceptance_duration'),
                            'pair_volumes': record['data'].get('pair_volumes'),
                        })
                        existing['data']['acceptance_count'] = len(existing['data']['acceptances'])

                unique_records = list(seen.values())

                if len(unique_records) < len(records_to_insert):
                    logger.info(
                        f"Aggregated {len(records_to_insert)} records into {len(unique_records)} "
                        f"(multiple acceptances per period merged)"
                    )

                # Batch insert to avoid PostgreSQL parameter limit (32767)
                BATCH_SIZE = 500
                total_stored = 0

                for i in range(0, len(unique_records), BATCH_SIZE):
                    batch = unique_records[i:i + BATCH_SIZE]

                    stmt = insert(GenerationDataRaw).values(batch)
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
                    total_stored += len(batch)

                await db.commit()

                result['records_stored'] = total_stored
                logger.info(f"Stored {total_stored} BOAV {bid_offer} records")

        logger.info(f"Completed: {result['records_stored']} total records stored")

    except Exception as e:
        error_msg = f"Error processing BOAV {bid_offer} data: {str(e)}"
        logger.error(error_msg)
        result['errors'].append(error_msg)

    return result


async def main(
    start_date: str,
    end_date: str,
    bid_offer: Optional[str] = None,
    dry_run: bool = False
):
    """
    Main import function.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        bid_offer: Optional - 'bid', 'offer', or None for both
        dry_run: If True, don't actually store data
    """
    print("=" * 80)
    print(" " * 20 + "ELEXON BOAV DATA IMPORT")
    print("=" * 80)
    print(f"Start Date: {start_date}")
    print(f"End Date: {end_date}")
    print(f"Bid/Offer: {bid_offer or 'both'}")
    print(f"Dry Run: {dry_run}")
    print()

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    # Calculate total days
    total_days = (end - start).days + 1

    # Get BM units once
    print("Fetching ELEXON BM units from database...")
    bm_units = await get_elexon_bm_units()
    print(f"Found {len(bm_units)} BM units")

    # Determine which types to fetch
    if bid_offer:
        bid_offer_types = [bid_offer]
    else:
        bid_offer_types = ['bid', 'offer']

    print(f"\nProcessing {total_days} day(s) for types: {', '.join(bid_offer_types)}")
    print("\n" + "=" * 80)
    print("Starting data fetch...")
    print("=" * 80 + "\n")

    # Process each day
    current_date = start
    total_records = 0
    total_errors = 0
    day_num = 1

    while current_date <= end:
        print(f"\nDay {day_num}/{total_days}: {current_date}")
        print("-" * 60)

        for bo_type in bid_offer_types:
            max_retries = 3
            retry_count = 0
            result = None

            while retry_count < max_retries:
                try:
                    result = await fetch_and_store_boav_data(
                        bm_units, current_date, bo_type, dry_run
                    )
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"  Failed (attempt {retry_count}/{max_retries}): {str(e)}")
                        print(f"  Retrying in 5 seconds...")
                        await asyncio.sleep(5)
                    else:
                        print(f"  Failed after {max_retries} attempts: {str(e)}")
                        result = {
                            'settlement_date': str(current_date),
                            'bid_offer': bo_type,
                            'total_units': len(bm_units),
                            'api_calls': 0,
                            'records_stored': 0,
                            'errors': [f"Failed after {max_retries} retries: {str(e)}"]
                        }

            total_records += result['records_stored']
            total_errors += len(result['errors'])

            if result['errors']:
                print(f"  {bo_type.upper()}: {len(result['errors'])} error(s)")
            else:
                print(f"  {bo_type.upper()}: {result['records_stored']:,} records")

            # Rate limiting between API calls
            await asyncio.sleep(1)

        # Move to next day
        current_date += timedelta(days=1)
        day_num += 1

        # Rate limiting between days
        if current_date <= end:
            await asyncio.sleep(2)

    # Print summary
    print("\n" + "=" * 80)
    print(" " * 30 + "SUMMARY")
    print("=" * 80)

    print(f"\nBM Units: {len(bm_units)}")
    print(f"Days Processed: {total_days}")
    print(f"Total Records Stored: {total_records:,}")
    print(f"Total Errors: {total_errors}")

    print("\n" + "=" * 80)

    if dry_run:
        print("\nDRY RUN - No data was actually stored")
    else:
        print("\nImport completed!")

    if total_records > 0 and not dry_run:
        print("\nNext step: Run aggregation to incorporate BOAV data into generation_data table")
        print(f"   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py \\")
        print(f"     --source ELEXON --date {start_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import ELEXON BOAV data from API')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument(
        '--bid-offer',
        choices=['bid', 'offer'],
        help='Fetch only bid or offer data (default: both)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be fetched without storing'
    )

    args = parser.parse_args()

    try:
        asyncio.run(main(
            start_date=args.start,
            end_date=args.end,
            bid_offer=args.bid_offer,
            dry_run=args.dry_run
        ))
    except KeyboardInterrupt:
        print("\n\nImport cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
