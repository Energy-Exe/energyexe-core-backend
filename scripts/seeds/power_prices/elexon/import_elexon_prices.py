#!/usr/bin/env python3
"""
Elexon MID Price Data Import Script

Fetches Market Index Data (MID) day-ahead power prices from the Elexon BMRS API
and stores them in the price_data_raw table.

Uses the same GB EIC code (10YGB----------A) as ENTSOE so that the processing
pipeline can map prices to GB windfarms via their bidzone.

Data available from September 29, 2016 onward. Public API, no key required.

Usage:
    # Import full history
    poetry run python scripts/seeds/power_prices/elexon/import_elexon_prices.py \
        --start 2016-09-29 --end 2026-02-12

    # Import with larger chunks
    poetry run python scripts/seeds/power_prices/elexon/import_elexon_prices.py \
        --start 2016-09-29 --end 2026-02-12 --chunk-days 60

    # Dry run
    poetry run python scripts/seeds/power_prices/elexon/import_elexon_prices.py \
        --start 2026-02-01 --end 2026-02-12 --dry-run
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional

import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.price_data import PriceDataRaw
from app.services.elexon_client import ElexonClient
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()

# GB EIC code (same as ENTSOE uses for GB bidzone)
GB_EIC_CODE = "10YGB----------A"


async def fetch_and_store_prices(
    start: datetime,
    end: datetime,
    dry_run: bool = False,
) -> Dict:
    """
    Fetch MID prices for a date range and store in price_data_raw.

    Returns:
        Dict with records_stored, api_calls, errors
    """
    result = {
        "records_stored": 0,
        "api_calls": 0,
        "errors": [],
    }

    client = ElexonClient()

    try:
        df, metadata = await client.fetch_market_index_prices(
            start=start,
            end=end,
        )
        result["api_calls"] = metadata.get("api_calls", 1)

        if df.empty:
            errors = metadata.get("errors", [])
            if errors:
                result["errors"].extend(
                    e if isinstance(e, str) else str(e) for e in errors
                )
            else:
                result["errors"].append(
                    f"No MID data for {start.date()} to {end.date()}"
                )
            return result

        if dry_run:
            result["records_stored"] = len(df)
            return result

        # Prepare records for bulk upsert
        import pandas as pd

        records_to_insert = []
        now = datetime.now(timezone.utc)

        for _, row in df.iterrows():
            timestamp = row.get("timestamp")
            if not isinstance(timestamp, datetime):
                timestamp = pd.to_datetime(timestamp)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            period_end = timestamp + timedelta(hours=1)
            price = float(row.get("price", 0))
            volume = float(row.get("volume", 0))

            data = {
                "price": price,
                "volume": volume,
                "currency": "GBP",
                "unit": "GBP/MWh",
                "data_provider": "APXMIDP",
                "fetch_metadata": {
                    "fetch_timestamp": now.isoformat(),
                    "fetch_method": "api_script",
                    "import_script": "import_elexon_prices.py",
                },
            }

            records_to_insert.append({
                "source": "ELEXON",
                "source_type": "api",
                "price_type": "day_ahead",
                "identifier": GB_EIC_CODE,
                "period_start": timestamp,
                "period_end": period_end,
                "period_type": "PT60M",
                "value_extracted": Decimal(str(price)),
                "unit": "GBP/MWh",
                "currency": "GBP",
                "data": data,
                "created_at": now,
                "updated_at": now,
            })

        if not records_to_insert:
            return result

        # Bulk upsert
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            stmt = insert(PriceDataRaw).values(records_to_insert)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_price_raw_source_identifier_period_type",
                set_={
                    "value_extracted": stmt.excluded.value_extracted,
                    "data": stmt.excluded.data,
                    "updated_at": datetime.now(timezone.utc),
                    "period_end": stmt.excluded.period_end,
                    "period_type": stmt.excluded.period_type,
                    "unit": stmt.excluded.unit,
                    "currency": stmt.excluded.currency,
                },
            )
            await db.execute(stmt)
            await db.commit()

        result["records_stored"] = len(records_to_insert)
        logger.info(
            f"Stored {len(records_to_insert)} Elexon MID price records "
            f"({start.date()} to {end.date()})"
        )

    except Exception as e:
        error_msg = f"Error fetching Elexon MID prices: {str(e)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)

    return result


async def main(
    start_date: str,
    end_date: str,
    chunk_days: int = 30,
    dry_run: bool = False,
):
    """
    Main import function.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        chunk_days: Days per chunk for large date ranges
        dry_run: If True, don't store data
    """
    print("=" * 80)
    print(" " * 15 + "ELEXON MID PRICE DATA IMPORT")
    print("=" * 80)
    print(f"Start Date:  {start_date}")
    print(f"End Date:    {end_date}")
    print(f"Identifier:  {GB_EIC_CODE}")
    print(f"Chunk Days:  {chunk_days}")
    print(f"Dry Run:     {dry_run}")
    print()

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, tzinfo=timezone.utc
    )
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )

    total_days = (end - start).days + 1
    num_chunks = (total_days + chunk_days - 1) // chunk_days

    print(f"Total days: {total_days}")
    if total_days > chunk_days:
        print(f"Processing in {num_chunks} chunks of {chunk_days} days")

    print("\n" + "=" * 80)
    print("Starting Elexon MID price fetch...")
    print("=" * 80 + "\n")

    # Process chunks
    all_results = []
    current_start = start
    chunk_num = 1
    total_api_calls = 0

    while current_start <= end:
        chunk_end = min(
            current_start + timedelta(
                days=chunk_days - 1, hours=23, minutes=59, seconds=59
            ),
            end,
        )
        chunk_days_actual = (chunk_end - current_start).days + 1

        if total_days > chunk_days:
            print(f"\n{'=' * 70}")
            print(
                f"Chunk {chunk_num}/{num_chunks}: "
                f"{current_start.date()} to {chunk_end.date()} "
                f"({chunk_days_actual} days)"
            )
            print("=" * 70)

        # Retry logic
        max_retries = 3
        result = None

        for attempt in range(1, max_retries + 1):
            try:
                result = await fetch_and_store_prices(
                    start=current_start,
                    end=chunk_end,
                    dry_run=dry_run,
                )
                break
            except Exception as e:
                if attempt < max_retries:
                    wait = 2 ** attempt
                    logger.warning(
                        f"Chunk {chunk_num} failed (attempt {attempt}), "
                        f"retrying in {wait}s..."
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        f"Chunk {chunk_num} failed after {max_retries} attempts"
                    )
                    result = {
                        "records_stored": 0,
                        "api_calls": 0,
                        "errors": [
                            f"Failed after {max_retries} retries: {str(e)}"
                        ],
                    }

        all_results.append(result)
        total_api_calls += result.get("api_calls", 0)

        # Print progress
        status = "OK" if result["records_stored"] > 0 else "SKIP"
        if result["errors"]:
            status = "ERR"
        print(
            f"  {current_start.date()} to {chunk_end.date()}: "
            f"{result['records_stored']:>5} records [{status}]"
        )
        if result["errors"]:
            for err in result["errors"][:2]:
                print(f"    ERROR: {err[:100]}")

        # Move to next chunk
        current_start = chunk_end + timedelta(seconds=1)
        chunk_num += 1

        if current_start <= end:
            await asyncio.sleep(1)

    # Print summary (format matches _parse_import_output() expectations)
    print("\n" + "=" * 80)
    print(" " * 30 + "SUMMARY")
    print("=" * 80)

    total_records = sum(r["records_stored"] for r in all_results)
    total_errors = sum(len(r["errors"]) for r in all_results)

    print(f"\nTotal API Calls: {total_api_calls}")
    print(f"Total Records Stored: {total_records:,}")
    print(f"Total Errors: {total_errors}")

    print("\n" + "=" * 80)

    if dry_run:
        print("\nDRY RUN - No data was actually stored")
    else:
        print("\nImport completed!")

    if total_records > 0 and not dry_run:
        print("\nNext step: Process raw prices to windfarm-level hourly data:")
        print(
            f"  poetry run python scripts/seeds/power_prices/process_to_hourly.py \\"
        )
        print(
            f"    --source ELEXON --bidzone-codes 10YGB----------A \\"
        )
        print(
            f"    --start-date {start_date} --end-date {end_date} --force"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import Elexon MID price data from BMRS API"
    )
    parser.add_argument(
        "--start", required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", required=True, help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=30,
        help="Days per API chunk (default: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be fetched without storing",
    )

    args = parser.parse_args()

    try:
        asyncio.run(
            main(
                start_date=args.start,
                end_date=args.end,
                chunk_days=args.chunk_days,
                dry_run=args.dry_run,
            )
        )
    except KeyboardInterrupt:
        print("\n\nImport cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
