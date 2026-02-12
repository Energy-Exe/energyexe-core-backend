#!/usr/bin/env python3
"""
ENTSOE Price Data Import Script

Fetches day-ahead (and optionally intraday) power prices from the ENTSOE
Transparency Platform API and stores them in the price_data_raw table.

Uses EIC codes as identifiers (matching CSV-imported data) so that the
processing pipeline can map prices to windfarms via bidzones.

Usage:
    # Import all 11 bidzones with windfarms, Nov 2025 to present
    poetry run python scripts/seeds/power_prices/import_prices_from_api.py \
        --start 2025-11-01 --end 2026-02-09

    # Import specific zones
    poetry run python scripts/seeds/power_prices/import_prices_from_api.py \
        --start 2025-11-01 --end 2026-02-09 --zones DK_1 DK_2 NO_1

    # Import with larger chunks for long backfills
    poetry run python scripts/seeds/power_prices/import_prices_from_api.py \
        --start 2021-01-01 --end 2026-02-09 --zones GB --chunk-days 90

    # Dry run
    poetry run python scripts/seeds/power_prices/import_prices_from_api.py \
        --start 2026-02-01 --end 2026-02-09 --zones DK_1 --dry-run
"""

import asyncio
import sys
import argparse
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Dict, Optional

import structlog

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from app.core.entsoe_mappings import AREA_CODE_TO_EIC, PRICE_IMPORT_BIDZONES
from app.models.price_data import PriceDataRaw
from app.services.entsoe_price_client import ENTSOEPriceClient
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()


async def fetch_and_store_prices(
    area_code: str,
    eic_code: str,
    start: datetime,
    end: datetime,
    price_types: List[str],
    dry_run: bool = False,
) -> Dict:
    """
    Fetch prices for a single bidzone and date range, store in price_data_raw.

    Returns:
        Dict with records_stored, api_calls, errors
    """
    result = {
        "area_code": area_code,
        "eic_code": eic_code,
        "records_stored": 0,
        "api_calls": 0,
        "errors": [],
    }

    client = ENTSOEPriceClient()

    try:
        df, metadata = await client.fetch_prices(
            start=start,
            end=end,
            area_code=area_code,
            price_types=price_types,
        )
        result["api_calls"] = len(price_types)

        if df.empty:
            errors = metadata.get("errors", [])
            if errors:
                result["errors"].extend(errors)
            else:
                result["errors"].append(f"No data returned for {area_code}")
            return result

        if dry_run:
            result["records_stored"] = len(df)
            return result

        # Prepare records for bulk upsert
        records_to_insert = []
        now = datetime.now(timezone.utc)

        for _, row in df.iterrows():
            import pandas as pd

            timestamp = row.get("timestamp")
            if not isinstance(timestamp, datetime):
                timestamp = pd.to_datetime(timestamp)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            period_end = timestamp + timedelta(hours=1)
            price = float(row.get("price", 0))
            currency = row.get("currency", "EUR")
            unit = row.get("unit", "EUR/MWh")
            price_type = row.get("price_type", "day_ahead")

            data = {
                "area_code": area_code,
                "price": price,
                "currency": currency,
                "unit": unit,
                "fetch_metadata": {
                    "fetch_timestamp": now.isoformat(),
                    "fetch_method": "api_script",
                    "import_script": "import_prices_from_api.py",
                },
            }

            records_to_insert.append({
                "source": "ENTSOE",
                "source_type": "api",
                "price_type": price_type,
                "identifier": eic_code,
                "period_start": timestamp,
                "period_end": period_end,
                "period_type": "PT60M",
                "value_extracted": Decimal(str(price)),
                "unit": unit,
                "currency": currency,
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
            f"Stored {len(records_to_insert)} price records for {area_code} ({eic_code})"
        )

    except Exception as e:
        error_msg = f"Error fetching prices for {area_code}: {str(e)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)

    return result


async def main(
    start_date: str,
    end_date: str,
    zones: Optional[List[str]] = None,
    price_types: Optional[List[str]] = None,
    chunk_days: int = 30,
    dry_run: bool = False,
):
    """
    Main import function.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        zones: Optional list of area codes (default: PRICE_IMPORT_BIDZONES)
        price_types: Price types to fetch (default: ["day_ahead"])
        chunk_days: Days per chunk for large date ranges
        dry_run: If True, don't store data
    """
    if zones is None:
        zones = list(PRICE_IMPORT_BIDZONES)
    if price_types is None:
        price_types = ["day_ahead"]

    print("=" * 80)
    print(" " * 20 + "ENTSOE PRICE DATA IMPORT")
    print("=" * 80)
    print(f"Start Date: {start_date}")
    print(f"End Date:   {end_date}")
    print(f"Zones:      {', '.join(zones)}")
    print(f"Price Types: {', '.join(price_types)}")
    print(f"Chunk Days: {chunk_days}")
    print(f"Dry Run:    {dry_run}")
    print()

    # Resolve EIC codes
    zone_eic_map = {}
    for zone in zones:
        eic = AREA_CODE_TO_EIC.get(zone)
        if not eic:
            print(f"WARNING: No EIC mapping for {zone}, skipping")
            continue
        zone_eic_map[zone] = eic
        print(f"  {zone:8} -> {eic}")

    if not zone_eic_map:
        print("ERROR: No valid zones to process")
        sys.exit(1)

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, tzinfo=timezone.utc
    )
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )

    total_days = (end - start).days + 1
    num_chunks = (total_days + chunk_days - 1) // chunk_days

    print(f"\nTotal days: {total_days}")
    if total_days > chunk_days:
        print(f"Processing in {num_chunks} chunks of {chunk_days} days")

    print("\n" + "=" * 80)
    print("Starting price fetch...")
    print("=" * 80 + "\n")

    # Process chunks
    all_results = []
    current_start = start
    chunk_num = 1
    total_api_calls = 0

    while current_start <= end:
        chunk_end = min(
            current_start + timedelta(days=chunk_days - 1, hours=23, minutes=59, seconds=59),
            end,
        )
        chunk_days_actual = (chunk_end - current_start).days + 1

        if total_days > chunk_days:
            print(f"\n{'=' * 70}")
            print(f"Chunk {chunk_num}/{num_chunks}: {current_start.date()} to {chunk_end.date()} ({chunk_days_actual} days)")
            print("=" * 70)

        chunk_results = []

        for area_code, eic_code in zone_eic_map.items():
            # Retry logic
            max_retries = 3
            result = None

            for attempt in range(1, max_retries + 1):
                try:
                    result = await fetch_and_store_prices(
                        area_code=area_code,
                        eic_code=eic_code,
                        start=current_start,
                        end=chunk_end,
                        price_types=price_types,
                        dry_run=dry_run,
                    )
                    break
                except Exception as e:
                    if attempt < max_retries:
                        wait = 2 ** attempt
                        logger.warning(
                            f"{area_code} failed (attempt {attempt}), retrying in {wait}s..."
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.error(f"{area_code} failed after {max_retries} attempts")
                        result = {
                            "area_code": area_code,
                            "eic_code": eic_code,
                            "records_stored": 0,
                            "api_calls": 0,
                            "errors": [f"Failed after {max_retries} retries: {str(e)}"],
                        }

            chunk_results.append(result)
            total_api_calls += result.get("api_calls", 0)

            # Print progress
            status = "OK" if result["records_stored"] > 0 else "SKIP"
            if result["errors"]:
                status = "ERR"
            print(
                f"  {area_code:8} ({eic_code}): {result['records_stored']:>5} records [{status}]"
            )

            # Rate limit between API calls
            await asyncio.sleep(1)

        all_results.extend(chunk_results)

        # Chunk summary
        if total_days > chunk_days:
            chunk_total = sum(r["records_stored"] for r in chunk_results)
            chunk_errors = sum(len(r["errors"]) for r in chunk_results)
            print(f"\n  Chunk {chunk_num} summary: {chunk_total:,} records, {chunk_errors} errors")

        # Move to next chunk
        current_start = chunk_end + timedelta(seconds=1)
        chunk_num += 1

        if current_start <= end:
            await asyncio.sleep(2)

    # Print summary (format matches _parse_import_output() expectations)
    print("\n" + "=" * 80)
    print(" " * 30 + "SUMMARY")
    print("=" * 80)

    total_records = sum(r["records_stored"] for r in all_results)
    total_errors = sum(len(r["errors"]) for r in all_results)

    print(f"\nTotal API Calls: {total_api_calls}")
    print(f"Total Records Stored: {total_records:,}")
    print(f"Total Errors: {total_errors}")

    # Results by zone
    zone_totals = {}
    for r in all_results:
        key = r["area_code"]
        if key not in zone_totals:
            zone_totals[key] = {"records": 0, "api_calls": 0, "errors": []}
        zone_totals[key]["records"] += r["records_stored"]
        zone_totals[key]["api_calls"] += r["api_calls"]
        zone_totals[key]["errors"].extend(r["errors"])

    print("\n\nResults by Bidzone:")
    print("-" * 80)

    for area_code, totals in sorted(zone_totals.items()):
        eic = zone_eic_map.get(area_code, "?")
        status_icon = "OK" if totals["records"] > 0 else "FAIL"
        print(f"\n  [{status_icon}] {area_code:8} ({eic})")
        print(f"       API Calls: {totals['api_calls']}")
        print(f"       Records:   {totals['records']:,}")
        if totals["errors"]:
            print(f"       Errors:    {len(totals['errors'])}")
            for err in totals["errors"][:3]:
                print(f"         - {err[:100]}")

    print("\n" + "=" * 80)

    if dry_run:
        print("\nDRY RUN - No data was actually stored")
    else:
        print("\nImport completed!")

    if total_records > 0 and not dry_run:
        print("\nNext step: Process raw prices to windfarm-level hourly data:")
        print(f"  poetry run python scripts/seeds/power_prices/process_to_hourly.py \\")
        print(f"    --start-date {start_date} --end-date {end_date} --force")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import ENTSOE price data from API")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--zones",
        nargs="+",
        help=f"Bidzone area codes (default: {', '.join(PRICE_IMPORT_BIDZONES)})",
    )
    parser.add_argument(
        "--price-types",
        nargs="+",
        default=["day_ahead"],
        help="Price types to fetch (default: day_ahead)",
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
                zones=args.zones,
                price_types=args.price_types,
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
