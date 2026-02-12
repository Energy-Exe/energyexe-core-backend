"""Process raw price data from price_data_raw to windfarm-level price_data table.

This script takes bidzone-level prices from price_data_raw and creates
windfarm-level hourly prices in the price_data table by mapping each
windfarm to its associated bidzone.

Usage:
    poetry run python scripts/seeds/power_prices/process_to_hourly.py

Options:
    --windfarm-ids: Comma-separated list of windfarm IDs to process
    --bidzone-codes: Comma-separated list of bidzone codes to process
    --start-date: Start date (YYYY-MM-DD format)
    --end-date: End date (YYYY-MM-DD format)
    --force: Reprocess even if data exists
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from app.services.price_processing_service import PriceProcessingService


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime."""
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return dt.replace(tzinfo=timezone.utc)


def parse_int_list(value: str) -> List[int]:
    """Parse comma-separated string to list of ints."""
    if not value:
        return []
    return [int(x.strip()) for x in value.split(',')]


def parse_str_list(value: str) -> List[str]:
    """Parse comma-separated string to list of strings."""
    if not value:
        return []
    return [x.strip() for x in value.split(',')]


async def process_prices(
    windfarm_ids: Optional[List[int]] = None,
    bidzone_codes: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    force_reprocess: bool = False,
    source: str = "ENTSOE",
):
    """Process raw prices to windfarm-level hourly data.

    Args:
        windfarm_ids: Optional list of windfarm IDs to process
        bidzone_codes: Optional list of bidzone codes to process
        start_date: Optional start date filter
        end_date: Optional end date filter
        force_reprocess: If True, reprocess even if data exists
        source: Price data source ("ENTSOE" or "ELEXON")
    """
    print("=" * 60)
    print("Process Price Data to Windfarm-Level Hourly")
    print("=" * 60)

    # Print processing parameters
    print(f"Source: {source}")
    print(f"Windfarm IDs: {windfarm_ids or 'All'}")
    print(f"Bidzone Codes: {bidzone_codes or 'All'}")
    print(f"Start Date: {start_date or 'Not specified'}")
    print(f"End Date: {end_date or 'Not specified'}")
    print(f"Force Reprocess: {force_reprocess}")
    print("=" * 60)

    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        service = PriceProcessingService(db)

        print("\nProcessing raw prices to windfarm-level hourly data...")

        result = await service.process_raw_to_hourly(
            windfarm_ids=windfarm_ids,
            bidzone_codes=bidzone_codes,
            start_date=start_date,
            end_date=end_date,
            force_reprocess=force_reprocess,
            source=source,
        )

    # Print results
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Success: {result['success']}")
    print(f"Windfarms Processed: {result['windfarms_processed']}")
    print(f"Records Created: {result['records_created']}")
    print(f"Records Updated: {result['records_updated']}")
    print(f"Duration: {result.get('duration_seconds', 0)} seconds")

    if result.get('by_windfarm'):
        print(f"\nResults by Windfarm:")
        for wf_id, wf_data in list(result['by_windfarm'].items())[:20]:
            print(f"  {wf_data['name']} (ID: {wf_id}, Bidzone: {wf_data['bidzone']}): "
                  f"{wf_data['records_created']} created, {wf_data['records_updated']} updated")

    if result.get('errors'):
        print(f"\nErrors ({len(result['errors'])}):")
        for error in result['errors'][:10]:
            print(f"  - {error}")

    print("=" * 60)

    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Process raw price data to windfarm-level hourly data'
    )
    parser.add_argument(
        '--windfarm-ids',
        default=None,
        help='Comma-separated list of windfarm IDs to process'
    )
    parser.add_argument(
        '--bidzone-codes',
        default=None,
        help='Comma-separated list of bidzone codes to process'
    )
    parser.add_argument(
        '--start-date',
        default=None,
        help='Start date (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--end-date',
        default=None,
        help='End date (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Reprocess even if data exists'
    )
    parser.add_argument(
        '--source',
        default='ENTSOE',
        help='Price data source: ENTSOE or ELEXON (default: ENTSOE)'
    )

    args = parser.parse_args()

    # Parse arguments
    windfarm_ids = parse_int_list(args.windfarm_ids) if args.windfarm_ids else None
    bidzone_codes = parse_str_list(args.bidzone_codes) if args.bidzone_codes else None
    start_date = parse_date(args.start_date) if args.start_date else None
    end_date = parse_date(args.end_date) if args.end_date else None

    # Run processing
    asyncio.run(process_prices(
        windfarm_ids=windfarm_ids,
        bidzone_codes=bidzone_codes,
        start_date=start_date,
        end_date=end_date,
        force_reprocess=args.force,
        source=args.source,
    ))


if __name__ == '__main__':
    main()
