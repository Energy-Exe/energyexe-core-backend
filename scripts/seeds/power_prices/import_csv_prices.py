"""Import ENTSOE power price data from CSV files into price_data_raw table.

This script reads CSV files from the seeds/power_prices/entsoe directory
and imports them into the price_data_raw table for later processing.

CSV Format (tab-separated):
- InstanceCode
- DateTime(UTC)
- ResolutionCode
- AreaCode
- AreaDisplayName
- AreaTypeCode
- MapCode
- ContractType
- Sequence
- Price[Currency/MWh]
- Currency
- UpdateTime(UTC)

Usage:
    poetry run python scripts/seeds/power_prices/import_csv_prices.py

Options:
    --file-pattern: Glob pattern to match CSV files (default: "*.csv")
    --limit: Maximum number of files to process (default: None = all)
    --bidzone: Only import data for specific bidzone code
    --dry-run: Don't actually insert data, just show what would be imported
"""

import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
import argparse
from typing import Dict, Any, List, Optional, Set

import pandas as pd
from tqdm import tqdm
import psutil

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from app.core.config import get_settings
from app.models.price_data import PriceDataRaw
from app.models.bidzone import Bidzone
from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession


# Directory containing the CSV files
CSV_DIR = Path(__file__).parent.parent / "power_prices" / "entsoe"


async def get_valid_bidzone_codes() -> Set[str]:
    """Fetch bidzone codes from database."""
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Bidzone.code)
        )
        bidzone_codes = {row[0] for row in result}

    print(f"Found {len(bidzone_codes)} bidzones in the system")
    return bidzone_codes


def parse_csv_file(file_path: Path, valid_bidzones: Optional[Set[str]] = None) -> pd.DataFrame:
    """Parse ENTSOE CSV file (tab-separated) into DataFrame.

    Args:
        file_path: Path to CSV file
        valid_bidzones: Optional set of bidzone codes to filter by

    Returns:
        DataFrame with parsed price data
    """
    print(f"  Reading {file_path.name}...")

    # Read tab-separated CSV
    df = pd.read_csv(
        file_path,
        sep='\t',
        encoding='utf-8',
        dtype={
            'InstanceCode': str,
            'ResolutionCode': str,
            'AreaCode': str,
            'AreaDisplayName': str,
            'AreaTypeCode': str,
            'MapCode': str,
            'ContractType': str,
            'Sequence': str,
            'Currency': str,
        }
    )

    # Clean column names (remove spaces and brackets)
    df.columns = [col.strip() for col in df.columns]

    # Rename Price column if it has brackets
    price_col = [col for col in df.columns if 'Price' in col][0] if any('Price' in col for col in df.columns) else None
    if price_col and price_col != 'Price':
        df.rename(columns={price_col: 'Price'}, inplace=True)

    # Parse datetime columns
    df['DateTime(UTC)'] = pd.to_datetime(df['DateTime(UTC)'])

    # Filter to valid bidzones if specified
    if valid_bidzones:
        initial_count = len(df)
        df = df[df['AreaCode'].isin(valid_bidzones)]
        filtered_count = len(df)
        if initial_count != filtered_count:
            print(f"    Filtered from {initial_count} to {filtered_count} rows (matched bidzones)")

    return df


def transform_to_price_records(df: pd.DataFrame, source_file: str) -> List[Dict[str, Any]]:
    """Transform DataFrame rows to price_data_raw records.

    Args:
        df: DataFrame with price data
        source_file: Name of source file for tracking

    Returns:
        List of dicts ready for database insert (deduplicated)
    """
    records = []
    seen_keys = set()  # Track unique combinations to avoid duplicates in same batch
    now = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        # Parse datetime
        period_start = row['DateTime(UTC)']
        if period_start.tzinfo is None:
            period_start = period_start.replace(tzinfo=timezone.utc)

        # Calculate period end based on resolution
        resolution = row.get('ResolutionCode', 'PT60M')
        if resolution == 'PT60M':
            period_end = period_start + timedelta(hours=1)
            period_type = 'PT60M'
        elif resolution == 'PT15M':
            period_end = period_start + timedelta(minutes=15)
            period_type = 'PT15M'
        elif resolution == 'PT30M':
            period_end = period_start + timedelta(minutes=30)
            period_type = 'PT30M'
        else:
            # Unknown resolution - default to hourly but preserve the original code
            period_end = period_start + timedelta(hours=1)
            period_type = resolution  # Store the actual resolution code for investigation

        # Extract price value
        price = row.get('Price', 0)
        if pd.isna(price):
            price = 0

        # Determine price type from contract type
        contract_type = str(row.get('ContractType', '')).strip()
        if 'Intraday' in contract_type or 'intraday' in contract_type.lower():
            price_type = 'intraday'
        else:
            price_type = 'day_ahead'

        # Create unique key for deduplication (matches DB constraint)
        identifier = row.get('AreaCode', '')
        unique_key = (identifier, period_start, price_type)
        if unique_key in seen_keys:
            continue  # Skip duplicate
        seen_keys.add(unique_key)

        # Extract currency
        currency = row.get('Currency', 'EUR')
        if pd.isna(currency):
            currency = 'EUR'

        # Build data JSONB
        data = {
            'instance_code': row.get('InstanceCode', ''),
            'area_code': row.get('AreaCode', ''),
            'area_display_name': row.get('AreaDisplayName', ''),
            'area_type_code': row.get('AreaTypeCode', ''),
            'map_code': row.get('MapCode', ''),
            'contract_type': contract_type,
            'sequence': row.get('Sequence', ''),
            'price': float(price) if not pd.isna(price) else None,
            'currency': currency,
            'resolution_code': resolution,
            'update_time': str(row.get('UpdateTime(UTC)', '')),
            'source_file': source_file,
            'import_timestamp': now.isoformat(),
        }

        records.append({
            'source': 'ENTSOE',
            'source_type': 'csv',
            'price_type': price_type,
            'identifier': identifier,
            'period_start': period_start,
            'period_end': period_end,
            'period_type': period_type,
            'value_extracted': Decimal(str(price)) if not pd.isna(price) else Decimal('0'),
            'unit': f'{currency}/MWh',
            'currency': currency,
            'data': data,
            'created_at': now,
            'updated_at': now,
        })

    return records


async def bulk_insert_records(
    db: AsyncSession,
    records: List[Dict[str, Any]],
    batch_size: int = 2000,  # PostgreSQL has 32767 param limit; 13 params * 2000 = 26000
) -> tuple[int, int]:
    """Bulk insert records using PostgreSQL upsert.

    Args:
        db: Database session
        records: List of record dicts
        batch_size: Number of records per batch

    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if not records:
        return 0, 0

    total_inserted = 0

    # Process in batches
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]

        stmt = insert(PriceDataRaw).values(batch)

        # Upsert on conflict
        stmt = stmt.on_conflict_do_update(
            constraint='uq_price_raw_source_identifier_period_type',
            set_={
                'value_extracted': stmt.excluded.value_extracted,
                'data': stmt.excluded.data,
                'updated_at': datetime.now(timezone.utc),
                'period_end': stmt.excluded.period_end,
                'currency': stmt.excluded.currency,
                'unit': stmt.excluded.unit,
            }
        )

        await db.execute(stmt)
        await db.commit()

        total_inserted += len(batch)

    return total_inserted, 0


async def import_csv_files(
    file_pattern: str = "*.csv",
    limit: Optional[int] = None,
    bidzone_filter: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Import CSV files from the seeds directory.

    Args:
        file_pattern: Glob pattern to match files
        limit: Maximum number of files to process
        bidzone_filter: Only import data for this bidzone
        dry_run: Don't actually insert data

    Returns:
        Summary of the import operation
    """
    start_time = datetime.now()

    # Check directory exists
    if not CSV_DIR.exists():
        print(f"ERROR: CSV directory not found: {CSV_DIR}")
        return {'success': False, 'error': f'Directory not found: {CSV_DIR}'}

    # Find CSV files
    csv_files = sorted(CSV_DIR.glob(file_pattern))

    if limit:
        csv_files = csv_files[:limit]

    if not csv_files:
        print(f"No CSV files found matching pattern: {file_pattern}")
        return {'success': False, 'error': 'No files found'}

    print(f"\nFound {len(csv_files)} CSV files to process")

    # Get valid bidzone codes
    valid_bidzones = await get_valid_bidzone_codes()

    if bidzone_filter:
        valid_bidzones = {bidzone_filter}
        print(f"Filtering to bidzone: {bidzone_filter}")

    results = {
        'success': True,
        'files_processed': 0,
        'total_records': 0,
        'by_file': {},
        'by_bidzone': {},
        'errors': [],
    }

    AsyncSessionLocal = get_session_factory()

    for csv_file in tqdm(csv_files, desc="Processing files"):
        # Create a fresh session for each file to isolate connection errors
        try:
            async with AsyncSessionLocal() as db:
                # Parse CSV file
                df = parse_csv_file(csv_file, valid_bidzones)

                if df.empty:
                    print(f"  Skipping {csv_file.name} - no matching data")
                    continue

                # Transform to records
                records = transform_to_price_records(df, csv_file.name)

                if not dry_run:
                    # Insert records
                    inserted, updated = await bulk_insert_records(db, records)
                else:
                    inserted = len(records)
                    updated = 0
                    print(f"  [DRY RUN] Would insert {inserted} records")

                results['files_processed'] += 1
                results['total_records'] += inserted
                results['by_file'][csv_file.name] = {
                    'records': inserted,
                    'bidzones': df['AreaCode'].unique().tolist(),
                }

                # Track by bidzone
                for bidzone in df['AreaCode'].unique():
                    if bidzone not in results['by_bidzone']:
                        results['by_bidzone'][bidzone] = 0
                    results['by_bidzone'][bidzone] += len(df[df['AreaCode'] == bidzone])

        except Exception as e:
            error_msg = f"Error processing {csv_file.name}: {str(e)}"
            print(f"  ERROR: {error_msg}")
            results['errors'].append(error_msg)

    # Calculate duration
    end_time = datetime.now()
    results['duration_seconds'] = round((end_time - start_time).total_seconds(), 2)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Import ENTSOE power price CSV files'
    )
    parser.add_argument(
        '--file-pattern',
        default='*.csv',
        help='Glob pattern to match CSV files (default: *.csv)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum number of files to process'
    )
    parser.add_argument(
        '--bidzone',
        default=None,
        help='Only import data for specific bidzone code'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Don't actually insert data, just show what would be imported"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("ENTSOE Power Price CSV Import")
    print("=" * 60)
    print(f"CSV Directory: {CSV_DIR}")
    print(f"File Pattern: {args.file_pattern}")
    print(f"Limit: {args.limit or 'None'}")
    print(f"Bidzone Filter: {args.bidzone or 'None'}")
    print(f"Dry Run: {args.dry_run}")
    print("=" * 60)

    # Run the import
    results = asyncio.run(import_csv_files(
        file_pattern=args.file_pattern,
        limit=args.limit,
        bidzone_filter=args.bidzone,
        dry_run=args.dry_run,
    ))

    # Print summary
    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)
    print(f"Success: {results['success']}")
    print(f"Files Processed: {results['files_processed']}")
    print(f"Total Records: {results['total_records']}")
    print(f"Duration: {results.get('duration_seconds', 0)} seconds")

    if results['by_bidzone']:
        print(f"\nRecords by Bidzone:")
        for bidzone, count in sorted(results['by_bidzone'].items(), key=lambda x: -x[1])[:20]:
            print(f"  {bidzone}: {count:,}")

    if results['errors']:
        print(f"\nErrors ({len(results['errors'])}):")
        for error in results['errors'][:10]:
            print(f"  - {error}")

    print("=" * 60)


if __name__ == '__main__':
    main()
