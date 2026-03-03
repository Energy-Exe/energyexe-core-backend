#!/usr/bin/env python3
"""
ECB Exchange Rate Import Script

Fetches daily exchange rates from the ECB Statistical Data Warehouse
and upserts them into the exchange_rates table.

Usage:
    # Full historical load (~16K records for 4 currencies since 2010)
    python scripts/seeds/exchange_rates/import_ecb_rates.py \
        --start 2010-01-01 --end 2026-02-28

    # Monthly refresh (last 7 days to fill gaps from weekends/holidays)
    python scripts/seeds/exchange_rates/import_ecb_rates.py \
        --start 2026-02-01 --end 2026-02-28

    # Dry run
    python scripts/seeds/exchange_rates/import_ecb_rates.py \
        --start 2026-02-01 --end 2026-02-28 --dry-run
"""

import asyncio
import sys
import argparse
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import structlog

# Add project root to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.exchange_rate import ExchangeRate
from app.services.ecb_client import ECBExchangeRateClient
from sqlalchemy.dialects.postgresql import insert

logger = structlog.get_logger()


async def import_exchange_rates(
    start_date: date,
    end_date: date,
    dry_run: bool = False,
) -> int:
    """
    Fetch ECB rates and upsert into exchange_rates table.

    Returns:
        Number of records stored.
    """
    client = ECBExchangeRateClient()
    df, metadata = await client.fetch_all_rates(start_date, end_date)

    if df is None or len(df) == 0:
        print("No rates fetched from ECB.")
        for m in metadata:
            if "error" in m:
                print(f"  {m['currency']}: {m['error']}")
        return 0

    print(f"Fetched {len(df)} rate records from ECB API")

    if dry_run:
        print("DRY RUN — no records stored")
        for currency in df["currency"].unique():
            subset = df[df["currency"] == currency]
            print(f"  {currency}: {len(subset)} records")
        return 0

    # Build records for upsert
    records = []
    for _, row in df.iterrows():
        rate = Decimal(str(row["rate"]))
        inverse = (Decimal("1") / rate).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        records.append({
            "base_currency": "EUR",
            "quote_currency": row["currency"],
            "rate_date": row["rate_date"],
            "rate": rate,
            "inverse_rate": inverse,
            "source": "ECB",
        })

    # Upsert in batches
    AsyncSessionLocal = get_session_factory()
    batch_size = 1000
    total_stored = 0

    async with AsyncSessionLocal() as session:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            stmt = insert(ExchangeRate).values(batch)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_exchange_rate_pair_date",
                set_={
                    "rate": stmt.excluded.rate,
                    "inverse_rate": stmt.excluded.inverse_rate,
                    "source": stmt.excluded.source,
                },
            )
            await session.execute(stmt)
            await session.commit()
            total_stored += len(batch)
            print(f"  Upserted batch {i // batch_size + 1}: {len(batch)} records")

    print(f"Records Stored: {total_stored}")
    return total_stored


def main():
    parser = argparse.ArgumentParser(description="Import ECB exchange rates")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't store")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    print(f"Importing ECB exchange rates: {start} to {end}")
    total = asyncio.run(import_exchange_rates(start, end, dry_run=args.dry_run))
    print(f"Done. Total records: {total}")


if __name__ == "__main__":
    main()
