#!/usr/bin/env python3
"""
ENTSOE API Diagnostic Script

Tests ENTSOE API connectivity for each configured control area.
Reports DataFrame shape, columns, resolution, and any errors.

Usage:
    cd /Users/mdfaisal/Documents/energyexe/energyexe-core-backend
    poetry run python scripts/debug/test_entsoe_api.py
    poetry run python scripts/debug/test_entsoe_api.py --days 3
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import argparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.core.config import get_settings
from app.services.entsoe_client import ENTSOEClient

import structlog
logger = structlog.get_logger()


# Control areas to test
TEST_AREAS = {
    "10YFR-RTE------C": "France (FR)",
    "10YDK-1--------W": "Denmark West (DK1)",
    "10YDK-2--------M": "Denmark East (DK2)",
    "10YBE----------2": "Belgium (BE)",
}


async def test_area(client: ENTSOEClient, area_code: str, area_name: str, start: datetime, end: datetime):
    """Test a single control area."""
    print(f"\n  Testing {area_name} ({area_code})...")

    try:
        df, metadata = await client.fetch_generation_per_unit(
            start=start.replace(tzinfo=None),
            end=end.replace(tzinfo=None),
            area_code=area_code,
            production_types=["wind"],
        )

        if df.empty:
            print(f"    RESULT: No data returned")
            if metadata.get("errors"):
                for err in metadata["errors"]:
                    print(f"    ERROR: {err}")
            return False

        # Analyze DataFrame
        print(f"    Shape: {df.shape}")
        print(f"    Columns: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")

        if "timestamp" in df.columns:
            import pandas as pd
            ts = pd.to_datetime(df["timestamp"])
            diffs = ts.diff().dropna()
            if not diffs.empty:
                min_delta = diffs.min().total_seconds()
                print(f"    Min timestamp delta: {min_delta}s ({int(min_delta/60)}min)")
                if min_delta <= 900:
                    print(f"    Resolution: PT15M (15-minute)")
                elif min_delta <= 1800:
                    print(f"    Resolution: PT30M (30-minute)")
                else:
                    print(f"    Resolution: PT60M (hourly)")

        if "eic_code" in df.columns:
            eics = df["eic_code"].unique()
            print(f"    Unique EIC codes: {len(eics)}")
            for eic in eics[:5]:
                count = len(df[df["eic_code"] == eic])
                print(f"      {eic}: {count} records")
            if len(eics) > 5:
                print(f"      ... and {len(eics) - 5} more")

        units_found = metadata.get("units_found", [])
        print(f"    Units found: {len(units_found)}")

        return True

    except Exception as e:
        print(f"    ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main(days_back: int = 2):
    settings = get_settings()

    print("=" * 70)
    print("  ENTSOE API Diagnostic Test")
    print("=" * 70)
    print(f"  API Key: {settings.ENTSOE_API_KEY[:10]}...")

    # Check entsoe-py version
    try:
        import entsoe
        print(f"  entsoe-py version: {entsoe.__version__}")
    except AttributeError:
        print(f"  entsoe-py version: (unknown)")

    # Test dates
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days_back)
    print(f"  Test period: {start.date()} to {end.date()}")

    client = ENTSOEClient()
    results = {}

    for area_code, area_name in TEST_AREAS.items():
        success = await test_area(client, area_code, area_name, start, end)
        results[area_name] = success
        await asyncio.sleep(1)  # Rate limiting

    # Summary
    print("\n" + "=" * 70)
    print("  Summary")
    print("=" * 70)
    for name, success in results.items():
        status = "\033[92mOK\033[0m" if success else "\033[91mFAIL\033[0m"
        print(f"  [{status}] {name}")

    passed = sum(1 for v in results.values() if v)
    print(f"\n  {passed}/{len(results)} areas returning data")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ENTSOE API Diagnostic Test')
    parser.add_argument('--days', type=int, default=2, help='Days back to test (default: 2)')
    args = parser.parse_args()

    asyncio.run(main(days_back=args.days))
