#!/usr/bin/env python3
"""
Spot-check ELEXON windfarm aggregated data against live B1610 API.

Fetches actual generation data directly from the ELEXON B1610 API for a given
windfarm's BMU codes, sums the half-hourly values into hourly totals, and
compares against the aggregated metered_mwh in generation_data.

Works for any ELEXON windfarm — just provide the windfarm name or BMU codes.

Usage:
    # Spot-check Farr for specific dates
    poetry run python scripts/seeds/raw_generation_data/elexon/spot_check_windfarm.py \
        --windfarm Farr --dates 2025-01-15 2025-03-20 2025-08-10

    # Spot-check by BMU codes directly
    poetry run python scripts/seeds/raw_generation_data/elexon/spot_check_windfarm.py \
        --bmu T_FARR-1 T_FARR-2 --dates 2025-06-03 2025-06-13

    # Spot-check a full month with random sampling (10 random days)
    poetry run python scripts/seeds/raw_generation_data/elexon/spot_check_windfarm.py \
        --windfarm Farr --month 2025-03 --sample 10

    # Spot-check all months in 2025 (1 random day per month)
    poetry run python scripts/seeds/raw_generation_data/elexon/spot_check_windfarm.py \
        --windfarm Farr --year 2025

    # Spot-check Aikengall
    poetry run python scripts/seeds/raw_generation_data/elexon/spot_check_windfarm.py \
        --windfarm Aikengall --year 2025

    # List all ELEXON windfarms and their BMU codes
    poetry run python scripts/seeds/raw_generation_data/elexon/spot_check_windfarm.py --list
"""

import asyncio
import sys
import random
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from typing import List, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
import asyncpg

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent.parent.parent))

from app.core.config import get_settings

# Tolerance for floating point comparison (MWh)
TOLERANCE = 0.1

# ELEXON B1610 streaming API
B1610_URL = "https://data.elexon.co.uk/bmrs/api/v1/datasets/B1610/stream"


def get_db_dsn() -> str:
    settings = get_settings()
    return str(settings.DATABASE_URL).replace('postgresql+asyncpg://', 'postgresql://')


async def get_windfarm_bmu_codes(conn: asyncpg.Connection, windfarm_name: str) -> List[str]:
    """Look up BMU codes for a windfarm by name (case-insensitive partial match)."""
    rows = await conn.fetch('''
        SELECT gu.code
        FROM generation_units gu
        JOIN windfarms wf ON gu.windfarm_id = wf.id
        WHERE gu.source = 'ELEXON'
          AND LOWER(wf.name) LIKE LOWER($1)
          AND gu.code IS NOT NULL
          AND gu.code != 'nan'
        ORDER BY gu.code
    ''', f'%{windfarm_name}%')
    return [r['code'] for r in rows]


async def list_elexon_windfarms(conn: asyncpg.Connection):
    """List all ELEXON windfarms and their BMU codes."""
    rows = await conn.fetch('''
        SELECT wf.name as windfarm_name, array_agg(gu.code ORDER BY gu.code) as bmu_codes
        FROM generation_units gu
        JOIN windfarms wf ON gu.windfarm_id = wf.id
        WHERE gu.source = 'ELEXON'
          AND gu.code IS NOT NULL
          AND gu.code != 'nan'
        GROUP BY wf.name
        ORDER BY wf.name
    ''')
    return rows


async def fetch_api_data(bmu_codes: List[str], check_date: date) -> Dict[datetime, float]:
    """
    Fetch B1610 data from ELEXON API for given BMUs on a specific date.
    Returns dict mapping UTC hour -> sum of MWh across all BMUs and settlement periods.
    """
    uk_tz = ZoneInfo("Europe/London")

    # ELEXON settlement day runs midnight-to-midnight UK time
    # We need to query by settlement date
    from_dt = datetime(check_date.year, check_date.month, check_date.day, tzinfo=uk_tz)
    to_dt = from_dt + timedelta(days=1)

    # Convert to UTC for API
    from_utc = from_dt.astimezone(timezone.utc)
    to_utc = to_dt.astimezone(timezone.utc)

    params_list = [
        ("from", from_utc.strftime("%Y-%m-%dT%H:%MZ")),
        ("to", to_utc.strftime("%Y-%m-%dT%H:%MZ")),
    ]
    for code in bmu_codes:
        params_list.append(("bmUnit", code))

    settings = get_settings()
    headers = {"Accept": "application/json"}
    if settings.ELEXON_API_KEY:
        headers["x-api-key"] = settings.ELEXON_API_KEY

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(B1610_URL, headers=headers, params=params_list)

        if response.status_code != 200:
            raise Exception(f"ELEXON API error {response.status_code}: {response.text[:200]}")

        data = response.json()

    if not data:
        return {}

    # Aggregate by UTC hour
    # Each record has settlementDate, settlementPeriod, quantity, bmUnit
    hourly_mwh = defaultdict(float)

    for record in data:
        settlement_date_str = record.get("settlementDate", "")
        settlement_period = record.get("settlementPeriod")
        quantity_mwh = record.get("quantity", 0)  # MWh for 30-min period

        if not settlement_date_str or settlement_period is None:
            continue

        # Convert settlement date + period to UTC hour
        # Settlement date is UK local date, period 1 starts at 00:00 UK time
        sd = datetime.strptime(settlement_date_str[:10], "%Y-%m-%d")
        uk_midnight = datetime(sd.year, sd.month, sd.day, tzinfo=uk_tz)
        sp_start_utc = uk_midnight.astimezone(timezone.utc) + timedelta(minutes=(settlement_period - 1) * 30)

        # The UTC hour this settlement period falls into
        utc_hour = sp_start_utc.replace(minute=0, second=0, microsecond=0)

        # B1610 quantity is already in MWh for the half-hour period
        mwh = float(quantity_mwh)
        hourly_mwh[utc_hour] += mwh

    return dict(hourly_mwh)


async def fetch_db_data(
    conn: asyncpg.Connection,
    bmu_codes: List[str],
    check_date: date
) -> Dict[datetime, Dict]:
    """
    Fetch aggregated data from generation_data for given BMUs on a specific date.
    Returns dict mapping UTC hour -> {metered, curtailed, generation}.
    """
    uk_tz = ZoneInfo("Europe/London")
    uk_midnight = datetime(check_date.year, check_date.month, check_date.day, tzinfo=uk_tz)
    start_utc = uk_midnight.astimezone(timezone.utc)
    end_utc = start_utc + timedelta(hours=24)

    rows = await conn.fetch('''
        SELECT gd.hour,
               SUM(gd.metered_mwh) as total_metered,
               SUM(gd.curtailed_mwh) as total_curtailed,
               SUM(gd.generation_mwh) as total_generation
        FROM generation_data gd
        JOIN generation_units gu ON gd.generation_unit_id = gu.id
        WHERE gu.code = ANY($1)
          AND gd.hour >= $2
          AND gd.hour < $3
        GROUP BY gd.hour
        ORDER BY gd.hour
    ''', bmu_codes, start_utc, end_utc)

    result = {}
    for r in rows:
        hour = r['hour']
        if hour.tzinfo is None:
            hour = hour.replace(tzinfo=timezone.utc)
        result[hour] = {
            'metered': float(r['total_metered']) if r['total_metered'] else 0.0,
            'curtailed': float(r['total_curtailed']) if r['total_curtailed'] else 0.0,
            'generation': float(r['total_generation']) if r['total_generation'] else 0.0,
        }
    return result


async def spot_check_day(
    conn: asyncpg.Connection,
    bmu_codes: List[str],
    check_date: date,
    verbose: bool = True
) -> Dict:
    """
    Spot-check a single day: compare API data vs DB aggregated data.
    Returns summary dict.
    """
    api_data = await fetch_api_data(bmu_codes, check_date)
    db_data = await fetch_db_data(conn, bmu_codes, check_date)

    # Get all hours from both sources
    all_hours = sorted(set(list(api_data.keys()) + list(db_data.keys())))

    results = {
        'date': check_date,
        'total_hours': len(all_hours),
        'pass': 0,
        'fail': 0,
        'missing_db': 0,
        'missing_api': 0,
        'failures': [],
    }

    if verbose:
        print(f"\n  {'Hour UTC':<22} {'API MWh':>10} {'DB Metered':>12} {'Diff':>10} {'Status':>8}")
        print(f"  {'-'*70}")

    for hour in all_hours:
        api_mwh = api_data.get(hour)
        db_entry = db_data.get(hour)

        if api_mwh is None:
            # Hour exists in DB but not in API — could be BOAV-only
            results['missing_api'] += 1
            if verbose:
                db_val = db_entry['metered'] if db_entry else 0
                print(f"  {hour.strftime('%Y-%m-%d %H:%M'):22} {'no API':>10} {db_val:12.2f} {'':>10} {'API-ONLY':>8}")
            continue

        if db_entry is None:
            results['missing_db'] += 1
            results['failures'].append({
                'hour': hour, 'api': api_mwh, 'db': None, 'status': 'MISSING'
            })
            if verbose:
                print(f"  {hour.strftime('%Y-%m-%d %H:%M'):22} {api_mwh:10.2f} {'N/A':>12} {'N/A':>10} {'MISSING':>8}")
            continue

        db_metered = db_entry['metered']
        diff = db_metered - api_mwh

        if abs(diff) <= TOLERANCE:
            results['pass'] += 1
            status = "OK"
        else:
            results['fail'] += 1
            status = "FAIL"
            results['failures'].append({
                'hour': hour, 'api': api_mwh, 'db': db_metered, 'diff': diff, 'status': 'FAIL'
            })

        if verbose and (status == "FAIL" or status == "MISSING"):
            curt = db_entry['curtailed']
            print(f"  {hour.strftime('%Y-%m-%d %H:%M'):22} {api_mwh:10.2f} {db_metered:12.2f} {diff:10.2f} {status:>8}  curt={curt:.2f}")
        elif verbose:
            print(f"  {hour.strftime('%Y-%m-%d %H:%M'):22} {api_mwh:10.2f} {db_metered:12.2f} {diff:10.2f} {status:>8}")

    return results


def get_dates_for_month(year: int, month: int) -> List[date]:
    """Get all dates in a month."""
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    dates = []
    d = start
    while d < end:
        dates.append(d)
        d += timedelta(days=1)
    return dates


async def main():
    parser = argparse.ArgumentParser(description="Spot-check ELEXON windfarm data against B1610 API")
    parser.add_argument('--windfarm', type=str, help='Windfarm name (partial match)')
    parser.add_argument('--bmu', nargs='+', help='BMU codes directly')
    parser.add_argument('--dates', nargs='+', help='Specific dates to check (YYYY-MM-DD)')
    parser.add_argument('--month', type=str, help='Check a month (YYYY-MM)')
    parser.add_argument('--year', type=int, help='Check all months in a year (1 random day per month)')
    parser.add_argument('--sample', type=int, default=3, help='Number of random days to sample per month (default: 3)')
    parser.add_argument('--tolerance', type=float, default=0.1, help='MWh tolerance for matching (default: 0.1)')
    parser.add_argument('--list', action='store_true', help='List all ELEXON windfarms')
    parser.add_argument('--verbose', action='store_true', help='Show hourly detail for each day')
    args = parser.parse_args()

    global TOLERANCE
    TOLERANCE = args.tolerance

    dsn = get_db_dsn()
    conn = await asyncpg.connect(dsn)

    try:
        # List mode
        if args.list:
            windfarms = await list_elexon_windfarms(conn)
            print(f"\nELEXON Windfarms ({len(windfarms)}):")
            print(f"{'Windfarm':<40} {'BMU Codes'}")
            print(f"{'-'*80}")
            for wf in windfarms:
                codes = ', '.join(wf['bmu_codes'])
                print(f"{wf['windfarm_name']:<40} {codes}")
            return

        # Resolve BMU codes
        bmu_codes = args.bmu
        windfarm_label = "Custom BMUs"

        if args.windfarm:
            bmu_codes = await get_windfarm_bmu_codes(conn, args.windfarm)
            windfarm_label = args.windfarm
            if not bmu_codes:
                print(f"No ELEXON BMU codes found for windfarm matching '{args.windfarm}'")
                print("Use --list to see available windfarms")
                return

        if not bmu_codes:
            print("Please provide --windfarm or --bmu")
            parser.print_help()
            return

        print(f"\nSpot-checking: {windfarm_label}")
        print(f"BMU codes: {', '.join(bmu_codes)}")
        print(f"Tolerance: {TOLERANCE} MWh")

        # Determine dates to check
        check_dates = []

        if args.dates:
            check_dates = [datetime.strptime(d, '%Y-%m-%d').date() for d in args.dates]

        elif args.month:
            year, month = map(int, args.month.split('-'))
            all_dates = get_dates_for_month(year, month)
            # Don't include future dates
            today = date.today()
            all_dates = [d for d in all_dates if d <= today]
            if args.sample and args.sample < len(all_dates):
                check_dates = sorted(random.sample(all_dates, args.sample))
            else:
                check_dates = all_dates

        elif args.year:
            today = date.today()
            for month in range(1, 13):
                all_dates = get_dates_for_month(args.year, month)
                all_dates = [d for d in all_dates if d <= today]
                if not all_dates:
                    continue
                sample_size = min(args.sample, len(all_dates))
                check_dates.extend(sorted(random.sample(all_dates, sample_size)))

        else:
            print("Please provide --dates, --month, or --year")
            parser.print_help()
            return

        print(f"Checking {len(check_dates)} days\n")
        print(f"{'='*80}")

        # Run checks
        all_results = []
        total_pass = 0
        total_fail = 0
        total_missing = 0
        total_hours = 0

        for check_date in check_dates:
            print(f"\n{check_date.strftime('%Y-%m-%d')} ({check_date.strftime('%A')})")
            try:
                result = await spot_check_day(conn, bmu_codes, check_date, verbose=args.verbose)
                all_results.append(result)

                day_total = result['pass'] + result['fail'] + result['missing_db']
                total_pass += result['pass']
                total_fail += result['fail']
                total_missing += result['missing_db']
                total_hours += day_total

                status_str = "PASS" if result['fail'] == 0 and result['missing_db'] == 0 else "ISSUES"
                print(f"  => {result['pass']}/{day_total} hours OK"
                      f" | {result['fail']} fail | {result['missing_db']} missing"
                      f"  [{status_str}]")

                if result['failures'] and not args.verbose:
                    for f in result['failures'][:3]:
                        if f['status'] == 'MISSING':
                            print(f"     ! {f['hour'].strftime('%H:%M')} API={f['api']:.2f} DB=N/A")
                        else:
                            print(f"     ! {f['hour'].strftime('%H:%M')} API={f['api']:.2f} DB={f['db']:.2f} diff={f['diff']:+.2f}")
                    if len(result['failures']) > 3:
                        print(f"     ... and {len(result['failures']) - 3} more issues")

            except Exception as e:
                print(f"  => ERROR: {e}")
                all_results.append({'date': check_date, 'pass': 0, 'fail': 0, 'missing_db': 0, 'total_hours': 0})

        # Final summary
        print(f"\n{'='*80}")
        print(f"SUMMARY — {windfarm_label} ({', '.join(bmu_codes)})")
        print(f"{'='*80}")
        print(f"Days checked:    {len(check_dates)}")
        print(f"Total hours:     {total_hours}")
        print(f"  PASS:          {total_pass} ({100*total_pass/total_hours:.1f}%)" if total_hours else "  PASS: 0")
        print(f"  FAIL:          {total_fail}")
        print(f"  MISSING in DB: {total_missing}")

        # Days with issues
        problem_days = [r for r in all_results if r.get('fail', 0) > 0 or r.get('missing_db', 0) > 0]
        if problem_days:
            print(f"\nDays with issues ({len(problem_days)}):")
            for r in problem_days:
                print(f"  {r['date']} — {r.get('fail',0)} fail, {r.get('missing_db',0)} missing")
        else:
            print(f"\nAll {len(check_dates)} days PASS!")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
