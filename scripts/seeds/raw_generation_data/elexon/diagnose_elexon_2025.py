#!/usr/bin/env python3
"""
Diagnostic script for ELEXON 2025 aggregation bugs.

Scans ALL ELEXON windfarms for:
1. Missing raw B1610 data (gaps in settlement periods)
2. NULL windfarm_id / generation_unit_id in aggregated records
3. Metered=0 when raw data exists (aggregation bug)
4. Hours with BOAV curtailment but no metered data

Also runs specific checks on Farr (T_FARR-1, T_FARR-2) test days from CSV comparison.

Usage:
    # Full scan of all ELEXON windfarms
    poetry run python scripts/seeds/raw_generation_data/elexon/diagnose_elexon_2025.py

    # Specific BMU codes only
    poetry run python scripts/seeds/raw_generation_data/elexon/diagnose_elexon_2025.py \
        --bmu T_FARR-1 T_FARR-2

    # Custom date range
    poetry run python scripts/seeds/raw_generation_data/elexon/diagnose_elexon_2025.py \
        --start 2025-06-01 --end 2025-06-30

    # Just the 5 Farr test days
    poetry run python scripts/seeds/raw_generation_data/elexon/diagnose_elexon_2025.py --farr-test-days
"""

import asyncio
import argparse
import functools
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

from pathlib import Path

# Override print to always flush
_original_print = print
print = functools.partial(_original_print, flush=True)

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

import asyncpg
from dotenv import load_dotenv

# Farr test days from the CSV comparison
FARR_TEST_CASES = [
    {'date': '2025-06-03', 'hour': 2,  'bug': 'Complete loss (hour before curtailment)', 'api_sum': 77.79},
    {'date': '2025-06-04', 'hour': 7,  'bug': 'Complete loss (hour before curtailment)', 'api_sum': 89.11},
    {'date': '2025-06-08', 'hour': 8,  'bug': 'Partial metered', 'api_sum': 9.91},
    {'date': '2025-06-08', 'hour': 9,  'bug': 'Partial metered', 'api_sum': 9.18},
    {'date': '2025-06-13', 'hour': 11, 'bug': 'Complete loss (hour before curtailment)', 'api_sum': 33.21},
    {'date': '2025-06-23', 'hour': 7,  'bug': 'Complete loss (hour before curtailment)', 'api_sum': 83.38},
]

UTC = ZoneInfo('UTC')


async def get_connection():
    """Get asyncpg connection."""
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    if db_url and db_url.startswith('postgresql+asyncpg://'):
        db_url = db_url.replace('postgresql+asyncpg://', 'postgresql://', 1)
    return await asyncpg.connect(db_url)


async def check_unique_constraint(conn):
    """Check the actual unique constraint on generation_data_raw."""
    print("\n" + "=" * 80)
    print("1. UNIQUE CONSTRAINT CHECK (generation_data_raw)")
    print("=" * 80)

    rows = await conn.fetch("""
        SELECT conname, pg_get_constraintdef(oid) as definition
        FROM pg_constraint
        WHERE conrelid = 'generation_data_raw'::regclass
        AND contype = 'u'
    """)

    if rows:
        for row in rows:
            print(f"  Constraint: {row['conname']}")
            print(f"  Definition: {row['definition']}")
    else:
        print("  No unique constraints found!")

    # Also check indexes
    rows = await conn.fetch("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'generation_data_raw'
        AND indexdef LIKE '%UNIQUE%'
    """)
    if rows:
        print("\n  Unique indexes:")
        for row in rows:
            print(f"    {row['indexname']}: {row['indexdef']}")


async def get_all_elexon_units(conn, bmu_codes=None):
    """Get all ELEXON generation units with windfarm info."""
    if bmu_codes:
        rows = await conn.fetch("""
            SELECT gu.id, gu.code, gu.name, gu.windfarm_id, gu.start_date,
                   gu.first_power_date, gu.capacity_mw, gu.source,
                   w.name as windfarm_name
            FROM generation_units gu
            LEFT JOIN windfarms w ON w.id = gu.windfarm_id
            WHERE gu.source = 'ELEXON' AND gu.code = ANY($1)
            ORDER BY gu.windfarm_id, gu.code
        """, bmu_codes)
    else:
        rows = await conn.fetch("""
            SELECT gu.id, gu.code, gu.name, gu.windfarm_id, gu.start_date,
                   gu.first_power_date, gu.capacity_mw, gu.source,
                   w.name as windfarm_name
            FROM generation_units gu
            LEFT JOIN windfarms w ON w.id = gu.windfarm_id
            WHERE gu.source = 'ELEXON'
            ORDER BY gu.windfarm_id, gu.code
        """)
    return rows


async def check_null_windfarm_ids(conn, start_date, end_date):
    """Check for NULL windfarm_id in aggregated ELEXON data."""
    print("\n" + "=" * 80)
    print("2. NULL windfarm_id / generation_unit_id CHECK")
    print("=" * 80)

    row = await conn.fetchrow("""
        SELECT
            COUNT(*) as total_records,
            SUM(CASE WHEN windfarm_id IS NULL THEN 1 ELSE 0 END) as null_windfarm,
            SUM(CASE WHEN generation_unit_id IS NULL THEN 1 ELSE 0 END) as null_unit
        FROM generation_data
        WHERE source = 'ELEXON'
        AND hour >= $1 AND hour < $2
    """, start_date, end_date)

    print(f"  Total ELEXON records: {row['total_records']:,}")
    print(f"  NULL windfarm_id: {row['null_windfarm']:,}")
    print(f"  NULL generation_unit_id: {row['null_unit']:,}")

    if row['null_windfarm'] > 0:
        print("\n  Top identifiers with NULL windfarm_id (via raw_data_ids):")
        # Sample some NULL records to understand which units are affected
        sample = await conn.fetch("""
            SELECT gd.hour, gd.metered_mwh, gd.generation_mwh, gd.raw_data_ids
            FROM generation_data gd
            WHERE gd.source = 'ELEXON'
            AND gd.windfarm_id IS NULL
            AND gd.hour >= $1 AND gd.hour < $2
            LIMIT 10
        """, start_date, end_date)
        for s in sample:
            print(f"    hour={s['hour']} metered={s['metered_mwh']} gen={s['generation_mwh']} raw_ids={s['raw_data_ids'][:3]}...")


async def check_metered_zero_with_raw_data(conn, start_date, end_date, bmu_codes=None):
    """Find hours where metered_mwh=0 but raw B1610 data exists."""
    print("\n" + "=" * 80)
    print("3. METERED=0 BUT RAW DATA EXISTS (aggregation bug)")
    print("=" * 80)

    # Find aggregated records with metered=0 or NULL that have BOAV curtailment
    if bmu_codes:
        rows = await conn.fetch("""
            SELECT gd.hour, gu.code, gd.metered_mwh, gd.curtailed_mwh, gd.generation_mwh,
                   gd.windfarm_id, gd.generation_unit_id
            FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gd.source = 'ELEXON'
            AND gu.code = ANY($3)
            AND gd.hour >= $1 AND gd.hour < $2
            AND (gd.metered_mwh = 0 OR gd.metered_mwh IS NULL)
            AND gd.curtailed_mwh IS NOT NULL AND gd.curtailed_mwh > 0
            ORDER BY gd.hour
            LIMIT 50
        """, start_date, end_date, bmu_codes)
    else:
        rows = await conn.fetch("""
            SELECT gd.hour, gu.code, gd.metered_mwh, gd.curtailed_mwh, gd.generation_mwh,
                   gd.windfarm_id, gd.generation_unit_id
            FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gd.source = 'ELEXON'
            AND gd.hour >= $1 AND gd.hour < $2
            AND (gd.metered_mwh = 0 OR gd.metered_mwh IS NULL)
            AND gd.curtailed_mwh IS NOT NULL AND gd.curtailed_mwh > 0
            ORDER BY gd.hour
            LIMIT 50
        """, start_date, end_date)

    print(f"  Found {len(rows)} hours with metered=0 AND curtailment>0 (showing up to 50):")
    if rows:
        print(f"  {'Hour':<22} {'BMU':<14} {'Metered':>10} {'Curtailed':>10} {'Generation':>10}")
        print(f"  {'-'*22} {'-'*14} {'-'*10} {'-'*10} {'-'*10}")
        for r in rows:
            print(f"  {str(r['hour']):<22} {r['code']:<14} {r['metered_mwh'] or 0:>10.2f} {r['curtailed_mwh']:>10.2f} {r['generation_mwh']:>10.2f}")


async def check_missing_hours_global(conn, start_date, end_date, bmu_codes=None):
    """Check for BOAV hours with no corresponding aggregated record."""
    print("\n" + "=" * 80)
    print("4. BOAV HOURS WITH NO AGGREGATED RECORD (missing data)")
    print("=" * 80)

    if bmu_codes:
        rows = await conn.fetch("""
            WITH boav_hours AS (
                SELECT
                    date_trunc('hour', r.period_start) as hour,
                    r.identifier,
                    COUNT(*) as boav_count,
                    SUM(ABS(r.value_extracted)) as total_curtailed
                FROM generation_data_raw r
                WHERE r.source = 'ELEXON'
                AND r.source_type = 'boav_bid'
                AND r.identifier = ANY($3)
                AND r.period_start >= $1 AND r.period_start < $2
                GROUP BY 1, 2
            ),
            agg_hours AS (
                SELECT gd.hour, gu.code
                FROM generation_data gd
                JOIN generation_units gu ON gd.generation_unit_id = gu.id
                WHERE gd.source = 'ELEXON'
                AND gu.code = ANY($3)
                AND gd.hour >= $1 AND gd.hour < $2
            )
            SELECT bh.hour, bh.identifier, bh.boav_count, bh.total_curtailed
            FROM boav_hours bh
            LEFT JOIN agg_hours ah ON bh.hour = ah.hour AND bh.identifier = ah.code
            WHERE ah.hour IS NULL
            ORDER BY bh.hour
            LIMIT 50
        """, start_date, end_date, bmu_codes)
    else:
        rows = await conn.fetch("""
            WITH boav_hours AS (
                SELECT
                    date_trunc('hour', r.period_start) as hour,
                    r.identifier,
                    COUNT(*) as boav_count,
                    SUM(ABS(r.value_extracted)) as total_curtailed
                FROM generation_data_raw r
                WHERE r.source = 'ELEXON'
                AND r.source_type = 'boav_bid'
                AND r.period_start >= $1 AND r.period_start < $2
                GROUP BY 1, 2
            ),
            agg_hours AS (
                SELECT gd.hour, gu.code
                FROM generation_data gd
                JOIN generation_units gu ON gd.generation_unit_id = gu.id
                WHERE gd.source = 'ELEXON'
                AND gd.hour >= $1 AND gd.hour < $2
            )
            SELECT bh.hour, bh.identifier, bh.boav_count, bh.total_curtailed
            FROM boav_hours bh
            LEFT JOIN agg_hours ah ON bh.hour = ah.hour AND bh.identifier = ah.code
            WHERE ah.hour IS NULL
            ORDER BY bh.hour
            LIMIT 50
        """, start_date, end_date)

    print(f"  Found {len(rows)} BOAV hour/unit combos with NO aggregated record (showing up to 50):")
    if rows:
        # Group by hour of day to see pattern
        hour_counts = defaultdict(int)
        for r in rows:
            hour_counts[r['hour'].hour] += 1

        print(f"\n  Distribution by UTC hour:")
        for h in sorted(hour_counts.keys()):
            print(f"    {h:02d}:00 UTC: {hour_counts[h]} missing")

        print(f"\n  {'Hour':<22} {'BMU':<14} {'BOAV Records':>12} {'Curtailed MWh':>14}")
        print(f"  {'-'*22} {'-'*14} {'-'*12} {'-'*14}")
        for r in rows[:20]:
            print(f"  {str(r['hour']):<22} {r['identifier']:<14} {r['boav_count']:>12} {float(r['total_curtailed']):>14.2f}")


async def check_raw_data_gaps(conn, start_date, end_date, bmu_codes):
    """Check for gaps in raw B1610 data for specific BMUs."""
    print("\n" + "=" * 80)
    print("5. RAW B1610 DATA GAPS (per BMU)")
    print("=" * 80)

    for code in bmu_codes:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT date_trunc('hour', period_start)) as distinct_hours,
                MIN(period_start) as earliest,
                MAX(period_start) as latest
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND identifier = $1
            AND source_type NOT IN ('boav_bid', 'boav_offer')
            AND period_start >= $2 AND period_start < $3
        """, code, start_date, end_date)

        total_hours_expected = int((end_date - start_date).total_seconds() / 3600)
        records_per_hour = 2  # Two 30-min settlement periods per hour
        expected_records = total_hours_expected * records_per_hour

        print(f"\n  {code}:")
        print(f"    Total B1610 records: {row['total_records']:,} (expected ~{expected_records:,})")
        print(f"    Distinct hours: {row['distinct_hours']:,} (expected ~{total_hours_expected:,})")
        if row['earliest']:
            print(f"    Range: {row['earliest']} to {row['latest']}")

        # Check source_type distribution
        types = await conn.fetch("""
            SELECT source_type, COUNT(*) as cnt
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND identifier = $1
            AND source_type NOT IN ('boav_bid', 'boav_offer')
            AND period_start >= $2 AND period_start < $3
            GROUP BY source_type
        """, code, start_date, end_date)
        type_strs = [f"{t['source_type']}={t['cnt']}" for t in types]
        print(f"    Source types: {', '.join(type_strs)}")


async def check_farr_test_days(conn):
    """Run specific checks on the 5 Farr test days from CSV comparison."""
    print("\n" + "=" * 80)
    print("6. FARR TEST DAY VERIFICATION")
    print("=" * 80)

    farr_codes = ['T_FARR-1', 'T_FARR-2']

    for tc in FARR_TEST_CASES:
        date = datetime.strptime(tc['date'], '%Y-%m-%d').replace(tzinfo=UTC)
        hour_start = date.replace(hour=tc['hour'])
        hour_end = hour_start + timedelta(hours=1)

        print(f"\n  {tc['date']} {tc['hour']:02d}:00 UTC — {tc['bug']}")
        print(f"  API sum expected: {tc['api_sum']:.2f} MWh")

        # Check raw B1610
        raw = await conn.fetch("""
            SELECT identifier, period_start, source_type, value_extracted,
                   data->>'settlement_date' as sd,
                   data->>'settlement_period' as sp,
                   data->>'import_export_ind' as ie,
                   data->>'metered_volume' as mv
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND identifier = ANY($1)
            AND source_type NOT IN ('boav_bid', 'boav_offer')
            AND period_start >= $2 AND period_start < $3
            ORDER BY identifier, period_start
        """, farr_codes, hour_start, hour_end)

        if raw:
            print(f"  Raw B1610 records: {len(raw)}")
            for r in raw:
                print(f"    {r['identifier']} | {r['period_start']} | {r['source_type']} | "
                      f"val={r['value_extracted']} | mv={r['mv']} | ie={r['ie']} | SD={r['sd']} SP={r['sp']}")
        else:
            print(f"  Raw B1610 records: NONE — data missing from raw table!")

        # Check raw BOAV
        boav = await conn.fetch("""
            SELECT identifier, period_start, value_extracted
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND identifier = ANY($1)
            AND source_type = 'boav_bid'
            AND period_start >= $2 AND period_start < $3
            ORDER BY identifier, period_start
        """, farr_codes, hour_start, hour_end)

        if boav:
            print(f"  BOAV records: {len(boav)}")
            for b in boav:
                print(f"    {b['identifier']} | {b['period_start']} | val={b['value_extracted']}")
        else:
            print(f"  BOAV records: none")

        # Check aggregated data
        agg = await conn.fetch("""
            SELECT gd.hour, gu.code, gd.metered_mwh, gd.curtailed_mwh, gd.generation_mwh,
                   gd.windfarm_id, gd.generation_unit_id
            FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gd.source = 'ELEXON'
            AND gu.code = ANY($1)
            AND gd.hour >= $2 AND gd.hour < $3
            ORDER BY gu.code
        """, farr_codes, hour_start, hour_end)

        if agg:
            print(f"  Aggregated records: {len(agg)}")
            total_metered = 0
            for a in agg:
                metered = float(a['metered_mwh']) if a['metered_mwh'] else 0
                total_metered += metered
                print(f"    {a['code']} | metered={a['metered_mwh']} | curtailed={a['curtailed_mwh']} | "
                      f"gen={a['generation_mwh']} | wf_id={a['windfarm_id']} | unit_id={a['generation_unit_id']}")
            print(f"  Total metered (both units): {total_metered:.2f} (expected: {tc['api_sum']:.2f}) "
                  f"{'OK' if abs(total_metered - tc['api_sum']) < 0.5 else 'MISMATCH!'}")
        else:
            print(f"  Aggregated records: NONE — completely missing!")


async def global_summary(conn, start_date, end_date):
    """Overall summary of ELEXON data health."""
    print("\n" + "=" * 80)
    print("7. GLOBAL ELEXON DATA SUMMARY")
    print("=" * 80)

    # Count windfarms affected by missing metered data
    rows = await conn.fetch("""
        SELECT w.name, w.id, COUNT(*) as affected_hours
        FROM generation_data gd
        JOIN generation_units gu ON gd.generation_unit_id = gu.id
        JOIN windfarms w ON gu.windfarm_id = w.id
        WHERE gd.source = 'ELEXON'
        AND gd.hour >= $1 AND gd.hour < $2
        AND (gd.metered_mwh = 0 OR gd.metered_mwh IS NULL)
        AND gd.curtailed_mwh IS NOT NULL AND gd.curtailed_mwh > 0
        GROUP BY w.name, w.id
        ORDER BY affected_hours DESC
    """, start_date, end_date)

    print(f"\n  Windfarms with metered=0 + curtailment>0 ({start_date.date()} to {end_date.date()}):")
    if rows:
        print(f"  {'Windfarm':<30} {'ID':>6} {'Affected Hours':>14}")
        print(f"  {'-'*30} {'-'*6} {'-'*14}")
        total = 0
        for r in rows:
            print(f"  {r['name']:<30} {r['id']:>6} {r['affected_hours']:>14}")
            total += r['affected_hours']
        print(f"  {'TOTAL':<30} {'':>6} {total:>14}")
    else:
        print(f"  None found (good!)")

    # Count total missing hours (BOAV exists but no agg record)
    row = await conn.fetchrow("""
        WITH boav_hours AS (
            SELECT
                date_trunc('hour', r.period_start) as hour,
                r.identifier
            FROM generation_data_raw r
            WHERE r.source = 'ELEXON'
            AND r.source_type = 'boav_bid'
            AND r.period_start >= $1 AND r.period_start < $2
            GROUP BY 1, 2
        ),
        agg_hours AS (
            SELECT gd.hour, gu.code
            FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gd.source = 'ELEXON'
            AND gd.hour >= $1 AND gd.hour < $2
        )
        SELECT COUNT(*) as missing
        FROM boav_hours bh
        LEFT JOIN agg_hours ah ON bh.hour = ah.hour AND bh.identifier = ah.code
        WHERE ah.hour IS NULL
    """, start_date, end_date)

    print(f"\n  Total BOAV hour/unit combos missing from aggregated data: {row['missing']:,}")


async def main():
    parser = argparse.ArgumentParser(description='Diagnose ELEXON 2025 aggregation bugs')
    parser.add_argument('--start', type=str, default='2025-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default='2025-12-31', help='End date (YYYY-MM-DD)')
    parser.add_argument('--bmu', nargs='+', help='Specific BMU codes to check')
    parser.add_argument('--farr-test-days', action='store_true', help='Run only Farr test day checks')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=UTC)
    end_date = datetime.strptime(args.end, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=UTC)
    end_date_exclusive = (datetime.strptime(args.end, '%Y-%m-%d') + timedelta(days=1)).replace(tzinfo=UTC)

    print("=" * 80)
    print("ELEXON 2025 AGGREGATION DIAGNOSTIC")
    print("=" * 80)
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    if args.bmu:
        print(f"BMU filter: {', '.join(args.bmu)}")

    conn = await get_connection()

    try:
        if args.farr_test_days:
            await check_unique_constraint(conn)
            await check_farr_test_days(conn)
            return

        # 1. Check unique constraint
        await check_unique_constraint(conn)

        # 2. Get all ELEXON units
        units = await get_all_elexon_units(conn, args.bmu)
        print(f"\n  Found {len(units)} ELEXON generation units")
        # Group by windfarm
        wf_groups = defaultdict(list)
        for u in units:
            wf_groups[u['windfarm_name'] or 'Unknown'].append(u['code'])
        print(f"  Across {len(wf_groups)} windfarms:")
        for wf, codes in sorted(wf_groups.items()):
            print(f"    {wf}: {', '.join(codes)}")

        bmu_codes = args.bmu or [u['code'] for u in units]

        # 3. Check NULL windfarm_ids
        await check_null_windfarm_ids(conn, start_date, end_date_exclusive)

        # 4. Check metered=0 with raw data
        await check_metered_zero_with_raw_data(conn, start_date, end_date_exclusive, bmu_codes)

        # 5. Check missing BOAV hours
        await check_missing_hours_global(conn, start_date, end_date_exclusive, bmu_codes)

        # 6. Check raw data gaps (only for specified BMUs or top affected)
        check_codes = args.bmu or ['T_FARR-1', 'T_FARR-2']  # Default to Farr if no filter
        await check_raw_data_gaps(conn, start_date, end_date_exclusive, check_codes)

        # 7. Farr test days (always run)
        await check_farr_test_days(conn)

        # 8. Global summary
        await global_summary(conn, start_date, end_date_exclusive)

    finally:
        await conn.close()

    print("\n" + "=" * 80)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
