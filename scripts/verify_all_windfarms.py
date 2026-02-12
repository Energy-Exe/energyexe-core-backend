#!/usr/bin/env python3
"""
Comprehensive per-windfarm verification of ELEXON 2025 aggregated data against raw source data.

For each ELEXON windfarm, this script:
1. Compares aggregated metered_mwh against raw B1610 settlement period sums
2. Checks for missing hours (raw data exists but no aggregated record)
3. Checks for orphaned hours (aggregated record but no raw data)
4. Verifies curtailed_mwh matches BOAV bid data
5. Checks for NULL windfarm_id or generation_unit_id
6. Reports discrepancies per windfarm

Usage:
    poetry run python scripts/verify_all_windfarms.py
    poetry run python scripts/verify_all_windfarms.py --month 6    # June only
    poetry run python scripts/verify_all_windfarms.py --windfarm 123  # Single windfarm
"""
import asyncio
import asyncpg
import sys
import argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from decimal import Decimal

sys.path.append(".")
from app.core.config import get_settings


async def get_all_elexon_windfarms(conn):
    """Get all windfarms that have ELEXON generation units."""
    return await conn.fetch("""
        SELECT DISTINCT w.id, w.name, w.nameplate_capacity_mw,
               array_agg(gu.code ORDER BY gu.code) as bmu_codes,
               array_agg(gu.id ORDER BY gu.code) as unit_ids,
               COUNT(gu.id) as unit_count
        FROM windfarms w
        JOIN generation_units gu ON gu.windfarm_id = w.id
        WHERE gu.source = 'ELEXON'
        GROUP BY w.id, w.name, w.nameplate_capacity_mw
        ORDER BY w.name
    """)


async def verify_windfarm(conn, windfarm, start_dt, end_dt, verbose=False):
    """Verify a single windfarm's aggregated data against raw source data.

    Returns a dict with verification results.
    """
    wf_id = windfarm['id']
    wf_name = windfarm['name']
    bmu_codes = list(windfarm['bmu_codes'])
    unit_ids = list(windfarm['unit_ids'])
    unit_count = windfarm['unit_count']

    issues = []
    stats = {
        'name': wf_name,
        'bmu_codes': bmu_codes,
        'unit_count': unit_count,
        'aggregated_hours': 0,
        'raw_hours': 0,
        'missing_hours': 0,
        'orphaned_hours': 0,
        'metered_mismatches': 0,
        'curtailed_mismatches': 0,
        'null_windfarm_ids': 0,
        'null_unit_ids': 0,
        'max_metered_diff': 0.0,
        'max_curtailed_diff': 0.0,
        'total_agg_metered': 0.0,
        'total_raw_metered': 0.0,
        'status': 'PASS',
    }

    # 1. Count aggregated records and check for NULLs
    agg_stats = await conn.fetch("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE windfarm_id IS NULL) as null_wf,
            COUNT(*) FILTER (WHERE generation_unit_id IS NULL) as null_uid,
            SUM(metered_mwh) as total_metered,
            SUM(curtailed_mwh) as total_curtailed
        FROM generation_data
        WHERE source = 'ELEXON'
        AND generation_unit_id = ANY($1::int[])
        AND hour >= $2 AND hour < $3
    """, unit_ids, start_dt, end_dt)

    agg = agg_stats[0]
    stats['aggregated_hours'] = agg['total']
    stats['null_windfarm_ids'] = agg['null_wf']
    stats['null_unit_ids'] = agg['null_uid']
    stats['total_agg_metered'] = float(agg['total_metered'] or 0)

    if agg['null_wf'] > 0:
        issues.append(f"NULL windfarm_id: {agg['null_wf']} records")
    if agg['null_uid'] > 0:
        issues.append(f"NULL generation_unit_id: {agg['null_uid']} records")

    # 2. Get raw B1610 hourly sums (using period_start floored to hour)
    # Group raw data by hour and identifier to get expected metered totals
    raw_hourly = await conn.fetch("""
        SELECT
            date_trunc('hour', period_start) as hour,
            identifier,
            SUM(value_extracted) as raw_metered,
            COUNT(*) as sp_count
        FROM generation_data_raw
        WHERE source = 'ELEXON'
        AND source_type IN ('api', 'csv')
        AND identifier = ANY($1)
        AND period_start >= $2 AND period_start < $3
        GROUP BY 1, 2
        ORDER BY 1, 2
    """, bmu_codes, start_dt, end_dt)

    # Build lookup: (hour, identifier) -> raw_metered
    raw_lookup = {}
    for r in raw_hourly:
        key = (r['hour'], r['identifier'])
        raw_lookup[key] = {
            'metered': float(r['raw_metered'] or 0),
            'sp_count': r['sp_count'],
        }

    # Count unique raw hours (across all units)
    raw_hours_set = set(r['hour'] for r in raw_hourly)
    stats['raw_hours'] = len(raw_hours_set)

    # 3. Get BOAV curtailment hourly sums
    boav_hourly = await conn.fetch("""
        SELECT
            date_trunc('hour', period_start) as hour,
            identifier,
            SUM(ABS(value_extracted)) as raw_curtailed,
            COUNT(*) as bid_count
        FROM generation_data_raw
        WHERE source = 'ELEXON'
        AND source_type = 'boav_bid'
        AND identifier = ANY($1)
        AND period_start >= $2 AND period_start < $3
        GROUP BY 1, 2
    """, bmu_codes, start_dt, end_dt)

    boav_lookup = {}
    for r in boav_hourly:
        key = (r['hour'], r['identifier'])
        boav_lookup[key] = float(r['raw_curtailed'] or 0)

    # 4. Get all aggregated records for comparison
    agg_records = await conn.fetch("""
        SELECT
            gd.hour, gu.code as identifier,
            gd.metered_mwh, gd.curtailed_mwh, gd.generation_mwh,
            gd.windfarm_id, gd.generation_unit_id, gd.raw_data_ids
        FROM generation_data gd
        JOIN generation_units gu ON gu.id = gd.generation_unit_id
        WHERE gd.source = 'ELEXON'
        AND gd.generation_unit_id = ANY($1::int[])
        AND gd.hour >= $2 AND gd.hour < $3
        ORDER BY gd.hour, gu.code
    """, unit_ids, start_dt, end_dt)

    agg_lookup = {}
    for r in agg_records:
        key = (r['hour'], r['identifier'])
        agg_lookup[key] = {
            'metered': float(r['metered_mwh'] or 0),
            'curtailed': float(r['curtailed_mwh'] or 0),
            'generation': float(r['generation_mwh'] or 0),
        }

    # 5. Compare: check for missing hours (raw exists, no aggregated)
    missing_count = 0
    for key in raw_lookup:
        if key not in agg_lookup:
            missing_count += 1
            if verbose and missing_count <= 5:
                hour, ident = key
                issues.append(f"Missing agg: {ident} {hour} (raw metered={raw_lookup[key]['metered']:.2f})")

    stats['missing_hours'] = missing_count
    if missing_count > 0:
        issues.append(f"Missing aggregated records: {missing_count} hour/unit combos have raw data but no aggregated record")

    # 6. Compare: check for orphaned hours (aggregated exists, no raw B1610)
    # Note: BOAV-only records are valid (fully curtailed hours)
    orphaned_count = 0
    for key in agg_lookup:
        if key not in raw_lookup and key not in boav_lookup:
            orphaned_count += 1
            if verbose and orphaned_count <= 5:
                hour, ident = key
                issues.append(f"Orphaned agg: {ident} {hour} (metered={agg_lookup[key]['metered']:.2f}, no raw data)")

    stats['orphaned_hours'] = orphaned_count
    if orphaned_count > 0:
        issues.append(f"Orphaned aggregated records: {orphaned_count} hour/unit combos have no raw data")

    # 7. Compare metered values where both exist
    metered_mismatches = 0
    max_diff = 0.0
    total_raw_metered = 0.0
    for key in raw_lookup:
        total_raw_metered += raw_lookup[key]['metered']
        if key in agg_lookup:
            raw_m = raw_lookup[key]['metered']
            agg_m = agg_lookup[key]['metered']
            diff = abs(raw_m - agg_m)
            if diff > 0.01:  # Allow 0.01 MWh rounding tolerance
                metered_mismatches += 1
                if diff > max_diff:
                    max_diff = diff
                if verbose and metered_mismatches <= 3:
                    hour, ident = key
                    issues.append(f"Metered mismatch: {ident} {hour} raw={raw_m:.3f} agg={agg_m:.3f} diff={diff:.3f}")

    stats['metered_mismatches'] = metered_mismatches
    stats['max_metered_diff'] = max_diff
    stats['total_raw_metered'] = total_raw_metered
    if metered_mismatches > 0:
        issues.append(f"Metered mismatches: {metered_mismatches} (max diff={max_diff:.3f} MWh)")

    # 8. Compare curtailed values where both exist
    curtailed_mismatches = 0
    max_curt_diff = 0.0
    for key in boav_lookup:
        if key in agg_lookup:
            raw_c = boav_lookup[key]
            agg_c = agg_lookup[key]['curtailed']
            diff = abs(raw_c - agg_c)
            if diff > 0.01:
                curtailed_mismatches += 1
                if diff > max_curt_diff:
                    max_curt_diff = diff
                if verbose and curtailed_mismatches <= 3:
                    hour, ident = key
                    issues.append(f"Curtailed mismatch: {ident} {hour} raw={raw_c:.3f} agg={agg_c:.3f}")

    stats['curtailed_mismatches'] = curtailed_mismatches
    stats['max_curtailed_diff'] = max_curt_diff
    if curtailed_mismatches > 0:
        issues.append(f"Curtailed mismatches: {curtailed_mismatches} (max diff={max_curt_diff:.3f} MWh)")

    # Determine status
    if stats['null_windfarm_ids'] > 0 or stats['null_unit_ids'] > 0:
        stats['status'] = 'FAIL'
    elif stats['missing_hours'] > stats['aggregated_hours'] * 0.01:  # >1% missing
        stats['status'] = 'WARN'
    elif stats['metered_mismatches'] > stats['aggregated_hours'] * 0.01:  # >1% mismatched
        stats['status'] = 'WARN'
    elif stats['missing_hours'] > 0 or stats['metered_mismatches'] > 0:
        stats['status'] = 'MINOR'

    stats['issues'] = issues
    return stats


async def main():
    parser = argparse.ArgumentParser(description='Verify ELEXON windfarm data')
    parser.add_argument('--month', type=int, help='Verify specific month (1-12)')
    parser.add_argument('--windfarm', type=int, help='Verify specific windfarm ID')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed issues')
    parser.add_argument('--start', type=str, default='2025-01-01', help='Start date YYYY-MM-DD')
    parser.add_argument('--end', type=str, default='2026-01-01', help='End date YYYY-MM-DD')
    args = parser.parse_args()

    settings = get_settings()
    dsn = str(settings.DATABASE_URL).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

    # Date range
    if args.month:
        start_dt = datetime(2025, args.month, 1, tzinfo=timezone.utc)
        if args.month == 12:
            end_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        else:
            end_dt = datetime(2025, args.month + 1, 1, tzinfo=timezone.utc)
    else:
        y1, m1, d1 = map(int, args.start.split('-'))
        y2, m2, d2 = map(int, args.end.split('-'))
        start_dt = datetime(y1, m1, d1, tzinfo=timezone.utc)
        end_dt = datetime(y2, m2, d2, tzinfo=timezone.utc)

    print("=" * 80)
    print(f"ELEXON WINDFARM VERIFICATION: {start_dt.date()} to {end_dt.date()}")
    print("=" * 80)

    # Get all windfarms
    windfarms = await get_all_elexon_windfarms(conn)

    if args.windfarm:
        windfarms = [w for w in windfarms if w['id'] == args.windfarm]
        if not windfarms:
            print(f"No ELEXON windfarm found with ID {args.windfarm}")
            await conn.close()
            return

    print(f"Verifying {len(windfarms)} ELEXON windfarms...\n")

    # Counters
    pass_count = 0
    minor_count = 0
    warn_count = 0
    fail_count = 0
    all_results = []

    for i, wf in enumerate(windfarms):
        try:
            result = await verify_windfarm(conn, wf, start_dt, end_dt, verbose=args.verbose)
            all_results.append(result)

            status = result['status']
            icon = {'PASS': '+', 'MINOR': '~', 'WARN': '!', 'FAIL': 'X'}[status]

            if status == 'PASS':
                pass_count += 1
            elif status == 'MINOR':
                minor_count += 1
            elif status == 'WARN':
                warn_count += 1
            else:
                fail_count += 1

            # Print compact line for each windfarm
            codes_str = ','.join(result['bmu_codes'][:3])
            if len(result['bmu_codes']) > 3:
                codes_str += f"+{len(result['bmu_codes'])-3}"

            line = (
                f"[{icon}] {result['name'][:35]:<35} "
                f"({codes_str:<25}) "
                f"agg={result['aggregated_hours']:>6,} "
                f"raw={result['raw_hours']:>6,} "
                f"miss={result['missing_hours']:>4} "
                f"mmatch={result['metered_mismatches']:>4} "
                f"{status}"
            )
            print(line)

            # Print issues for non-PASS windfarms
            if status != 'PASS' and result['issues']:
                for issue in result['issues']:
                    print(f"     -> {issue}")

        except Exception as e:
            print(f"[E] {wf['name']}: ERROR - {e}")
            fail_count += 1

        # Progress indicator every 50 windfarms
        if (i + 1) % 50 == 0:
            print(f"  ... processed {i+1}/{len(windfarms)} windfarms ...")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total windfarms:    {len(windfarms)}")
    print(f"  PASS:             {pass_count}")
    print(f"  MINOR issues:     {minor_count}")
    print(f"  WARN:             {warn_count}")
    print(f"  FAIL:             {fail_count}")

    # Aggregate stats
    total_agg = sum(r['aggregated_hours'] for r in all_results)
    total_raw = sum(r['raw_hours'] for r in all_results)
    total_missing = sum(r['missing_hours'] for r in all_results)
    total_orphaned = sum(r['orphaned_hours'] for r in all_results)
    total_mm = sum(r['metered_mismatches'] for r in all_results)
    total_cm = sum(r['curtailed_mismatches'] for r in all_results)
    total_null_wf = sum(r['null_windfarm_ids'] for r in all_results)
    total_null_uid = sum(r['null_unit_ids'] for r in all_results)

    print(f"\nAggregated records: {total_agg:,}")
    print(f"Raw B1610 hours:    {total_raw:,}")
    print(f"Missing agg hours:  {total_missing:,}")
    print(f"Orphaned agg hours: {total_orphaned:,}")
    print(f"Metered mismatches: {total_mm:,}")
    print(f"Curtail mismatches: {total_cm:,}")
    print(f"NULL windfarm_ids:  {total_null_wf:,}")
    print(f"NULL unit_ids:      {total_null_uid:,}")

    # List windfarms with issues
    problem_wfs = [r for r in all_results if r['status'] != 'PASS']
    if problem_wfs:
        print(f"\n--- WINDFARMS WITH ISSUES ({len(problem_wfs)}) ---")
        for r in sorted(problem_wfs, key=lambda x: x['status']):
            print(f"\n  [{r['status']}] {r['name']} ({','.join(r['bmu_codes'][:3])})")
            print(f"       agg={r['aggregated_hours']:,} raw={r['raw_hours']:,} missing={r['missing_hours']} mismatches={r['metered_mismatches']}")
            for issue in r['issues']:
                print(f"       - {issue}")

    total_agg_metered = sum(r['total_agg_metered'] for r in all_results)
    total_raw_metered = sum(r['total_raw_metered'] for r in all_results)
    print(f"\nTotal agg metered:  {total_agg_metered:,.2f} MWh")
    print(f"Total raw metered:  {total_raw_metered:,.2f} MWh")
    diff_pct = abs(total_agg_metered - total_raw_metered) / max(abs(total_raw_metered), 1) * 100
    print(f"Difference:         {abs(total_agg_metered - total_raw_metered):,.2f} MWh ({diff_pct:.4f}%)")

    await conn.close()
    print("\n" + "=" * 80)
    print("VERIFICATION COMPLETE")
    print("=" * 80)


if __name__ == '__main__':
    asyncio.run(main())
