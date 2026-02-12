#!/usr/bin/env python3
"""Investigate metered mismatches - are they all on BST clock-change days?"""
import asyncio, asyncpg, sys
from datetime import datetime, timezone, timedelta
from collections import Counter

sys.path.append(".")
from app.core.config import get_settings

# UK BST dates in 2025
# Spring forward: March 30 (clocks go from 1am to 2am)
# Fall back: October 26 (clocks go from 2am to 1am)
BST_DATES = {
    datetime(2025, 3, 30).date(),  # Spring forward
    datetime(2025, 10, 26).date(), # Fall back
}

async def investigate():
    settings = get_settings()
    dsn = str(settings.DATABASE_URL).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

    # Get all ELEXON units
    units = await conn.fetch("""
        SELECT gu.id, gu.code, gu.windfarm_id, w.name as wf_name
        FROM generation_units gu
        JOIN windfarms w ON w.id = gu.windfarm_id
        WHERE gu.source = 'ELEXON'
        ORDER BY w.name, gu.code
    """)

    unit_ids = [u['id'] for u in units]
    code_map = {u['id']: u['code'] for u in units}

    start_dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)

    # Find hours where aggregated metered doesn't match simple sum of raw data
    # Check what dates the mismatches fall on
    print("=== CHECKING MISMATCH DATES ===")
    print("Getting aggregated data with their raw_data_ids...")

    # Sample approach: check a specific windfarm with mismatches - Farr (13 mismatches)
    farr_units = await conn.fetch("""
        SELECT id, code FROM generation_units
        WHERE code IN ('T_FARR-1', 'T_FARR-2') AND source = 'ELEXON'
    """)
    farr_ids = [u['id'] for u in farr_units]

    print(f"\n--- FARR ({[u['code'] for u in farr_units]}) ---")

    # Get aggregated records
    agg = await conn.fetch("""
        SELECT gd.hour, gu.code, gd.metered_mwh, gd.raw_data_ids
        FROM generation_data gd
        JOIN generation_units gu ON gu.id = gd.generation_unit_id
        WHERE gd.source = 'ELEXON'
        AND gd.generation_unit_id = ANY($1::int[])
        AND gd.hour >= $2 AND gd.hour < $3
        ORDER BY gd.hour, gu.code
    """, farr_ids, start_dt, end_dt)

    # Get raw hourly sums (using simple date_trunc)
    raw = await conn.fetch("""
        SELECT date_trunc('hour', period_start) as hour, identifier,
               SUM(value_extracted) as raw_sum
        FROM generation_data_raw
        WHERE source = 'ELEXON'
        AND source_type IN ('api', 'csv')
        AND identifier IN ('T_FARR-1', 'T_FARR-2')
        AND period_start >= $1 AND period_start < $2
        GROUP BY 1, 2
    """, start_dt, end_dt)

    raw_lookup = {}
    for r in raw:
        key = (r['hour'], r['identifier'])
        raw_lookup[key] = float(r['raw_sum'] or 0)

    mismatch_dates = []
    for r in agg:
        key = (r['hour'], r['code'])
        if key in raw_lookup:
            diff = abs(float(r['metered_mwh'] or 0) - raw_lookup[key])
            if diff > 0.01:
                d = r['hour'].date()
                is_bst = d in BST_DATES
                mismatch_dates.append(d)
                print(f"  Mismatch: {r['hour']} {r['code']} agg={float(r['metered_mwh']):.3f} raw={raw_lookup[key]:.3f} diff={diff:.3f} BST_DAY={'YES' if is_bst else 'NO'}")

    if mismatch_dates:
        date_counts = Counter(mismatch_dates)
        print(f"\n  Mismatch dates: {dict(date_counts)}")
        bst_count = sum(1 for d in mismatch_dates if d in BST_DATES)
        print(f"  On BST days: {bst_count}/{len(mismatch_dates)}")

    # Now check a few more windfarms with larger mismatches
    print("\n--- CHECKING ALL MISMATCH DATES ACROSS ALL WINDFARMS ---")

    # Get all unit codes
    all_codes = [u['code'] for u in units]
    all_unit_ids = [u['id'] for u in units]

    # Compare aggregated vs raw for ALL units, find mismatches
    # Use a sampling approach: check specific hours around BST dates
    bst_hours = []
    for bst_date in BST_DATES:
        for h in range(24):
            bst_hours.append(datetime(bst_date.year, bst_date.month, bst_date.day, h, tzinfo=timezone.utc))
        # Also check day before and after
        prev = bst_date - timedelta(days=1)
        next_d = bst_date + timedelta(days=1)
        for h in range(24):
            bst_hours.append(datetime(prev.year, prev.month, prev.day, h, tzinfo=timezone.utc))
            bst_hours.append(datetime(next_d.year, next_d.month, next_d.day, h, tzinfo=timezone.utc))

    # Count mismatches on BST vs non-BST dates globally
    # Check a random sample of non-BST days
    non_bst_sample = [
        datetime(2025, 2, 15, tzinfo=timezone.utc),
        datetime(2025, 5, 15, tzinfo=timezone.utc),
        datetime(2025, 7, 15, tzinfo=timezone.utc),
        datetime(2025, 9, 15, tzinfo=timezone.utc),
    ]

    for sample_date in non_bst_sample:
        sample_end = sample_date + timedelta(days=1)
        agg_sample = await conn.fetch("""
            SELECT gd.hour, gu.code, gd.metered_mwh
            FROM generation_data gd
            JOIN generation_units gu ON gu.id = gd.generation_unit_id
            WHERE gd.source = 'ELEXON'
            AND gd.hour >= $1 AND gd.hour < $2
            ORDER BY gd.hour, gu.code
        """, sample_date, sample_end)

        raw_sample = await conn.fetch("""
            SELECT date_trunc('hour', period_start) as hour, identifier,
                   SUM(value_extracted) as raw_sum
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND source_type IN ('api', 'csv')
            AND period_start >= $1 AND period_start < $2
            GROUP BY 1, 2
        """, sample_date, sample_end)

        raw_lookup2 = {}
        for r in raw_sample:
            raw_lookup2[(r['hour'], r['identifier'])] = float(r['raw_sum'] or 0)

        mm_count = 0
        for r in agg_sample:
            key = (r['hour'], r['code'])
            if key in raw_lookup2:
                diff = abs(float(r['metered_mwh'] or 0) - raw_lookup2[key])
                if diff > 0.01:
                    mm_count += 1
        print(f"  {sample_date.date()} (non-BST): {mm_count} mismatches out of {len(agg_sample)} records")

    # Now check BST days
    for bst_date in sorted(BST_DATES):
        bst_start = datetime(bst_date.year, bst_date.month, bst_date.day, tzinfo=timezone.utc)
        bst_end = bst_start + timedelta(days=1)

        agg_bst = await conn.fetch("""
            SELECT gd.hour, gu.code, gd.metered_mwh
            FROM generation_data gd
            JOIN generation_units gu ON gu.id = gd.generation_unit_id
            WHERE gd.source = 'ELEXON'
            AND gd.hour >= $1 AND gd.hour < $2
            ORDER BY gd.hour, gu.code
        """, bst_start, bst_end)

        raw_bst = await conn.fetch("""
            SELECT date_trunc('hour', period_start) as hour, identifier,
                   SUM(value_extracted) as raw_sum
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND source_type IN ('api', 'csv')
            AND period_start >= $1 AND period_start < $2
            GROUP BY 1, 2
        """, bst_start, bst_end)

        raw_lookup3 = {}
        for r in raw_bst:
            raw_lookup3[(r['hour'], r['identifier'])] = float(r['raw_sum'] or 0)

        mm_count = 0
        for r in agg_bst:
            key = (r['hour'], r['code'])
            if key in raw_lookup3:
                diff = abs(float(r['metered_mwh'] or 0) - raw_lookup3[key])
                if diff > 0.01:
                    mm_count += 1
        print(f"  {bst_date} (BST DAY): {mm_count} mismatches out of {len(agg_bst)} records")

    # Also check the 24 missing hours for WARN windfarms
    print("\n--- INVESTIGATING MISSING HOURS (WARN WINDFARMS) ---")
    warn_codes = {
        'Camster II': ['E_CMSTW-2'],
        'Crystal Rig 4': ['T_CRYRW-4'],
        'Douglas West Ext': ['T_DWEXW-1'],
        'Kilgallioch Ext': ['T_KLGLW-2'],
    }

    for name, codes in warn_codes.items():
        wf_units = await conn.fetch("""
            SELECT id, code FROM generation_units
            WHERE code = ANY($1) AND source = 'ELEXON'
        """, codes)
        wf_ids = [u['id'] for u in wf_units]

        # Find raw hours that don't have aggregated records
        raw_hours = await conn.fetch("""
            SELECT date_trunc('hour', period_start) as hour, identifier
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND source_type IN ('api', 'csv')
            AND identifier = ANY($1)
            AND period_start >= $2 AND period_start < $3
            GROUP BY 1, 2
        """, codes, start_dt, end_dt)

        agg_hours = await conn.fetch("""
            SELECT hour, gu.code
            FROM generation_data gd
            JOIN generation_units gu ON gu.id = gd.generation_unit_id
            WHERE gd.source = 'ELEXON'
            AND gd.generation_unit_id = ANY($1::int[])
            AND gd.hour >= $2 AND gd.hour < $3
        """, wf_ids, start_dt, end_dt)

        raw_set = set((r['hour'], r['identifier']) for r in raw_hours)
        agg_set = set((r['hour'], r['code']) for r in agg_hours)
        missing = raw_set - agg_set

        if missing:
            dates = sorted(set(h.date() for h, _ in missing))
            print(f"  {name}: {len(missing)} missing hours on dates: {dates}")
            # Show which hours
            for h, c in sorted(missing)[:5]:
                print(f"    {h} {c}")
            if len(missing) > 5:
                print(f"    ... and {len(missing)-5} more")

    await conn.close()
    print("\nDone!")

asyncio.run(investigate())
