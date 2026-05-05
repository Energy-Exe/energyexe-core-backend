"""Investigate why back-calculating Raggovidda capacity from monthly CF + gen
gives values that fluctuate around 45 MW pre-2022 instead of exactly 45.

Hypotheses to test:
1. Multiple rows per hour (different sources, e.g. NVE + ENTSOE)
2. Some rows have capacity_mw != 45
3. Some hours missing → SUM(gen)/AVG(cf)/N_hours mismatch with calendar hours
4. AVG(capacity_factor) is not equivalent to SUM(gen)/SUM(capacity_mw*hours_in_row)
"""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory

async_session_maker = get_session_factory()

WINDFARM_ID = 7206


async def main():
    async with async_session_maker() as db:
        print("=" * 80)
        print("RAGGOVIDDA CAPACITY BACK-CALCULATION INVESTIGATION")
        print("=" * 80)

        # 1. What sources have data for windfarm 7206?
        print("\n[1] Sources contributing rows pre-2022 (per year):")
        rs = await db.execute(text("""
            SELECT EXTRACT(YEAR FROM hour)::int AS yr, source, COUNT(*) AS rows,
                   COUNT(DISTINCT generation_unit_id) AS units
            FROM generation_data
            WHERE windfarm_id = :wf AND hour < '2022-01-01'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """), {"wf": WINDFARM_ID})
        print(f"  {'year':<6}{'source':<14}{'rows':>10}{'units':>8}")
        for r in rs:
            print(f"  {r.yr:<6}{r.source:<14}{r.rows:>10}{r.units:>8}")

        # 2. Distinct capacity_mw values per source pre-2022
        print("\n[2] Distinct capacity_mw values per source pre-2022:")
        rs = await db.execute(text("""
            SELECT source, capacity_mw, COUNT(*) AS rows
            FROM generation_data
            WHERE windfarm_id = :wf AND hour < '2022-01-01'
            GROUP BY source, capacity_mw
            ORDER BY source, capacity_mw
        """), {"wf": WINDFARM_ID})
        print(f"  {'source':<14}{'capacity_mw':>14}{'rows':>10}")
        for r in rs:
            print(f"  {r.source:<14}{r.capacity_mw:>14}{r.rows:>10}")

        # 3. Hours that have rows from multiple sources (double-counting risk)
        print("\n[3] Hours with multiple source rows pre-2022:")
        rs = await db.execute(text("""
            SELECT n_sources, COUNT(*) AS hours
            FROM (
                SELECT hour, COUNT(DISTINCT source) AS n_sources
                FROM generation_data
                WHERE windfarm_id = :wf AND hour < '2022-01-01'
                GROUP BY hour
            ) s GROUP BY n_sources ORDER BY n_sources
        """), {"wf": WINDFARM_ID})
        for r in rs:
            print(f"  {r.n_sources} sources/hour: {r.hours} hours")

        # 4. Per-month back-calculation comparison: 3 ways
        print("\n[4] Per-month capacity back-calc, three formulas (NVE only):")
        print("  fmA = SUM(gen) / (AVG(cf) * hours_in_month)")
        print("  fmB = SUM(gen) / (AVG(cf) * row_count)")
        print("  fmC = SUM(gen) / SUM(cf)        # gen-weighted")
        print(f"  {'month':<10}{'rows':>6}{'sum_gen':>12}{'avg_cf':>9}{'sum_cf':>9}{'fmA':>8}{'fmB':>8}{'fmC':>8}")
        rs = await db.execute(text("""
            SELECT
                TO_CHAR(date_trunc('month', hour), 'YYYY-MM') AS mo,
                COUNT(*) AS rows,
                SUM(generation_mwh)::float AS sum_gen,
                AVG(capacity_factor)::float AS avg_cf,
                SUM(capacity_factor)::float AS sum_cf,
                EXTRACT(EPOCH FROM (
                    date_trunc('month', hour) + INTERVAL '1 month' - date_trunc('month', hour)
                ))::int / 3600 AS hrs_in_month
            FROM generation_data
            WHERE windfarm_id = :wf AND source = 'NVE'
              AND hour < '2022-01-01' AND hour >= '2014-01-01'
              AND capacity_factor IS NOT NULL
            GROUP BY 1, 6
            ORDER BY 1
            LIMIT 24
        """), {"wf": WINDFARM_ID})
        for r in rs:
            fmA = r.sum_gen / (r.avg_cf * r.hrs_in_month) if r.avg_cf else 0
            fmB = r.sum_gen / (r.avg_cf * r.rows) if r.avg_cf else 0
            fmC = r.sum_gen / r.sum_cf if r.sum_cf else 0
            print(f"  {r.mo:<10}{r.rows:>6}{r.sum_gen:>12.1f}{r.avg_cf:>9.4f}{r.sum_cf:>9.2f}{fmA:>8.2f}{fmB:>8.2f}{fmC:>8.2f}")

        # 5. Spot-check: months where capacity_mw varies within a single month
        print("\n[5] Months where capacity_mw varies within month (NVE only):")
        rs = await db.execute(text("""
            SELECT TO_CHAR(date_trunc('month', hour), 'YYYY-MM') AS mo,
                   COUNT(DISTINCT capacity_mw) AS distinct_caps,
                   MIN(capacity_mw)::float AS min_cap,
                   MAX(capacity_mw)::float AS max_cap
            FROM generation_data
            WHERE windfarm_id = :wf AND source = 'NVE'
              AND hour < '2022-01-01' AND hour >= '2014-01-01'
            GROUP BY 1
            HAVING COUNT(DISTINCT capacity_mw) > 1
            ORDER BY 1
        """), {"wf": WINDFARM_ID})
        any_var = False
        for r in rs:
            any_var = True
            print(f"  {r.mo}: {r.distinct_caps} distinct caps, min={r.min_cap}, max={r.max_cap}")
        if not any_var:
            print("  none — capacity_mw is constant within every month pre-2022")

        # 6. Hour gaps: months with fewer rows than calendar hours
        print("\n[6] NVE pre-2022 monthly row count vs calendar hours:")
        rs = await db.execute(text("""
            WITH m AS (
                SELECT date_trunc('month', hour) AS mo,
                       COUNT(*) AS rows
                FROM generation_data
                WHERE windfarm_id = :wf AND source = 'NVE'
                  AND hour < '2022-01-01' AND hour >= '2014-12-01'
                GROUP BY 1
            )
            SELECT TO_CHAR(mo, 'YYYY-MM') AS mo,
                   rows,
                   EXTRACT(EPOCH FROM (mo + INTERVAL '1 month' - mo))::int / 3600 AS hrs
            FROM m WHERE rows != EXTRACT(EPOCH FROM (mo + INTERVAL '1 month' - mo))::int / 3600
            ORDER BY 1
            LIMIT 30
        """), {"wf": WINDFARM_ID})
        gaps = list(rs)
        if not gaps:
            print("  no row-count gaps — every NVE month has full calendar hours")
        else:
            print(f"  {'month':<10}{'rows':>6}{'hrs':>5}{'diff':>6}")
            for r in gaps:
                print(f"  {r.mo:<10}{r.rows:>6}{r.hrs:>5}{r.hrs-r.rows:>6}")

        # 7. Check unit start_date — does Raggovidda 12695 actually start mid-month?
        print("\n[7] Generation unit 12695 / 12805 metadata:")
        rs = await db.execute(text("""
            SELECT id, name, capacity_mw::float, is_active, start_date, end_date,
                   windfarm_id
            FROM generation_units
            WHERE id IN (12695, 12805)
            ORDER BY id
        """))
        for r in rs:
            print(f"  id={r.id} {r.name!r}, cap={r.capacity_mw}, active={r.is_active}, start={r.start_date}, end={r.end_date}, wf={r.windfarm_id}")


asyncio.run(main())
