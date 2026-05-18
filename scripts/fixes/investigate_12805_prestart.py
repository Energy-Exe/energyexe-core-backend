"""Drill-down: unit 12805 (Raggovidda 2) has start_date=2022-08-23 but
appears with rows in 2021-11 and 2021-12. Find out what happened.
"""
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory
async_session_maker = get_session_factory()


async def main():
    async with async_session_maker() as db:
        print("=" * 80)
        print("UNIT 12805 PRE-START-DATE ATTRIBUTION")
        print("=" * 80)

        print("\n[A] All distinct months unit 12805 has data, with row counts:")
        rs = await db.execute(text("""
            SELECT TO_CHAR(date_trunc('month', hour), 'YYYY-MM') AS mo,
                   COUNT(*) AS rows,
                   MIN(hour) AS first_hr,
                   MAX(hour) AS last_hr,
                   AVG(generation_mwh)::float AS avg_gen,
                   AVG(capacity_factor)::float AS avg_cf
            FROM generation_data
            WHERE generation_unit_id = 12805
            GROUP BY 1
            ORDER BY 1
        """))
        print(f"  {'month':<10}{'rows':>6}{'first':>22}{'last':>22}{'avg_gen':>10}{'avg_cf':>9}")
        for r in rs:
            print(f"  {r.mo:<10}{r.rows:>6}{str(r.first_hr):>22}{str(r.last_hr):>22}{r.avg_gen:>10.2f}{r.avg_cf:>9.4f}")

        print("\n[B] Same for unit 12695 around the boundary (Nov 2021 - Sep 2022):")
        rs = await db.execute(text("""
            SELECT TO_CHAR(date_trunc('month', hour), 'YYYY-MM') AS mo,
                   COUNT(*) AS rows,
                   MIN(capacity_mw)::float AS min_cap,
                   MAX(capacity_mw)::float AS max_cap,
                   AVG(generation_mwh)::float AS avg_gen
            FROM generation_data
            WHERE generation_unit_id = 12695
              AND hour >= '2021-11-01' AND hour < '2022-10-01'
            GROUP BY 1 ORDER BY 1
        """))
        print(f"  {'month':<10}{'rows':>6}{'min_cap':>10}{'max_cap':>10}{'avg_gen':>10}")
        for r in rs:
            print(f"  {r.mo:<10}{r.rows:>6}{r.min_cap:>10}{r.max_cap:>10}{r.avg_gen:>10.2f}")

        print("\n[C] Hours present in BOTH 12695 and 12805 (overlap = double-count):")
        rs = await db.execute(text("""
            SELECT TO_CHAR(date_trunc('month', a.hour), 'YYYY-MM') AS mo,
                   COUNT(*) AS overlap_hrs
            FROM generation_data a
            JOIN generation_data b ON a.hour = b.hour AND a.windfarm_id = b.windfarm_id AND a.source = b.source
            WHERE a.generation_unit_id = 12695 AND b.generation_unit_id = 12805
              AND a.hour < '2022-08-23'
            GROUP BY 1 ORDER BY 1
        """))
        any_overlap = False
        for r in rs:
            any_overlap = True
            print(f"  {r.mo}: {r.overlap_hrs} overlapping hours pre-12805-start")
        if not any_overlap:
            print("  no overlap — 12805 rows in pre-start months don't conflict with 12695")

        print("\n[D] Sample of 12805 rows pre-start_date (10 rows):")
        rs = await db.execute(text("""
            SELECT hour, generation_mwh::float AS gen, capacity_mw::float AS cap,
                   capacity_factor::float AS cf, windfarm_id, source
            FROM generation_data
            WHERE generation_unit_id = 12805 AND hour < '2022-08-23'
            ORDER BY hour LIMIT 10
        """))
        for r in rs:
            print(f"  {r.hour}  gen={r.gen:.2f}  cap={r.cap}  cf={r.cf:.3f}  wf={r.windfarm_id}  src={r.source}")

        print("\n[E] Total 12805 rows pre/post its declared start:")
        rs = await db.execute(text("""
            SELECT
              COUNT(*) FILTER (WHERE hour < '2022-08-23') AS pre,
              COUNT(*) FILTER (WHERE hour >= '2022-08-23') AS post
            FROM generation_data WHERE generation_unit_id = 12805
        """))
        r = rs.first()
        print(f"  pre-2022-08-23:  {r.pre:,}")
        print(f"  post-2022-08-23: {r.post:,}")


asyncio.run(main())
