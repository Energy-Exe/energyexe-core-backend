"""Look at every NVE unit with code 1090 — active, inactive, orphan — and
check whether 12805's 36,318 pre-2022-08-23 rows are real (NVE genuinely
sent data with code=1090 before the unit's declared start_date) or
whether the repair re-aggregator picked the wrong unit.
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
        print("NVE CODE 1090 — every unit, every windfarm")
        print("=" * 80)

        rs = await db.execute(text("""
            SELECT id, name, source, code, capacity_mw::float, is_active,
                   windfarm_id, start_date, end_date,
                   (SELECT COUNT(*) FROM generation_data gd WHERE gd.generation_unit_id = gu.id) AS gd_rows
            FROM generation_units gu
            WHERE code = '1090' AND source = 'NVE'
            ORDER BY id
        """))
        for r in rs:
            print(f"  id={r.id} {r.name!r:40} cap={r.capacity_mw:6.2f} active={r.is_active} "
                  f"wf={r.windfarm_id} start={r.start_date} end={r.end_date} gd_rows={r.gd_rows:,}")

        # Same for code 46 in case there is a still-hidden unit
        print("\nNVE CODE 46 — every unit, every windfarm")
        rs = await db.execute(text("""
            SELECT id, name, source, code, capacity_mw::float, is_active,
                   windfarm_id, start_date, end_date,
                   (SELECT COUNT(*) FROM generation_data gd WHERE gd.generation_unit_id = gu.id) AS gd_rows
            FROM generation_units gu
            WHERE code = '46' AND source = 'NVE'
            ORDER BY id
        """))
        for r in rs:
            print(f"  id={r.id} {r.name!r:40} cap={r.capacity_mw:6.2f} active={r.is_active} "
                  f"wf={r.windfarm_id} start={r.start_date} end={r.end_date} gd_rows={r.gd_rows:,}")

        print("\n[Q] Does the RAW NVE data have code=1090 entries before 2022-08-23?")
        rs = await db.execute(text("""
            SELECT TO_CHAR(date_trunc('month', period_start), 'YYYY-MM') AS mo,
                   COUNT(*) AS rows,
                   MIN(period_start) AS first_pt,
                   MAX(period_start) AS last_pt
            FROM generation_data_raw
            WHERE source = 'NVE' AND identifier = '1090'
              AND period_start < '2022-08-23'
            GROUP BY 1 ORDER BY 1
        """))
        any_raw = False
        for r in rs:
            any_raw = True
            print(f"  {r.mo}: {r.rows} raw rows  first={r.first_pt} last={r.last_pt}")
        if not any_raw:
            print("  no raw rows with identifier=1090 pre 2022-08-23 — so the 1254 rows on 12805 are bogus re-aggregation output")

        print("\n[R] Does the RAW NVE data have code=46 entries before 2022-08-23?")
        rs = await db.execute(text("""
            SELECT COUNT(*) AS rows, MIN(period_start) AS first_pt, MAX(period_start) AS last_pt
            FROM generation_data_raw
            WHERE source = 'NVE' AND identifier = '46' AND period_start < '2022-08-23'
        """))
        r = rs.first()
        print(f"  rows={r.rows:,}  first={r.first_pt}  last={r.last_pt}")


asyncio.run(main())
