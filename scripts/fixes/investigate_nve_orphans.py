"""Investigate the 6 NVE units with windfarm_id=NULL but containing data.

  12787 Ytre Vikna Phase 1   code=39   1,859 rows
  12797 Fjeldskår            code=1  142,464 rows
  12798 Sandøy               code=4  187,896 rows
  12800 Vikna                code=22  88,633 rows
  12801 Kvalnes              code=23  76,308 rows
  12802 Hovden Vesterålen    code=24 110,030 rows

Questions:
  1. Do real windfarms with these names exist in the DB? (NVE codes
     are stable per-farm; if the windfarm row exists, we can re-attach.)
  2. What is on generation_data.windfarm_id for these unit rows — also NULL,
     or pointing somewhere?
  3. Do other NVE units share the same `code` (1, 4, 22, 23, 24, 39) and have
     proper windfarm attachment? (Phase-units pattern.)
  4. When were these units created / data loaded?
  5. Where does raw NVE data for these codes live? Does an active counterpart
     exist that we should merge into?
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


UNITS = [
    (12787, "Ytre Vikna Phase 1", "39"),
    (12797, "Fjeldskår",          "1"),
    (12798, "Sandøy",             "4"),
    (12800, "Vikna",              "22"),
    (12801, "Kvalnes",            "23"),
    (12802, "Hovden Vesterålen",  "24"),
]


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def main():
    S = get_session_factory()
    async with S() as db:
        # 1. Full unit metadata
        banner("1. Unit metadata + data summary")
        for uid, uname, code in UNITS:
            rs = await db.execute(text("""
                SELECT id, name, code, source, capacity_mw::float AS cap, is_active,
                       windfarm_id, start_date, end_date, first_power_date,
                       created_at, updated_at,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows,
                       (SELECT SUM(generation_mwh)::float FROM generation_data WHERE generation_unit_id = gu.id) AS gen
                FROM generation_units gu WHERE id = :u
            """), {"u": uid})
            r = rs.first()
            if r:
                print(f"\n  id={r.id} '{r.name}' code={r.code} src={r.source} cap={r.cap}")
                print(f"    active={r.is_active}  wf={r.windfarm_id}  "
                      f"start={r.start_date}  fpd={r.first_power_date}  end={r.end_date}")
                print(f"    created={r.created_at}  updated={r.updated_at}")
                print(f"    rows={r.rows:,}  gen={(r.gen or 0):,.0f} MWh")

        # 2. Windfarm in DB matching each unit name
        banner("2. Windfarms with matching name in DB")
        for uid, uname, code in UNITS:
            rs = await db.execute(text("""
                SELECT id, name, code, country_id, status, first_power_date
                FROM windfarms
                WHERE name ILIKE :p1 OR name ILIKE :p2
                ORDER BY id
            """), {"p1": f"%{uname.split()[0]}%", "p2": f"%{uname}%"})
            print(f"\n  unit {uid} '{uname}':")
            rows = list(rs)
            if not rows:
                print(f"    (no windfarm with name matching)")
            for r in rows:
                print(f"    wf id={r.id} '{r.name}' code={r.code} status={r.status} fpd={r.first_power_date}")

        # 3. Other NVE units sharing the same code (find the "canonical" sibling)
        banner("3. Other NVE units sharing the same code (sibling phase units)")
        for uid, uname, code in UNITS:
            rs = await db.execute(text("""
                SELECT id, name, code, capacity_mw::float AS cap, is_active,
                       windfarm_id, start_date, end_date,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
                FROM generation_units gu
                WHERE source = 'NVE' AND code = :c AND id != :u
                ORDER BY is_active DESC, id
            """), {"c": code, "u": uid})
            rows = list(rs)
            print(f"\n  unit {uid} '{uname}' (code {code}): {len(rows)} siblings")
            for r in rows:
                print(f"    sibling id={r.id:>5} '{r.name[:40]:<42}' "
                      f"cap={(r.cap or 0):>6.1f} active={r.is_active} "
                      f"wf={r.windfarm_id} rows={r.rows:,}")

        # 4. generation_data.windfarm_id distribution for these unit rows
        banner("4. generation_data.windfarm_id distribution per unit")
        for uid, uname, code in UNITS:
            rs = await db.execute(text("""
                SELECT windfarm_id, COUNT(*) AS rows,
                       MIN(hour) AS first_hr, MAX(hour) AS last_hr,
                       SUM(generation_mwh)::float AS gen
                FROM generation_data
                WHERE generation_unit_id = :u
                GROUP BY 1 ORDER BY rows DESC
            """), {"u": uid})
            print(f"\n  unit {uid} '{uname}':")
            for r in rs:
                print(f"    gd.wf={r.windfarm_id}: {r.rows:>7,} rows "
                      f"({str(r.first_hr)[:10]} → {str(r.last_hr)[:10]}) "
                      f"gen={(r.gen or 0):>12,.0f}")

        # 5. generation_unit_mapping rows for these units (or for these codes)
        banner("5. generation_unit_mapping rows referencing these units")
        rs = await db.execute(text("""
            SELECT id, source, source_identifier, generation_unit_id, windfarm_id, is_active, created_at
            FROM generation_unit_mapping
            WHERE generation_unit_id = ANY(:ids)
               OR source_identifier ILIKE ANY(ARRAY['%Fjeldskår%','%Sandøy%','%Vikna%','%Kvalnes%','%Hovden%','%Ytre Vikna%'])
            ORDER BY id
        """), {"ids": [u for u, *_ in UNITS]})
        rows = list(rs)
        print(f"  {len(rows)} mapping rows")
        for r in rows:
            print(f"    id={r.id} src={r.source} ident='{r.source_identifier}' "
                  f"unit={r.generation_unit_id} wf={r.windfarm_id} active={r.is_active} created={r.created_at}")

        # 6. Raw NVE data for these codes — confirm data origin
        banner("6. NVE raw rows by identifier (codes 1, 4, 22, 23, 24, 39)")
        rs = await db.execute(text("""
            SELECT identifier, source_type, COUNT(*) AS rows,
                   MIN(period_start) AS first_pt, MAX(period_start) AS last_pt
            FROM generation_data_raw
            WHERE source = 'NVE' AND identifier IN ('1','4','22','23','24','39')
            GROUP BY 1, 2 ORDER BY identifier, source_type
        """))
        for r in rs:
            print(f"  id={r.identifier:<4} type={r.source_type:<10} rows={r.rows:,} "
                  f"{r.first_pt} → {r.last_pt}")


asyncio.run(main())
