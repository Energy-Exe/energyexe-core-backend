"""Targeted drilldowns:

A. The 4 CSV rows that still show is_active=True in DB:
   - Aikengall IIa (ELEXON T_AKGLW-3)
   - Havøygavlen Phase 3, Phase 4 (NVE)
   - Smøla Phase 2 (NVE)

B. Hundhammerfjellet — 205k rows on 2 inactive units, only 1,235 MWh on active.
   Question: are the active units silent/empty? What is the canonical unit?

C. The "DELETE ME" Aberdeen ELEXON unit (code 65511345553).

D. ENTSOE offshore farms: London Array, Lincs, Gwynt Y Mor — what
   active units do they have, and why is no data attributed to them?

E. Per-source breakdown of the 4 still-active units' data.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


async def main():
    S = get_session_factory()
    async with S() as db:
        # A. The 4 still-active units from CSV
        print("=" * 80)
        print("A. CSV-marked-inactive but DB says is_active=True")
        print("=" * 80)
        rs = await db.execute(text("""
            SELECT id, name, source, code, capacity_mw::float AS cap,
                   is_active, windfarm_id, start_date, end_date, first_power_date,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
            FROM generation_units gu
            WHERE id IN (12792, 12504, 12508, 12731)
        """))
        for r in rs:
            print(f"  id={r.id} {r.name!r} src={r.source} code={r.code} cap={r.cap} "
                  f"active={r.is_active} rows={r.rows}")

        # B. Hundhammerfjellet
        print("\n" + "=" * 80)
        print("B. Hundhammerfjellet — all units & their data")
        print("=" * 80)
        rs = await db.execute(text("""
            SELECT id, name, source, code, capacity_mw::float AS cap, is_active,
                   start_date, first_power_date,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows,
                   (SELECT MIN(hour) FROM generation_data WHERE generation_unit_id = gu.id) AS first_hr,
                   (SELECT MAX(hour) FROM generation_data WHERE generation_unit_id = gu.id) AS last_hr
            FROM generation_units gu
            WHERE windfarm_id = 7191
            ORDER BY is_active DESC, id
        """))
        print(f"  {'id':>6}{'is_active':>11}  {'name':<40}{'src':<8}{'code':<10}{'cap':>7}"
              f"{'rows':>9}{'first':>14}{'last':>14}")
        for r in rs:
            first = str(r.first_hr)[:10] if r.first_hr else "-"
            last = str(r.last_hr)[:10] if r.last_hr else "-"
            print(f"  {r.id:>6}{str(r.is_active):>11}  {r.name[:38]:<40}"
                  f"{r.source:<8}{(r.code or '')[:8]:<10}{r.cap or 0:>7.1f}"
                  f"{r.rows:>9,}{first:>14}{last:>14}")

        # C. Aberdeen DELETE ME
        print("\n" + "=" * 80)
        print("C. Aberdeen ELEXON 'DELETE ME' unit")
        print("=" * 80)
        rs = await db.execute(text("""
            SELECT id, name, source, code, capacity_mw::float AS cap, is_active,
                   windfarm_id, start_date, end_date, first_power_date,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
            FROM generation_units gu
            WHERE source = 'ELEXON' AND code = '65511345553'
        """))
        for r in rs:
            print(f"  id={r.id} name={r.name!r} cap={r.cap} active={r.is_active} "
                  f"wf={r.windfarm_id} rows={r.rows}")
            print(f"    start={r.start_date} fpd={r.first_power_date}")

        # D. ENTSOE offshore — what other units exist on these windfarms?
        print("\n" + "=" * 80)
        print("D. ENTSOE offshore: all units per windfarm with data attribution")
        print("=" * 80)
        for wf_id, wf_name in [
            (7392, "London Array"),
            (7391, "Lincs"),
            (7378, "Gwynt Y Mor"),
            (7385, "Hornsea 2"),
            (7406, "Rampion"),
        ]:
            print(f"\n  -- {wf_name} (id={wf_id}) --")
            rs = await db.execute(text("""
                SELECT id, name, source, is_active,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
                FROM generation_units gu WHERE windfarm_id = :w
                ORDER BY is_active DESC, id
            """), {"w": wf_id})
            for r in rs:
                print(f"    id={r.id} {r.name[:30]:<32} src={r.source:<8} "
                      f"active={r.is_active} rows={r.rows:,}")

        # Also check which sources have rows for these windfarms and on which units
        print("\n  By (windfarm, source, is_active) — total rows in generation_data:")
        rs = await db.execute(text("""
            SELECT gd.windfarm_id, wf.name, gd.source, gu.is_active,
                   COUNT(*) AS rows, SUM(gd.generation_mwh)::float AS gen
            FROM generation_data gd
            JOIN generation_units gu ON gu.id = gd.generation_unit_id
            JOIN windfarms wf ON wf.id = gd.windfarm_id
            WHERE gd.windfarm_id IN (7392, 7391, 7378, 7385, 7406)
            GROUP BY 1, 2, 3, 4
            ORDER BY 2, 3, 4
        """))
        for r in rs:
            print(f"    wf={r.name[:24]:<26} src={r.source:<8} active={r.is_active} "
                  f"rows={r.rows:,} gen={r.gen:,.0f}")


asyncio.run(main())
