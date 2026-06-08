"""For the 8 reconnected ENTSOE units, compare against the ELEXON coverage on
the same windfarm:
  - Does ELEXON cover the same hours?
  - If yes, the ENTSOE unit is redundant (delete candidate).
  - If no (ELEXON has gaps in that period), the ENTSOE unit fills value (keep).
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


UNITS = [
    (12328, 7350, "ABRB0-1 → Aberdeen"),
    (12346, 7371, "East Anglia One"),
    (12348, 7373, "Galloper GAOFO-1"),
    (12349, 7373, "Galloper GAOFO-2"),
    (12350, 7373, "Galloper GAOFO-3"),
    (12351, 7373, "Galloper GAOFO-4"),
    (12361, 7384, "Hornsea 1"),
    (12385, 7404, "Ormonde Eng Ltd"),
]


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def main():
    S = get_session_factory()
    async with S() as db:
        for uid, wf, label in UNITS:
            banner(f"unit {uid} ({label}) on wf {wf}")

            # ENTSOE unit time range
            rs = await db.execute(text("""
                SELECT MIN(hour) AS lo, MAX(hour) AS hi, COUNT(*) AS n,
                       SUM(generation_mwh)::float AS gen
                FROM generation_data WHERE generation_unit_id = :u
            """), {"u": uid})
            e = rs.first()
            print(f"  ENTSOE unit data: {e.n:>7,} rows  {e.lo}  →  {e.hi}  gen={e.gen:,.0f}")

            # ELEXON coverage on same windfarm (only active units)
            rs = await db.execute(text("""
                SELECT MIN(gd.hour) AS lo, MAX(gd.hour) AS hi, COUNT(*) AS n,
                       SUM(gd.generation_mwh)::float AS gen
                FROM generation_data gd
                JOIN generation_units gu ON gu.id = gd.generation_unit_id
                WHERE gd.windfarm_id = :w AND gd.source = 'ELEXON' AND gu.is_active = TRUE
            """), {"w": wf})
            x = rs.first()
            if x.n and x.n > 0:
                print(f"  ELEXON coverage:  {x.n:>7,} rows  {x.lo}  →  {x.hi}  gen={x.gen:,.0f}")
            else:
                print(f"  ELEXON coverage:  NONE")

            # Does ELEXON cover the ENTSOE period? Check hours present in ENTSOE
            # but NOT in ELEXON aggregate.
            rs = await db.execute(text("""
                WITH entsoe AS (
                    SELECT hour FROM generation_data
                    WHERE generation_unit_id = :u
                ),
                elexon AS (
                    SELECT DISTINCT gd.hour
                    FROM generation_data gd
                    JOIN generation_units gu ON gu.id = gd.generation_unit_id
                    WHERE gd.windfarm_id = :w AND gd.source = 'ELEXON' AND gu.is_active = TRUE
                )
                SELECT COUNT(*) AS entsoe_only
                FROM entsoe e WHERE NOT EXISTS (SELECT 1 FROM elexon x WHERE x.hour = e.hour)
            """), {"u": uid, "w": wf})
            entsoe_only = rs.scalar()
            print(f"  Hours in ENTSOE but NOT in ELEXON: {entsoe_only:,}")

            # When does ELEXON's data on this windfarm actually START?
            rs = await db.execute(text("""
                SELECT MIN(gd.hour) AS first_elexon_hour
                FROM generation_data gd
                JOIN generation_units gu ON gu.id = gd.generation_unit_id
                WHERE gd.windfarm_id = :w AND gd.source = 'ELEXON' AND gu.is_active = TRUE
            """), {"w": wf})
            first_el = rs.scalar()
            print(f"  ELEXON earliest hour: {first_el}")

            # ENTSOE pre-ELEXON window — how many hours, how much gen?
            if first_el:
                rs = await db.execute(text("""
                    SELECT COUNT(*) AS n, SUM(generation_mwh)::float AS gen
                    FROM generation_data
                    WHERE generation_unit_id = :u AND hour < :el
                """), {"u": uid, "el": first_el})
                pre = rs.first()
                if pre.n and pre.n > 0:
                    print(f"  ENTSOE rows BEFORE ELEXON starts: {pre.n:,} rows, "
                          f"{pre.gen:,.0f} MWh (UNIQUE value)")
                else:
                    print(f"  ENTSOE rows BEFORE ELEXON starts: 0  → fully redundant")


asyncio.run(main())
