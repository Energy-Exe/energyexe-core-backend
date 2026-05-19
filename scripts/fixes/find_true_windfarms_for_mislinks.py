"""Find the TRUE windfarms for each mislinked unit.

The 8 confirmed cross-contaminations need a target windfarm. Lookup:
  - 'East Anglia' — does it exist?
  - 'Hornsea' — Hornsea 1 vs Hornsea 2 vs Hornsea Project ...
  - 'Galloper' — does it exist?
  - 'Aberdeen Bay' / 'EOWDC' — ABRB code
  - 'Ormonde' — confirm 7404 is correct (already seen in earlier output)

Also: check what aliases exist via windfarm_aliases (if such a table).

Then for the broader audit, run the 50 'EIA active mismatch' cases through
to confirm they are aliases, not bugs.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory

TARGETS = [
    "Ormonde",
    "East Anglia",
    "Hornsea",
    "Galloper",
    "Aberdeen",
    "Beatrice",
    "Dudgeon",
    "Hollandse",
    "Gode Wind",
    "EOWDC",
]


async def main():
    S = get_session_factory()
    async with S() as db:
        # 1. Search windfarms by name LIKE for each target
        print("=" * 100)
        print("Windfarms in DB containing each target token")
        print("=" * 100)
        # discover windfarms columns first
        rs = await db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='windfarms' AND table_schema='public'
            ORDER BY ordinal_position
        """))
        wf_cols = [r.column_name for r in rs]
        print(f"  windfarms columns: {wf_cols}")

        for t in TARGETS:
            rs = await db.execute(text("""
                SELECT id, name,
                       (SELECT COUNT(*) FROM generation_units WHERE windfarm_id = w.id) AS n_units
                FROM windfarms w
                WHERE name ILIKE :p
                ORDER BY id
            """), {"p": f"%{t}%"})
            rows = list(rs)
            print(f"\n  '{t}': {len(rows)} matching windfarms")
            for r in rows:
                print(f"    id={r.id} '{r.name}' n_units={r.n_units}")

        # 2. Look at every unit attached to Hornsea 2 (7385) to confirm Ormonde is the only
        #    pre-2022 oddity.
        print("\n" + "=" * 100)
        print("All units linked to Hornsea 2 (wf 7385)")
        print("=" * 100)
        rs = await db.execute(text("""
            SELECT id, name, code, source, capacity_mw::float AS cap, is_active,
                   start_date, first_power_date,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
            FROM generation_units gu
            WHERE windfarm_id = 7385
            ORDER BY is_active DESC, id
        """))
        for r in rs:
            print(f"  id={r.id:>5} {r.name[:34]:<36} src={r.source:<7} "
                  f"code={(r.code or '')[:20]:<22} cap={(r.cap or 0):>6.1f} "
                  f"active={r.is_active} rows={r.rows:,}")

        # 3. Same for other victims
        for wf_id, wf_name in [(7370, "Dudgeon"), (7380, "Hollandse Kust Zuid"),
                                (7374, "Gode Wind 1&2"), (7359, "Beatrice"),
                                (7404, "Ormonde")]:
            print(f"\nAll units on wf {wf_id} '{wf_name}':")
            rs = await db.execute(text("""
                SELECT id, name, code, source, is_active,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows,
                       (SELECT MIN(hour) FROM generation_data WHERE generation_unit_id = gu.id) AS first_hr,
                       (SELECT MAX(hour) FROM generation_data WHERE generation_unit_id = gu.id) AS last_hr
                FROM generation_units gu WHERE windfarm_id = :w ORDER BY is_active DESC, id
            """), {"w": wf_id})
            for r in rs:
                first = str(r.first_hr)[:10] if r.first_hr else "-"
                last = str(r.last_hr)[:10] if r.last_hr else "-"
                print(f"  id={r.id:>5} {r.name[:34]:<36} src={r.source:<7} "
                      f"code={(r.code or '')[:20]:<22} active={r.is_active} "
                      f"rows={r.rows:>7,} {first} → {last}")

        # 4. Look at the windfarm_aliases or similar
        print("\n" + "=" * 100)
        print("Check for windfarm-alias tables")
        print("=" * 100)
        rs = await db.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name ILIKE '%alias%'
        """))
        for r in rs:
            print(f"  table: {r.table_name}")

        # 5. Spot-check 5 EIA active mismatch units to confirm they're aliases
        print("\n" + "=" * 100)
        print("EIA active mismatches — spot check")
        print("=" * 100)
        rs = await db.execute(text("""
            SELECT gu.id, gu.name, gu.code, wf.name AS wf_name, wf.id AS wf_id,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
            FROM generation_units gu JOIN windfarms wf ON wf.id = gu.windfarm_id
            WHERE gu.id IN (10446, 10529, 10662, 10845, 11200, 11253)
        """))
        for r in rs:
            print(f"  id={r.id} '{r.name}' code={r.code} → wf={r.wf_id} '{r.wf_name}' "
                  f"rows={r.rows:,}")

        # 6. Use generation_unit_mapping if exists to see the canonical mapping
        rs = await db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='generation_unit_mapping' AND table_schema='public'
            ORDER BY ordinal_position
        """))
        cols = [r.column_name for r in rs]
        if cols:
            print(f"\n  generation_unit_mapping columns: {cols}")
            rs = await db.execute(text("""
                SELECT * FROM generation_unit_mapping
                WHERE generation_unit_id IN (12385, 12346, 12361, 12348, 12349, 12350, 12351, 12328)
                LIMIT 30
            """))
            for r in rs:
                print(f"    {dict(r._mapping)}")


asyncio.run(main())
