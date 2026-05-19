"""Trace where ABRB0-1 (12328) and Galloper (12348-12351) data actually came
from, since neither has raw rows with its own EIC code.

Possibilities:
  1. Raw rows exist with a slightly different identifier (e.g. without trailing
     check char, or different prefix).
  2. Data was loaded directly into generation_data via an Excel/CSV seed.
  3. The raw rows exist but were keyed under a different source_type.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory

CHECKS = [
    (12328, "ABRB0-1",                            "48W00000ABRBO-1G", "ABRBO"),
    (12348, "Galloper Offshore Wind Farm GAOFO-1", "48W00000GAOFO-1Z", "GAOFO"),
    (12349, "Galloper Offshore Wind Farm GAOFO-2", "48W00000GAOFO-2X", "GAOFO"),
    (12350, "Galloper Offshore Wind Farm GAOFO-3", "48W00000GAOFO-3V", "GAOFO"),
    (12351, "Galloper Offshore Wind Farm GAOFO-4", "48W00000GAOFO-4T", "GAOFO"),
]


async def main():
    S = get_session_factory()
    async with S() as db:
        for uid, uname, code, prefix in CHECKS:
            print(f"\n{'='*100}")
            print(f"Unit {uid} '{uname}' code={code} prefix='{prefix}'")
            print('='*100)

            # 1. Raw rows whose identifier CONTAINS the prefix anywhere
            rs = await db.execute(text("""
                SELECT identifier, source_type, COUNT(*) AS rows,
                       MIN(period_start) AS first_pt, MAX(period_start) AS last_pt
                FROM generation_data_raw
                WHERE source = 'ENTSOE' AND identifier ILIKE :p
                GROUP BY 1, 2 ORDER BY rows DESC LIMIT 10
            """), {"p": f"%{prefix}%"})
            print(f"\n  Raw rows with identifier ILIKE '%{prefix}%':")
            rows = list(rs)
            for r in rows:
                print(f"    id={r.identifier} type={r.source_type}: rows={r.rows:,} "
                      f"{r.first_pt} → {r.last_pt}")
            if not rows:
                print(f"    (none)")

            # 2. Raw rows whose data->>'generation_unit_name' or 'generation_unit_code'
            #    contains the prefix
            rs = await db.execute(text("""
                SELECT identifier, source_type,
                       data->>'generation_unit_code' AS code,
                       data->>'generation_unit_name' AS name,
                       COUNT(*) AS rows,
                       MIN(period_start) AS first_pt, MAX(period_start) AS last_pt
                FROM generation_data_raw
                WHERE source = 'ENTSOE'
                  AND (data->>'generation_unit_code' ILIKE :p
                       OR data->>'generation_unit_name' ILIKE :p)
                GROUP BY 1, 2, 3, 4
                ORDER BY rows DESC LIMIT 10
            """), {"p": f"%{prefix}%"})
            print(f"\n  Raw rows whose data.code/name contains '{prefix}':")
            rows = list(rs)
            for r in rows:
                print(f"    id={r.identifier} type={r.source_type} "
                      f"data.code='{r.code}' data.name='{r.name}': rows={r.rows:,} "
                      f"{r.first_pt} → {r.last_pt}")
            if not rows:
                print(f"    (none)")

            # 3. Look at the actual generation_data rows for this unit — what
            #    is their `source_type` if such a column exists?
            rs = await db.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='generation_data' AND table_schema='public'
                ORDER BY ordinal_position
            """))
            gd_cols = [r.column_name for r in rs]
            extras = ", ".join(c for c in gd_cols if c in {"source_type", "raw_source"})
            print(f"\n  generation_data cols of interest: source plus {extras or '(none of source_type/raw_source)'}")

            # 4. For one example hour, look at what raw rows existed
            rs = await db.execute(text("""
                SELECT gd.hour, gd.source, gd.generation_mwh::float, gd.capacity_mw::float
                FROM generation_data gd
                WHERE gd.generation_unit_id = :u
                ORDER BY gd.hour LIMIT 3
            """), {"u": uid})
            print(f"\n  Sample generation_data rows for this unit:")
            for r in rs:
                print(f"    {r.hour}  gen={r.generation_mwh}  cap={r.capacity_mw}  source={r.source}")

        # 5. Broader: was there a seed script run that explicitly inserted Galloper data?
        print(f"\n{'='*100}")
        print(f"Look at seed/import scripts that might have inserted these")
        print('='*100)
        # Check for the existence of any raw row with identifier patterns containing
        # 'galloper' or 'aberdeen' in plain text
        for term in ["galloper", "aberdeen", "abrbo", "gaofo"]:
            rs = await db.execute(text("""
                SELECT identifier, source_type, COUNT(*) AS n
                FROM generation_data_raw
                WHERE source = 'ENTSOE'
                  AND (identifier ILIKE :t
                       OR data::text ILIKE :t)
                GROUP BY 1, 2 LIMIT 5
            """), {"t": f"%{term}%"})
            print(f"\n  '{term}' anywhere in raw:")
            for r in rs:
                print(f"    id={r.identifier} type={r.source_type}: {r.n:,}")


asyncio.run(main())
