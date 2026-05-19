"""Drill down on confirmed mislink cases:

  - 12385 Ormonde Eng Ltd     → wf 7385 Hornsea 2          (2014-12 → 2021-05)
  - 12346 East Anglia One     → wf 7370 Dudgeon            (2019-07 → 2021-05)
  - 12361 Hornsea 1           → wf 7380 Hollandse Kust Zuid (2019-07 → 2021-05)
  - 12348-12351 Galloper      → wf 7374 Gode Wind 1&2       (2021-01 → 2021-05)
  - 12328 ABRB0-1             → wf 7359 Beatrice             (Aberdeen Bay?)

Questions:
  1. Does generation_units.windfarm_id match generation_data.windfarm_id, or
     do they differ? (If GD.windfarm_id is right, only the unit attribution
     is wrong; if both are wrong, data is duplicated on the wrong windfarm.)
  2. For each mislinked unit, look up the raw ENTSOE record (generation_data_raw
     where identifier matches the unit code) — does the raw data carry the
     TRUE windfarm identifier? This tells us the matching logic failed
     at the aggregation step, not the ingest step.
  3. All mislinked units end at 2021-05-31 — what happened on that date?
     Was there a one-time import / re-aggregation?
  4. Does the windfarm 7385 (Hornsea 2) have *correct* Hornsea 2 data via
     other units, or is the Ormonde mislink the ONLY pre-2022 data the
     end-user sees?
  5. For all 4 active "Mermaid/Seastar/Thorntonbank" type cases, are they
     real mislinks or aliases (e.g. SeaMade is a JV of Mermaid + Seastar).

Run:
    poetry run python scripts/fixes/investigate_mislink_root_cause.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory

MISLINKED = [
    (12385, "Ormonde Eng Ltd", 7385, "Hornsea 2", "48W00000OMNDO-1J"),
    (12346, "East Anglia One", 7370, "Dudgeon", "48W000000EAAO-1R"),
    (12361, "Hornsea 1",       7380, "Hollandse Kust Zuid", "48W00000HOWAO-1M"),
    (12348, "Galloper GAOFO-1",7374, "Gode Wind 1&2", "48W00000GAOFO-1Z"),
    (12349, "Galloper GAOFO-2",7374, "Gode Wind 1&2", "48W00000GAOFO-2X"),
    (12350, "Galloper GAOFO-3",7374, "Gode Wind 1&2", "48W00000GAOFO-3V"),
    (12351, "Galloper GAOFO-4",7374, "Gode Wind 1&2", "48W00000GAOFO-4T"),
    (12328, "ABRB0-1",         7359, "Beatrice",     "48W00000ABRBO-1G"),
]


async def main():
    S = get_session_factory()
    async with S() as db:
        # Q1: Per unit, does generation_data.windfarm_id always equal the
        #     unit's windfarm_id, or do some rows point elsewhere?
        print("=" * 100)
        print("Q1. Per-unit consistency of generation_data.windfarm_id vs unit.windfarm_id")
        print("=" * 100)
        for uid, uname, wf_id, wf_name, code in MISLINKED:
            rs = await db.execute(text("""
                SELECT gd.windfarm_id, COUNT(*) AS rows
                FROM generation_data gd
                WHERE gd.generation_unit_id = :uid
                GROUP BY gd.windfarm_id
                ORDER BY rows DESC
            """), {"uid": uid})
            wf_distr = list(rs)
            distr_str = ", ".join(
                f"wf={r.windfarm_id}:{r.rows:,}" for r in wf_distr
            ) or "no rows"
            print(f"  unit {uid} '{uname}' (unit.wf_id={wf_id}): {distr_str}")

        # Q2: Look up raw rows for each unit's ENTSOE code. Does the raw record
        #     carry an additional identifier?
        print("\n" + "=" * 100)
        print("Q2. Raw ENTSOE rows by identifier code")
        print("=" * 100)
        for uid, uname, wf_id, wf_name, code in MISLINKED:
            rs = await db.execute(text("""
                SELECT identifier,
                       COUNT(*) AS rows,
                       MIN(period_start) AS first_pt,
                       MAX(period_start) AS last_pt,
                       (SELECT data FROM generation_data_raw raw2
                        WHERE raw2.identifier = raw.identifier AND raw2.source='ENTSOE'
                        LIMIT 1) AS sample_data
                FROM generation_data_raw raw
                WHERE source = 'ENTSOE' AND identifier = :id
                GROUP BY identifier
            """), {"id": code})
            rows = list(rs)
            if not rows:
                print(f"  unit {uid} code={code}: NO RAW ROWS — data was loaded via different identifier")
            for r in rows:
                print(f"  unit {uid} code={code}: rows={r.rows:,} first={r.first_pt} last={r.last_pt}")
                if r.sample_data:
                    print(f"    sample raw.data: {dict(r.sample_data)}")

        # Q2b: Reverse — look up identifiers that PRODUCED rows for the mislinked units.
        print("\n" + "=" * 100)
        print("Q2b. Reverse: what raw identifiers actually produced the generation_data rows?")
        print("=" * 100)
        for uid, uname, wf_id, wf_name, code in MISLINKED:
            rs = await db.execute(text("""
                SELECT source, source_type, identifier, COUNT(*) AS rows,
                       MIN(period_start) AS first_pt, MAX(period_start) AS last_pt
                FROM generation_data_raw raw
                WHERE EXISTS (
                    SELECT 1 FROM generation_data gd
                    WHERE gd.generation_unit_id = :uid
                      AND gd.hour = raw.period_start
                )
                  AND raw.source = 'ENTSOE'
                GROUP BY source, source_type, identifier
                ORDER BY rows DESC
                LIMIT 5
            """), {"uid": uid})
            rows = list(rs)
            print(f"\n  unit {uid} '{uname}': raw identifiers contributing to its hours:")
            for r in rows:
                print(f"    src={r.source} type={r.source_type} id={r.identifier}: "
                      f"rows={r.rows:,} {r.first_pt} → {r.last_pt}")

        # Q3: All mislinked units have data ending 2021-05-31. Look at when the
        #     rows were inserted / last updated.
        print("\n" + "=" * 100)
        print("Q3. When were these rows inserted? (look at created_at / updated_at)")
        print("=" * 100)
        for uid, uname, *_ in MISLINKED:
            rs = await db.execute(text("""
                SELECT MIN(created_at) AS earliest_ins,
                       MAX(created_at) AS latest_ins,
                       MIN(updated_at) AS earliest_upd,
                       MAX(updated_at) AS latest_upd,
                       COUNT(*) AS rows
                FROM generation_data
                WHERE generation_unit_id = :uid
            """), {"uid": uid})
            r = rs.first()
            if r and r.rows:
                print(f"  unit {uid} '{uname}': rows={r.rows:,}")
                print(f"    created_at: {r.earliest_ins} → {r.latest_ins}")
                print(f"    updated_at: {r.earliest_upd} → {r.latest_upd}")

        # Q4: For each "victim" windfarm (the wrong target), is there OTHER
        #     data (different sources/units) covering the same period? i.e.
        #     does the end-user see DOUBLE data, REPLACEMENT data, or sole
        #     data?
        print("\n" + "=" * 100)
        print("Q4. Victim-windfarm data composition by source + active flag")
        print("=" * 100)
        victims = {7385, 7370, 7380, 7374, 7359}
        for v in victims:
            rs = await db.execute(text("""
                SELECT wf.name AS wf_name, gd.source, gu.is_active,
                       COUNT(*) AS rows, SUM(gd.generation_mwh)::float AS gen,
                       MIN(gd.hour) AS first_hr, MAX(gd.hour) AS last_hr
                FROM generation_data gd
                JOIN generation_units gu ON gu.id = gd.generation_unit_id
                JOIN windfarms wf ON wf.id = gd.windfarm_id
                WHERE gd.windfarm_id = :v
                GROUP BY 1, 2, 3
                ORDER BY 2, 3 DESC
            """), {"v": v})
            print(f"\n  wf {v}:")
            for r in rs:
                print(f"    src={r.source:<8} active={r.is_active} rows={r.rows:,} "
                      f"gen={(r.gen or 0):>13,.0f} MWh  {str(r.first_hr)[:10]} → {str(r.last_hr)[:10]}")

        # Q5: For the 8 still-active mismatch units, check if they're aliases
        #     (look up windfarm aliases)
        print("\n" + "=" * 100)
        print("Q5. Active 'mismatched' units — are they JV/aliases or actual mislinks?")
        print("=" * 100)
        for uid, expected_alias_wf in [
            (10334, "SeaMade"),
            (10335, "SeaMade"),
            (10314, "Nysted"),
            (10342, "Thornton Bank"),
            (10252, "Fécamp"),
            (10326, "Saint-Nazaire"),
        ]:
            rs = await db.execute(text("""
                SELECT gu.id, gu.name, gu.source, gu.code,
                       gu.windfarm_id, wf.name AS wf_name,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
                FROM generation_units gu LEFT JOIN windfarms wf ON wf.id = gu.windfarm_id
                WHERE gu.id = :uid
            """), {"uid": uid})
            for r in rs:
                print(f"  id={r.id} '{r.name}' code={r.code} → wf {r.windfarm_id} '{r.wf_name}' "
                      f"rows={r.rows:,}")


asyncio.run(main())
