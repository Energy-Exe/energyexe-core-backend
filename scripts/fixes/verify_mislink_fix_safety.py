"""Triple-check verification before fixing the 8 ENTSOE unit-windfarm mislinks.

For each of 8 units (Ormonde, ABRB0-1, Hornsea 1, East Anglia One, 4 Gallopers),
verify that:

  1. The unit's `code` and the raw ENTSOE rows agree on the *real* windfarm
     (i.e., the data is genuinely Ormonde/Aberdeen/etc., not mislabeled).
     Sample raw.data JSON to confirm map_code/area_code/generation_unit_name.

  2. The proposed correct windfarm (7404 Ormonde, 7350 Aberdeen, etc.) has no
     pre-existing ENTSOE data that would overlap (i.e., we're not about to
     create duplicates).

  3. The proposed correct windfarm has compatible metadata: country, bidzone,
     status, capacity ballpark.

  4. The victim windfarms (5 wrong targets) lose no legitimate data — every
     row attributed to one of the 8 units IS the only ENTSOE contribution to
     that victim, and removing it leaves OTHER sources untouched.

  5. Full-length cross-source check: for each row that will move, are there
     parallel (ELEXON / EEX / EIA) rows on the SAME wf at the SAME hour that
     would now mean the data IS authentic at that target wf?

  6. data_anomalies, performance_summaries, generation_unit_mapping all
     reference the right wf_id post-fix.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


# (unit_id, unit_name, code, wrong_wf, correct_wf)
PLAN = [
    (12385, "Ormonde Eng Ltd",                   "48W00000OMNDO-1J",  7385, 7404),
    (12328, "ABRB0-1",                           "48W00000ABRBO-1G",  7359, 7350),
    (12361, "Hornsea 1",                         "48W00000HOWAO-1M",  7380, 7384),
    (12346, "East Anglia One",                   "48W000000EAAO-1R",  7370, 7371),
    (12348, "Galloper Offshore Wind Farm GAOFO-1","48W00000GAOFO-1Z", 7374, 7373),
    (12349, "Galloper Offshore Wind Farm GAOFO-2","48W00000GAOFO-2X", 7374, 7373),
    (12350, "Galloper Offshore Wind Farm GAOFO-3","48W00000GAOFO-3V", 7374, 7373),
    (12351, "Galloper Offshore Wind Farm GAOFO-4","48W00000GAOFO-4T", 7374, 7373),
]


async def main():
    S = get_session_factory()
    async with S() as db:
        # -------- 1. RAW DATA AUTHENTICITY --------
        print("=" * 100)
        print("STEP 1. Raw ENTSOE data authenticity — does the data carry the right name/code?")
        print("=" * 100)
        for uid, uname, code, wrong, right in PLAN:
            print(f"\n  unit {uid} '{uname}' code={code}")
            rs = await db.execute(text("""
                SELECT
                  data->>'generation_unit_code' AS gu_code,
                  data->>'generation_unit_name' AS gu_name,
                  data->>'map_code'             AS map_code,
                  data->>'area_code'            AS area_code,
                  data->>'installed_capacity_mw' AS inst_cap,
                  data->>'generation_unit_type' AS type,
                  COUNT(*) AS n,
                  MIN(period_start) AS first_pt,
                  MAX(period_start) AS last_pt
                FROM generation_data_raw
                WHERE source = 'ENTSOE' AND identifier = :code
                GROUP BY 1, 2, 3, 4, 5, 6
                ORDER BY n DESC
                LIMIT 4
            """), {"code": code})
            rows = list(rs)
            if not rows:
                print(f"    !! NO raw rows for code {code}")
            for r in rows:
                print(f"    raw.code={r.gu_code} raw.name='{r.gu_name}' map={r.map_code} "
                      f"area={r.area_code} cap={r.inst_cap} type={r.type} count={r.n:,}")
                print(f"      {r.first_pt} → {r.last_pt}")

        # -------- 2. PRE-EXISTING ENTSOE DATA ON CORRECT WINDFARM --------
        print("\n" + "=" * 100)
        print("STEP 2. Does the correct-target windfarm already have ENTSOE data?")
        print("         (we want NO overlap — if it does, we have a real problem)")
        print("=" * 100)
        for uid, uname, code, wrong, right in PLAN:
            rs = await db.execute(text("""
                SELECT gd.source,
                       gu.id AS uid, gu.name AS uname, gu.code AS ucode,
                       COUNT(*) AS rows,
                       MIN(gd.hour) AS first_hr, MAX(gd.hour) AS last_hr,
                       SUM(gd.generation_mwh)::float AS gen
                FROM generation_data gd
                JOIN generation_units gu ON gu.id = gd.generation_unit_id
                WHERE gd.windfarm_id = :w
                GROUP BY 1, 2, 3, 4
                ORDER BY gd.source, gu.id
            """), {"w": right})
            print(f"\n  CORRECT wf {right} (for unit {uid}):")
            for r in rs:
                print(f"    src={r.source:<8} unit={r.uid} '{r.uname[:30]}' code={r.ucode}: "
                      f"{r.rows:,} rows, {r.first_hr} → {r.last_hr}, gen={(r.gen or 0):,.0f}")

        # -------- 3. CORRECT WINDFARM METADATA --------
        print("\n" + "=" * 100)
        print("STEP 3. Correct-windfarm metadata sanity check")
        print("=" * 100)
        ids_right = sorted({right for _, _, _, _, right in PLAN})
        rs = await db.execute(text("""
            SELECT w.id, w.name, w.status, w.nameplate_capacity_mw::float AS cap,
                   w.commercial_operational_date, w.first_power_date,
                   w.country_id, c.name AS country_name,
                   w.bidzone_id
            FROM windfarms w
            LEFT JOIN countries c ON c.id = w.country_id
            WHERE w.id = ANY(:ids)
            ORDER BY w.id
        """), {"ids": ids_right})
        for r in rs:
            print(f"  wf {r.id} '{r.name}': status={r.status}, cap={r.cap}, "
                  f"country={r.country_name}, COD={r.commercial_operational_date}, "
                  f"FPD={r.first_power_date}, bidzone={r.bidzone_id}")

        # -------- 4. VICTIM WINDFARM POST-FIX STATE --------
        print("\n" + "=" * 100)
        print("STEP 4. Victim windfarms — what data remains after we remove the bad ENTSOE rows?")
        print("=" * 100)
        ids_wrong = sorted({wrong for _, _, _, wrong, _ in PLAN})
        for wf_id in ids_wrong:
            print(f"\n  VICTIM wf {wf_id}:")
            rs = await db.execute(text("""
                SELECT gd.source, gu.is_active, COUNT(*) AS rows,
                       MIN(gd.hour) AS first_hr, MAX(gd.hour) AS last_hr,
                       SUM(gd.generation_mwh)::float AS gen
                FROM generation_data gd
                JOIN generation_units gu ON gu.id = gd.generation_unit_id
                WHERE gd.windfarm_id = :w
                GROUP BY 1, 2 ORDER BY 1
            """), {"w": wf_id})
            for r in rs:
                print(f"    src={r.source:<8} active={r.is_active} "
                      f"rows={r.rows:>9,} {str(r.first_hr)[:10]} → {str(r.last_hr)[:10]} "
                      f"gen={(r.gen or 0):>14,.0f}")

        # -------- 5. CROSS-SOURCE VERIFICATION: for the data being moved, is
        #     there a parallel source at the CORRECT windfarm that would
        #     corroborate?
        print("\n" + "=" * 100)
        print("STEP 5. Cross-source corroboration — sample 5 hours per unit")
        print("=" * 100)
        for uid, uname, code, wrong, right in PLAN:
            print(f"\n  unit {uid} → moving to wf {right}:")
            rs = await db.execute(text("""
                WITH unit_rows AS (
                    SELECT hour, generation_mwh::float AS my_gen
                    FROM generation_data
                    WHERE generation_unit_id = :u
                    ORDER BY hour
                    LIMIT 5
                )
                SELECT ur.hour, ur.my_gen,
                       (SELECT json_agg(json_build_object('src', gd.source,
                                                          'gen', gd.generation_mwh,
                                                          'unit_id', gd.generation_unit_id))
                        FROM generation_data gd
                        WHERE gd.hour = ur.hour
                          AND gd.windfarm_id = :right_wf
                          AND gd.generation_unit_id != :u) AS other_at_correct
                FROM unit_rows ur
            """), {"u": uid, "right_wf": right})
            for r in rs:
                print(f"    {r.hour}  my_gen={r.my_gen:.2f}  other_at_correct={r.other_at_correct}")

        # -------- 6. RELATED TABLES --------
        print("\n" + "=" * 100)
        print("STEP 6. Related tables that may carry the wrong windfarm_id")
        print("=" * 100)
        unit_ids = [u for u, *_ in PLAN]
        wrong_ids = sorted({wrong for _, _, _, wrong, _ in PLAN})
        for tbl_name, has_wf_id, has_unit_id in [
            ("data_anomalies", True, True),
            ("performance_summaries", True, False),
            ("generation_unit_mapping", True, True),
            ("p50_targets", True, False),
            ("peer_aggregates", True, False),
        ]:
            # Check if table exists
            rs = await db.execute(text("""
                SELECT 1 FROM information_schema.tables
                WHERE table_name = :t AND table_schema = 'public'
            """), {"t": tbl_name})
            if not rs.first():
                print(f"  {tbl_name}: table not present, skipped")
                continue
            # Pull columns
            rs = await db.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = :t AND table_schema = 'public'
            """), {"t": tbl_name})
            cols = [r.column_name for r in rs]
            wf_col = "windfarm_id" if "windfarm_id" in cols else None
            unit_col = "generation_unit_id" if "generation_unit_id" in cols else None
            print(f"\n  {tbl_name} (cols include: wf={wf_col}, unit={unit_col})")
            if unit_col:
                rs = await db.execute(text(f"""
                    SELECT COUNT(*) AS n,
                           COUNT(DISTINCT {wf_col}) AS distinct_wfids
                    FROM {tbl_name}
                    WHERE {unit_col} = ANY(:ids)
                """ if wf_col else f"""
                    SELECT COUNT(*) AS n, NULL AS distinct_wfids FROM {tbl_name}
                    WHERE {unit_col} = ANY(:ids)
                """), {"ids": unit_ids})
                r = rs.first()
                print(f"    rows referencing one of our 8 units: {r.n}")
            if wf_col:
                rs = await db.execute(text(f"""
                    SELECT {wf_col} AS wfid, COUNT(*) AS n
                    FROM {tbl_name}
                    WHERE {wf_col} = ANY(:ids)
                    GROUP BY 1 ORDER BY n DESC
                """), {"ids": wrong_ids})
                rows = list(rs)
                if rows:
                    print(f"    rows referencing victim wf_ids:")
                    for r in rows:
                        print(f"      wf {r.wfid}: {r.n:,}")

        # -------- 7. TOTAL ROWS THE FIX WILL TOUCH --------
        print("\n" + "=" * 100)
        print("STEP 7. Total rows the fix will UPDATE")
        print("=" * 100)
        rs = await db.execute(text("""
            SELECT generation_unit_id, COUNT(*) AS n
            FROM generation_data
            WHERE generation_unit_id = ANY(:ids)
            GROUP BY generation_unit_id
            ORDER BY 1
        """), {"ids": unit_ids})
        total = 0
        for r in rs:
            print(f"  unit {r.generation_unit_id}: {r.n:,} generation_data rows")
            total += r.n
        print(f"  TOTAL: {total:,} generation_data rows + 8 generation_units rows "
              f"+ 8 generation_unit_mapping rows")


asyncio.run(main())
