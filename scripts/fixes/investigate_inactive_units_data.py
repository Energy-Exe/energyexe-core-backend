"""Investigate generation_data linked to inactive generation units.

Input: /Users/mdfaisal/Downloads/inactive_generation_units_with_source.csv
       (code, name, status, source, windfarm_name)

Goals:
  1. Match each CSV row to a generation_units row in DB by (source, code, name).
     Note: NVE phase units share `code` so name is the discriminator.
  2. Count rows in generation_data linked to each inactive unit.
  3. Detect double-counting risk: hours where both an inactive unit AND an
     active unit on the same windfarm carry rows for the same hour
     (post-aggregation, comparison_service / generation_export_service sum by
     windfarm_id, so any row with windfarm_id set contributes — regardless of
     unit attribution).
  4. Quantify aggregate impact: sum of generation_mwh across linked rows,
     grouped by source and windfarm_name.

Run:
    poetry run python scripts/fixes/investigate_inactive_units_data.py
"""

import asyncio
import csv
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory

CSV_PATH = "/Users/mdfaisal/Downloads/inactive_generation_units_with_source.csv"


def load_csv():
    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


async def main():
    csv_rows = load_csv()
    print(f"Loaded {len(csv_rows)} rows from CSV")
    by_source = defaultdict(int)
    for r in csv_rows:
        by_source[r["source"]] += 1
    for s, n in sorted(by_source.items()):
        print(f"  {s}: {n}")

    S = get_session_factory()
    async with S() as db:
        # --- 1. Match each CSV row to a unit in DB (name + source + code).
        print("\n[1] Matching CSV rows to generation_units rows in DB...")
        matched = []
        unmatched = []
        for r in csv_rows:
            rs = await db.execute(
                text(
                    """
                    SELECT id, name, code, source, capacity_mw::float AS cap,
                           is_active, windfarm_id, start_date, end_date,
                           first_power_date
                    FROM generation_units
                    WHERE source = :src AND code = :code AND name = :name
                    """
                ),
                {"src": r["source"], "code": r["code"], "name": r["name"]},
            )
            units = list(rs)
            if len(units) == 0:
                unmatched.append(r)
            elif len(units) == 1:
                u = units[0]
                matched.append((r, u))
            else:
                # Same (source, code, name) → multi-row. Take all.
                for u in units:
                    matched.append((r, u))
        print(f"  matched: {len(matched)}")
        print(f"  unmatched: {len(unmatched)}")
        if unmatched:
            print("  Sample unmatched (first 10):")
            for r in unmatched[:10]:
                print(
                    f"    src={r['source']:8} code={r['code']:20} name={r['name']!r}"
                )

        # --- 2. Active vs inactive split among matched
        active_matched = [m for m in matched if m[1].is_active]
        inactive_matched = [m for m in matched if not m[1].is_active]
        print(f"\n[2] Of matched: {len(inactive_matched)} inactive, {len(active_matched)} ACTIVE (unexpected — flag)")
        if active_matched:
            print("  ACTIVE ones from CSV (these were marked inactive in input but are still is_active=true):")
            for r, u in active_matched[:20]:
                print(f"    id={u.id} {u.name!r} src={u.source} active={u.is_active}")

        # --- 3. Count generation_data rows linked to each inactive unit
        print("\n[3] Counting generation_data rows linked to each inactive unit...")
        ids = [u.id for _, u in inactive_matched]
        if not ids:
            print("  No inactive units matched — nothing to query.")
            return

        rs = await db.execute(
            text(
                """
                SELECT generation_unit_id,
                       COUNT(*) AS rows,
                       MIN(hour) AS first_hr,
                       MAX(hour) AS last_hr,
                       SUM(generation_mwh)::float AS sum_gen,
                       COUNT(DISTINCT source) AS distinct_sources,
                       COUNT(DISTINCT windfarm_id) AS distinct_wfids,
                       BOOL_OR(windfarm_id IS NULL) AS any_null_wfid
                FROM generation_data
                WHERE generation_unit_id = ANY(:ids)
                GROUP BY generation_unit_id
                """
            ),
            {"ids": ids},
        )
        per_unit = {row.generation_unit_id: row for row in rs}

        units_with_data = []
        units_no_data = []
        for r, u in inactive_matched:
            if u.id in per_unit:
                units_with_data.append((r, u, per_unit[u.id]))
            else:
                units_no_data.append((r, u))
        print(f"  inactive units WITH generation_data: {len(units_with_data)}")
        print(f"  inactive units with NO generation_data: {len(units_no_data)}")

        if not units_with_data:
            print("\nNo inactive units have generation_data linked. No impact.")
            return

        # --- 4. Per-source / per-windfarm impact summary
        print("\n[4] Per-source impact summary:")
        by_source = defaultdict(lambda: {"units": 0, "rows": 0, "gen_mwh": 0.0})
        for r, u, d in units_with_data:
            s = u.source
            by_source[s]["units"] += 1
            by_source[s]["rows"] += d.rows
            by_source[s]["gen_mwh"] += float(d.sum_gen or 0)
        print(
            f"  {'source':<10}{'units':>8}{'rows':>12}{'sum_gen_mwh':>16}"
        )
        for s, v in sorted(by_source.items()):
            print(f"  {s:<10}{v['units']:>8}{v['rows']:>12,}{v['gen_mwh']:>16,.0f}")

        # --- 5. Per-windfarm impact (top 30 by row count)
        print("\n[5] Per-windfarm impact (top 30 by linked row count):")
        by_wf = defaultdict(lambda: {"units": 0, "rows": 0, "gen_mwh": 0.0, "src": set()})
        for r, u, d in units_with_data:
            wf = r["windfarm_name"]
            by_wf[wf]["units"] += 1
            by_wf[wf]["rows"] += d.rows
            by_wf[wf]["gen_mwh"] += float(d.sum_gen or 0)
            by_wf[wf]["src"].add(u.source)
        wf_sorted = sorted(by_wf.items(), key=lambda kv: -kv[1]["rows"])
        print(
            f"  {'windfarm':<32}{'src':<10}{'units':>6}{'rows':>10}{'sum_gen_mwh':>14}"
        )
        for wf, v in wf_sorted[:30]:
            srcs = ",".join(sorted(v["src"]))
            print(
                f"  {wf[:30]:<32}{srcs:<10}{v['units']:>6}{v['rows']:>10,}{v['gen_mwh']:>14,.0f}"
            )

        # --- 6. Sample 20 inactive-unit rows with details
        print("\n[6] Sample of 20 inactive units that have generation_data:")
        print(
            f"  {'id':>7}{'src':<8}{'code':<14}{'name':<32}{'cap':>6}{'rows':>9}{'first':>22}"
        )
        for r, u, d in units_with_data[:20]:
            code = (u.code or "")[:12]
            name = (u.name or "")[:30]
            cap = u.cap if u.cap is not None else 0.0
            print(
                f"  {u.id:>7}{u.source:<8}{code:<14}{name:<32}"
                f"{cap:>6.1f}{d.rows:>9,}{str(d.first_hr)[:19]:>22}"
            )

        # --- 7. Double-counting risk: hours where BOTH an inactive unit AND
        #         any other (likely active) unit on the same windfarm have rows.
        #         If yes — and our recently-fixed sums use windfarm_id — these
        #         contributions add up.
        print("\n[7] Double-count risk: shared hours with other units on same windfarm:")
        rs = await db.execute(
            text(
                """
                WITH inactive_rows AS (
                    SELECT gd.generation_unit_id, gd.hour, gd.windfarm_id, gd.source,
                           gd.generation_mwh::float AS gen
                    FROM generation_data gd
                    WHERE gd.generation_unit_id = ANY(:ids)
                ),
                companions AS (
                    SELECT ir.generation_unit_id AS inactive_id,
                           COUNT(*) AS shared_hours,
                           SUM(ir.gen) AS inactive_gen_in_shared,
                           SUM(other.generation_mwh)::float AS other_gen_in_shared
                    FROM inactive_rows ir
                    JOIN generation_data other
                      ON other.hour = ir.hour
                     AND other.windfarm_id = ir.windfarm_id
                     AND other.source = ir.source
                     AND other.generation_unit_id != ir.generation_unit_id
                    GROUP BY 1
                )
                SELECT inactive_id, shared_hours,
                       inactive_gen_in_shared, other_gen_in_shared
                FROM companions
                ORDER BY shared_hours DESC
                LIMIT 30
                """
            ),
            {"ids": ids},
        )
        rows = list(rs)
        if not rows:
            print("  No inactive-unit rows share an (hour, windfarm_id, source) with another unit.")
            print("  → Inactive rows are NOT double-counted in any aggregation that filters by hour+windfarm.")
        else:
            print(
                f"  {'unit_id':>8}{'shared_hrs':>12}{'inactive_gen':>16}{'other_gen':>16}"
            )
            for r in rows:
                print(
                    f"  {r.inactive_id:>8}{r.shared_hours:>12,}"
                    f"{(r.inactive_gen_in_shared or 0):>16,.0f}"
                    f"{(r.other_gen_in_shared or 0):>16,.0f}"
                )

        # --- 8. Are inactive-unit rows being summed in windfarm-level aggregations?
        #         (i.e. do they have windfarm_id set?). Aggregate gen contribution.
        print("\n[8] Aggregate gen-contribution if windfarm-level sum includes them:")
        rs = await db.execute(
            text(
                """
                SELECT gd.windfarm_id,
                       wf.name AS wf_name,
                       COUNT(*) AS rows,
                       SUM(gd.generation_mwh)::float AS sum_gen,
                       MIN(gd.hour) AS first_hr,
                       MAX(gd.hour) AS last_hr
                FROM generation_data gd
                LEFT JOIN windfarms wf ON wf.id = gd.windfarm_id
                WHERE gd.generation_unit_id = ANY(:ids)
                  AND gd.windfarm_id IS NOT NULL
                GROUP BY gd.windfarm_id, wf.name
                ORDER BY rows DESC
                LIMIT 30
                """
            ),
            {"ids": ids},
        )
        rows = list(rs)
        if not rows:
            print("  No inactive-unit rows have windfarm_id set — they don't contribute to any windfarm-level sum.")
        else:
            total_rows = sum(r.rows for r in rows)
            total_gen = sum(float(r.sum_gen or 0) for r in rows)
            print(
                f"  {'wf_id':>7}{'wf_name':<28}{'rows':>10}{'sum_gen_mwh':>14}{'first':>22}"
            )
            for r in rows:
                name = (r.wf_name or "<NULL>")[:26]
                print(
                    f"  {r.windfarm_id:>7}{name:<28}{r.rows:>10,}"
                    f"{(r.sum_gen or 0):>14,.0f}{str(r.first_hr)[:19]:>22}"
                )
            print(f"\n  TOTAL across listed: rows={total_rows:,}, gen={total_gen:,.0f} MWh")

        # --- 9. Orphan rows (windfarm_id IS NULL on inactive-unit rows).
        rs = await db.execute(
            text(
                """
                SELECT COUNT(*) AS n,
                       SUM(generation_mwh)::float AS sum_gen
                FROM generation_data
                WHERE generation_unit_id = ANY(:ids)
                  AND windfarm_id IS NULL
                """
            ),
            {"ids": ids},
        )
        r = rs.first()
        print(f"\n[9] Inactive-unit rows with windfarm_id=NULL (truly orphan):")
        print(f"  rows={r.n:,}  gen={(r.sum_gen or 0):,.0f} MWh")

        # --- 10. FK references that block deletion (we don't delete here, just enumerate)
        print("\n[10] Referencing rows by table (would need cleanup before any deletion):")
        for tbl, col in [
            ("generation_data", "generation_unit_id"),
            ("data_anomalies", "generation_unit_id"),
            ("generation_unit_mapping", "generation_unit_id"),
        ]:
            rs = await db.execute(
                text(f"SELECT COUNT(*) AS n FROM {tbl} WHERE {col} = ANY(:ids)"),
                {"ids": ids},
            )
            print(f"  {tbl}: {rs.scalar():,}")


asyncio.run(main())
