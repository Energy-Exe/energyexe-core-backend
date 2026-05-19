"""Follow-up: for each windfarm impacted by inactive-unit rows, quantify
whether the inactive contribution is DUPLICATING active-unit data (the bad
case — reports inflate) or filling otherwise-empty hours (less bad).

For each affected windfarm + source, compute:
  - rows_inactive          : rows attributed to one of our inactive units
  - rows_active            : rows attributed to an active unit on same windfarm
  - shared_hours           : hours where both inactive AND active rows exist
  - inactive_only_hours    : hours covered only by inactive rows (not in active)
  - sum_gen_inactive       : total gen from inactive-unit rows
  - sum_gen_active         : total gen from active-unit rows
  - sum_gen_in_shared_inactive  : inactive gen during shared hours
  - sum_gen_in_shared_active    : active gen during shared hours

Interpretation:
  - shared_hours == rows_inactive AND inactive≈active  → pure duplication
  - inactive_only_hours == rows_inactive               → unique data; deletion would lose it
  - mixed                                              → partial overlap

Run:
    poetry run python scripts/fixes/investigate_inactive_units_overlap.py
"""
import asyncio
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory

CSV_PATH = "/Users/mdfaisal/Downloads/inactive_generation_units_with_source.csv"


async def main():
    # Load CSV
    csv_rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            csv_rows.append(r)

    S = get_session_factory()
    async with S() as db:
        # Match CSV rows → DB inactive unit ids
        inactive_ids = []
        for r in csv_rows:
            rs = await db.execute(
                text(
                    """
                    SELECT id FROM generation_units
                    WHERE source = :src AND code = :code AND name = :name
                      AND is_active = false
                    """
                ),
                {"src": r["source"], "code": r["code"], "name": r["name"]},
            )
            inactive_ids.extend([row.id for row in rs])
        print(f"Inactive unit ids matched: {len(inactive_ids)}")

        # Per (windfarm_id, source) impact analysis
        # Active units = generation_units.is_active=True attributed to same windfarm.
        rs = await db.execute(
            text(
                """
                WITH inactive_data AS (
                    SELECT gd.windfarm_id, gd.source, gd.hour,
                           SUM(gd.generation_mwh)::float AS gen
                    FROM generation_data gd
                    WHERE gd.generation_unit_id = ANY(:ids)
                    GROUP BY gd.windfarm_id, gd.source, gd.hour
                ),
                active_data AS (
                    SELECT gd.windfarm_id, gd.source, gd.hour,
                           SUM(gd.generation_mwh)::float AS gen
                    FROM generation_data gd
                    JOIN generation_units gu ON gu.id = gd.generation_unit_id
                    WHERE gu.is_active = true
                    GROUP BY gd.windfarm_id, gd.source, gd.hour
                ),
                joined AS (
                    SELECT
                      COALESCE(i.windfarm_id, a.windfarm_id) AS wf_id,
                      COALESCE(i.source,      a.source)      AS src,
                      COALESCE(i.hour,        a.hour)        AS hr,
                      i.gen AS i_gen,
                      a.gen AS a_gen
                    FROM inactive_data i
                    FULL OUTER JOIN active_data a USING (windfarm_id, source, hour)
                ),
                per_wf AS (
                    SELECT
                      wf_id, src,
                      SUM(CASE WHEN i_gen IS NOT NULL THEN 1 ELSE 0 END) AS rows_inactive,
                      SUM(CASE WHEN a_gen IS NOT NULL THEN 1 ELSE 0 END) AS rows_active,
                      SUM(CASE WHEN i_gen IS NOT NULL AND a_gen IS NOT NULL THEN 1 ELSE 0 END) AS shared_hours,
                      SUM(CASE WHEN i_gen IS NOT NULL AND a_gen IS NULL THEN 1 ELSE 0 END) AS inactive_only_hours,
                      SUM(CASE WHEN i_gen IS NULL AND a_gen IS NOT NULL THEN 1 ELSE 0 END) AS active_only_hours,
                      SUM(COALESCE(i_gen, 0)) AS sum_inactive,
                      SUM(COALESCE(a_gen, 0)) AS sum_active,
                      SUM(CASE WHEN i_gen IS NOT NULL AND a_gen IS NOT NULL THEN i_gen ELSE 0 END) AS sum_inactive_in_shared,
                      SUM(CASE WHEN i_gen IS NOT NULL AND a_gen IS NOT NULL THEN a_gen ELSE 0 END) AS sum_active_in_shared
                    FROM joined
                    WHERE wf_id IN (
                       SELECT DISTINCT windfarm_id FROM generation_data
                       WHERE generation_unit_id = ANY(:ids) AND windfarm_id IS NOT NULL
                    )
                    GROUP BY wf_id, src
                )
                SELECT p.*, wf.name AS wf_name
                FROM per_wf p
                LEFT JOIN windfarms wf ON wf.id = p.wf_id
                WHERE rows_inactive > 0
                ORDER BY rows_inactive DESC
                """
            ),
            {"ids": inactive_ids},
        )
        rows = list(rs)

        print(f"\nAffected (windfarm, source) groups: {len(rows)}")
        print(f"\n{'wf_name':<26}{'src':<8}{'i_rows':>9}{'a_rows':>9}"
              f"{'shared':>9}{'i_only':>9}{'i_gen':>14}{'a_gen':>14}"
              f"{'i_in_shared':>14}{'a_in_shared':>14}")
        total_i_rows = 0
        total_a_rows = 0
        total_shared = 0
        total_inactive_only = 0
        total_i_gen = 0.0
        total_a_gen = 0.0
        total_i_in_shared = 0.0
        total_a_in_shared = 0.0

        category_dup = []   # shared_hours / rows_inactive > 0.95 AND magnitudes similar
        category_unique = []  # inactive_only_hours / rows_inactive > 0.95
        category_mixed = []

        for r in rows:
            name = (r.wf_name or "<NULL>")[:24]
            print(
                f"{name:<26}{r.src:<8}"
                f"{r.rows_inactive:>9,}{r.rows_active:>9,}"
                f"{r.shared_hours:>9,}{r.inactive_only_hours:>9,}"
                f"{(r.sum_inactive or 0):>14,.0f}{(r.sum_active or 0):>14,.0f}"
                f"{(r.sum_inactive_in_shared or 0):>14,.0f}"
                f"{(r.sum_active_in_shared or 0):>14,.0f}"
            )
            total_i_rows += r.rows_inactive
            total_a_rows += r.rows_active
            total_shared += r.shared_hours
            total_inactive_only += r.inactive_only_hours
            total_i_gen += float(r.sum_inactive or 0)
            total_a_gen += float(r.sum_active or 0)
            total_i_in_shared += float(r.sum_inactive_in_shared or 0)
            total_a_in_shared += float(r.sum_active_in_shared or 0)

            shared_ratio = r.shared_hours / max(r.rows_inactive, 1)
            unique_ratio = r.inactive_only_hours / max(r.rows_inactive, 1)
            if shared_ratio >= 0.95:
                category_dup.append((r.wf_name, r.src, r.rows_inactive, r.sum_inactive))
            elif unique_ratio >= 0.95:
                category_unique.append((r.wf_name, r.src, r.rows_inactive, r.sum_inactive))
            else:
                category_mixed.append((r.wf_name, r.src, r.rows_inactive, r.sum_inactive,
                                       shared_ratio, unique_ratio))

        print(f"\n--- TOTALS ---")
        print(f"  inactive rows:        {total_i_rows:>14,}")
        print(f"  active   rows:        {total_a_rows:>14,}")
        print(f"  shared hours:         {total_shared:>14,}  (BOTH inactive and active have a row)")
        print(f"  inactive_only hours:  {total_inactive_only:>14,}  (only inactive — would be lost if deleted)")
        print(f"  inactive gen:         {total_i_gen:>14,.0f} MWh")
        print(f"  active gen:           {total_a_gen:>14,.0f} MWh")
        print(f"  inactive gen in shared:  {total_i_in_shared:>11,.0f} MWh  (likely duplicating active)")
        print(f"  active gen in shared:    {total_a_in_shared:>11,.0f} MWh")

        print(f"\n--- CATEGORIES ---")
        print(f"  Pure duplicate (≥95% rows shared with active): {len(category_dup)}")
        for wf, src, rows_, gen_ in category_dup[:20]:
            print(f"    {wf!r} ({src}): {rows_:,} rows, {gen_:,.0f} MWh — DELETING IS SAFE (active data still there)")

        print(f"\n  Unique-only (≥95% rows have NO active counterpart): {len(category_unique)}")
        for wf, src, rows_, gen_ in category_unique[:20]:
            print(f"    {wf!r} ({src}): {rows_:,} rows, {gen_:,.0f} MWh — DELETING WOULD LOSE THIS DATA")

        print(f"\n  Mixed: {len(category_mixed)}")
        for wf, src, rows_, gen_, sr, ur in category_mixed[:30]:
            print(f"    {wf!r} ({src}): {rows_:,} rows, {gen_:,.0f} MWh, shared={sr:.0%}, only={ur:.0%}")


asyncio.run(main())
