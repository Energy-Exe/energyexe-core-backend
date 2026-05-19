"""Fix 8 mislinked ENTSOE generation_units that point to the wrong windfarm.

Background:
  A 2025-09-18 batch into generation_unit_mapping set wrong windfarm_id for
  8 ENTSOE units. The error propagated to generation_units.windfarm_id and
  every generation_data row those units produced (windfarm_id is copied on
  aggregate). End-user impact: ~6.4 GWh of historical generation appears on
  the wrong windfarm's charts.

  Confirmed via raw-data trace and per-hour cross-source match (Galloper
  ENTSOE values ≈ ELEXON values per unit within 1-5%) — the data is real,
  only the pointer is wrong. The fix is to re-attach the unit (and its
  generation_data rows) to the correct windfarm.

Plan:
  Unit                              wrong wf -> correct wf   gen_data rows
  12385 Ormonde Eng Ltd             7385     -> 7404         41,366
  12328 ABRB0-1                     7359     -> 7350         13,221
  12361 Hornsea 1                   7380     -> 7384          9,646
  12346 East Anglia One             7370     -> 7371          9,742
  12348 Galloper GAOFO-1            7374     -> 7373          2,519
  12349 Galloper GAOFO-2            7374     -> 7373          2,519
  12350 Galloper GAOFO-3            7374     -> 7373          2,519
  12351 Galloper GAOFO-4            7374     -> 7373          2,519
                                                       total 84,051

  Plus: clear 346 stale performance_summaries on the 5 victim windfarms so
  the cached values regenerate from de-contaminated input.

Run:
    poetry run python scripts/fixes/fix_mislinked_entsoe_units.py             # dry-run (default)
    poetry run python scripts/fixes/fix_mislinked_entsoe_units.py --execute   # commit
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


# (unit_id, unit_name, wrong_wf, correct_wf, expected_rows)
FIXES = [
    (12385, "Ormonde Eng Ltd",                    7385, 7404, 41_366),
    (12328, "ABRB0-1",                            7359, 7350, 13_221),
    (12361, "Hornsea 1",                          7380, 7384,  9_646),
    (12346, "East Anglia One",                    7370, 7371,  9_742),
    (12348, "Galloper Offshore Wind Farm GAOFO-1", 7374, 7373,  2_519),
    (12349, "Galloper Offshore Wind Farm GAOFO-2", 7374, 7373,  2_519),
    (12350, "Galloper Offshore Wind Farm GAOFO-3", 7374, 7373,  2_519),
    (12351, "Galloper Offshore Wind Farm GAOFO-4", 7374, 7373,  2_519),
]

VICTIM_WF_IDS = sorted({wrong for _, _, wrong, _, _ in FIXES})
CORRECT_WF_IDS = sorted({right for _, _, _, right, _ in FIXES})


def banner(title: str):
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)


async def precheck(db) -> bool:
    """Verify current state matches expectations. Abort on mismatch."""
    banner("PRE-CHECK — verify current DB state matches the plan")
    ok = True

    # 1. Each unit must exist, be ENTSOE, and currently point to the wrong wf
    #    OR to NULL (a prior partial-fix attempt left mapping + gen_data wrong
    #    but cleared unit.windfarm_id). Both states are pre-fix; we resolve
    #    both to the correct wf.
    for uid, uname, wrong, right, exp_rows in FIXES:
        rs = await db.execute(text("""
            SELECT id, name, source, windfarm_id, is_active
            FROM generation_units WHERE id = :u
        """), {"u": uid})
        u = rs.first()
        if u is None:
            print(f"  [FAIL] unit {uid} not found")
            ok = False
            continue
        if u.source != "ENTSOE":
            print(f"  [FAIL] unit {uid} source={u.source} (expected ENTSOE)")
            ok = False
        if u.windfarm_id == right:
            print(f"  [SKIP] unit {uid:>5} '{u.name[:36]}' already at correct wf {right}")
        elif u.windfarm_id in (wrong, None):
            current = u.windfarm_id if u.windfarm_id is not None else "NULL"
            print(f"  [ OK ] unit {uid:>5} '{u.name[:36]}' wf={current} → will set to {right}")
        else:
            print(f"  [FAIL] unit {uid} current wf={u.windfarm_id} "
                  f"(expected wrong wf {wrong} or NULL)")
            ok = False

    # 2. Each mapping row must exist with the wrong wf.
    for uid, _, wrong, _, _ in FIXES:
        rs = await db.execute(text("""
            SELECT id, windfarm_id FROM generation_unit_mapping
            WHERE generation_unit_id = :u
        """), {"u": uid})
        rows = list(rs)
        if len(rows) != 1:
            print(f"  [FAIL] mapping count for unit {uid} = {len(rows)} (expected 1)")
            ok = False
            continue
        if rows[0].windfarm_id != wrong:
            print(f"  [FAIL] mapping for unit {uid} wf={rows[0].windfarm_id} (expected {wrong})")
            ok = False

    # 3. generation_data row counts per unit/wrong wf.
    for uid, _, wrong, _, exp_rows in FIXES:
        rs = await db.execute(text("""
            SELECT COUNT(*) AS n FROM generation_data
            WHERE generation_unit_id = :u AND windfarm_id = :w
        """), {"u": uid, "w": wrong})
        n = rs.scalar()
        # Drift tolerance: backend daily aggregation may have nudged counts a
        # little since the audit. Warn if drift > 5% or > 100 rows.
        drift = abs(n - exp_rows)
        if drift > max(100, exp_rows * 0.05):
            print(f"  [WARN] unit {uid} gen_data rows={n:,} (expected {exp_rows:,}, drift {drift:,})")
        else:
            print(f"  [ OK ] unit {uid} gen_data rows={n:,} (expected ~{exp_rows:,})")

    # 4. Target windfarms must exist.
    for wf_id in CORRECT_WF_IDS:
        rs = await db.execute(text("""
            SELECT id, name FROM windfarms WHERE id = :w
        """), {"w": wf_id})
        w = rs.first()
        if w is None:
            print(f"  [FAIL] target windfarm {wf_id} not found")
            ok = False
        else:
            print(f"  [ OK ] target wf {wf_id} '{w.name}' exists")

    # 5. No ENTSOE rows on target windfarms (would indicate a prior overlap).
    for wf_id in CORRECT_WF_IDS:
        rs = await db.execute(text("""
            SELECT COUNT(*) AS n FROM generation_data
            WHERE windfarm_id = :w AND source = 'ENTSOE'
        """), {"w": wf_id})
        n = rs.scalar()
        if n > 0:
            print(f"  [WARN] target wf {wf_id} already has {n:,} ENTSOE rows — "
                  "verify no duplicate hours before commit")
        else:
            print(f"  [ OK ] target wf {wf_id} has no pre-existing ENTSOE rows")

    return ok


async def run_fix(db, execute: bool) -> dict:
    """Run the 4-step transaction. Returns rowcounts per step."""
    banner("APPLY FIX" + (" (EXECUTING)" if execute else " (DRY-RUN — rolled back at end)"))

    counts = {"mapping": 0, "units": 0, "gen_data": 0, "perf_summaries": 0}

    # Step 1 — generation_unit_mapping
    for uid, _, _, right, _ in FIXES:
        rs = await db.execute(text("""
            UPDATE generation_unit_mapping
               SET windfarm_id = :w, updated_at = now()
             WHERE generation_unit_id = :u
        """), {"u": uid, "w": right})
        counts["mapping"] += rs.rowcount

    # Step 2 — generation_units
    for uid, _, _, right, _ in FIXES:
        rs = await db.execute(text("""
            UPDATE generation_units
               SET windfarm_id = :w, updated_at = now()
             WHERE id = :u
        """), {"u": uid, "w": right})
        counts["units"] += rs.rowcount

    # Step 3 — generation_data (the big one)
    for uid, _, wrong, right, _ in FIXES:
        rs = await db.execute(text("""
            UPDATE generation_data
               SET windfarm_id = :w, updated_at = now()
             WHERE generation_unit_id = :u
               AND windfarm_id = :wrong
        """), {"u": uid, "w": right, "wrong": wrong})
        counts["gen_data"] += rs.rowcount

    # Step 4 — clear stale performance_summaries on victim windfarms
    rs = await db.execute(text("""
        DELETE FROM performance_summaries WHERE windfarm_id = ANY(:wfs)
    """), {"wfs": VICTIM_WF_IDS})
    counts["perf_summaries"] = rs.rowcount

    print(f"  rows affected:")
    print(f"    generation_unit_mapping:    {counts['mapping']:>8,}")
    print(f"    generation_units:           {counts['units']:>8,}")
    print(f"    generation_data:            {counts['gen_data']:>8,}")
    print(f"    performance_summaries (del):{counts['perf_summaries']:>8,}")

    return counts


async def postcheck(db) -> bool:
    """Verify the fix produced the expected state."""
    banner("POST-CHECK — verify final state")
    ok = True

    # A. Every unit / mapping / sample gen_data row now points to the correct wf.
    for uid, uname, _, right, _ in FIXES:
        rs = await db.execute(text("""
            SELECT
              (SELECT windfarm_id FROM generation_units WHERE id = :u) AS unit_wf,
              (SELECT windfarm_id FROM generation_unit_mapping WHERE generation_unit_id = :u LIMIT 1) AS map_wf,
              (SELECT windfarm_id FROM generation_data WHERE generation_unit_id = :u LIMIT 1) AS gd_wf,
              (SELECT COUNT(*) FROM generation_data
                WHERE generation_unit_id = :u AND windfarm_id = :right) AS rows_correct,
              (SELECT COUNT(*) FROM generation_data
                WHERE generation_unit_id = :u AND windfarm_id != :right) AS rows_wrong
        """), {"u": uid, "right": right})
        r = rs.first()
        verdict = (
            r.unit_wf == right
            and r.map_wf == right
            and (r.gd_wf == right or r.gd_wf is None)
            and r.rows_wrong == 0
        )
        mark = "[ OK ]" if verdict else "[FAIL]"
        print(f"  {mark} unit {uid:>5} '{uname[:34]}': "
              f"unit_wf={r.unit_wf} map_wf={r.map_wf} sample_gd_wf={r.gd_wf} "
              f"rows@correct={r.rows_correct:,} rows@wrong={r.rows_wrong:,}")
        if not verdict:
            ok = False

    # B. Hornsea 2 pre-2022 must be empty (was 41,366 rows of Ormonde data).
    rs = await db.execute(text("""
        SELECT COUNT(*) AS n FROM generation_data
        WHERE windfarm_id = 7385 AND hour < '2022-01-01'
    """))
    n = rs.scalar()
    mark = "[ OK ]" if n == 0 else "[FAIL]"
    print(f"\n  {mark} Hornsea 2 (7385) pre-2022 rows = {n} (expected 0)")
    if n != 0:
        ok = False

    # C. Ormonde now carries both ELEXON and ENTSOE.
    rs = await db.execute(text("""
        SELECT source, COUNT(*) AS rows
        FROM generation_data WHERE windfarm_id = 7404
        GROUP BY source ORDER BY source
    """))
    src_rows = {r.source: r.rows for r in rs}
    has_both = "ELEXON" in src_rows and "ENTSOE" in src_rows and src_rows["ENTSOE"] >= 40_000
    mark = "[ OK ]" if has_both else "[FAIL]"
    print(f"  {mark} Ormonde (7404) sources: {src_rows}")
    if not has_both:
        ok = False

    # D. Galloper now carries both ELEXON and 4× ENTSOE units.
    rs = await db.execute(text("""
        SELECT source, COUNT(DISTINCT generation_unit_id) AS units, COUNT(*) AS rows
        FROM generation_data WHERE windfarm_id = 7373
        GROUP BY source ORDER BY source
    """))
    rows = list(rs)
    print(f"  Galloper (7373) sources:")
    for r in rows:
        print(f"    {r.source}: {r.units} units, {r.rows:,} rows")

    return ok


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true",
                        help="Actually commit the changes (default: dry-run)")
    parser.add_argument("--skip-precheck", action="store_true",
                        help="Skip pre-checks (NOT recommended)")
    args = parser.parse_args()

    S = get_session_factory()
    async with S() as db:
        if not args.skip_precheck:
            ok = await precheck(db)
            if not ok:
                print("\n*** PRE-CHECK FAILED — aborting without changes ***")
                return 1

        await run_fix(db, execute=args.execute)
        await postcheck(db)

        if args.execute:
            await db.commit()
            print("\n*** COMMITTED ***")
            # Run postcheck again after commit, against a fresh transaction,
            # to make sure the changes really stuck.
            async with S() as db2:
                banner("POST-COMMIT VERIFICATION (fresh session)")
                ok = await postcheck(db2)
                if not ok:
                    print("\n!!! POST-COMMIT VERIFICATION FAILED — investigate immediately !!!")
                    return 2
        else:
            await db.rollback()
            print("\n*** DRY-RUN — rolled back. Re-run with --execute to commit. ***")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)
