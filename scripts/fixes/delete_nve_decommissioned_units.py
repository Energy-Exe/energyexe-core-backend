"""Delete 3 NVE decommissioned-farm units that hold orphan generation data.

These are old Norwegian windfarms NVE has historical data for, but which were
never created as `windfarms` rows in our DB. Their unit rows hold ~329k rows
of generation_data with `windfarm_id = NULL`, so the data doesn't surface on
any chart — but takes up DB space and clouds future audits.

Units to delete:
  id=12797  Fjeldskår           NVE code=1   2002-01-01 → 2018-04-02  142,464 rows
  id=12801  Kvalnes             NVE code=23  2009-06-12 → 2018-02-24   76,308 rows
  id=12802  Hovden Vesterålen   NVE code=24  2003-03-03 → 2015-09-20  110,030 rows

Tables touched:
  generation_data      DELETE rows                             ~328,802
  generation_units     DELETE rows                                    3

Tables NOT touched:
  generation_data_raw  retained (~348,144 rows under NVE identifiers '1','23','24')
                       so a future seed can resurrect the data if needed.
  generation_unit_mapping  no rows reference these units (verified).
  data_anomalies       verified 0 rows.
  performance_summaries  no rows (no windfarm record exists for these).

Run:
    poetry run python scripts/fixes/delete_nve_decommissioned_units.py            # dry-run
    poetry run python scripts/fixes/delete_nve_decommissioned_units.py --execute  # commit
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


# (unit_id, name, code, expected_gen_data_rows)
UNITS = [
    (12797, "Fjeldskår",         "1",  142_464),
    (12801, "Kvalnes",           "23",  76_308),
    (12802, "Hovden Vesterålen", "24", 110_030),
]
UNIT_IDS = [u for u, *_ in UNITS]


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def precheck(db) -> bool:
    """Confirm DB state matches plan before doing anything."""
    banner("PRE-CHECK")
    ok = True

    # 1. Each unit must exist, be NVE, inactive, and have windfarm_id=NULL.
    for uid, uname, code, exp_rows in UNITS:
        rs = await db.execute(text("""
            SELECT id, name, code, source, is_active, windfarm_id
            FROM generation_units WHERE id = :u
        """), {"u": uid})
        u = rs.first()
        if u is None:
            print(f"  [FAIL] unit {uid} not found")
            ok = False
            continue
        if u.source != "NVE":
            print(f"  [FAIL] unit {uid} source={u.source} (expected NVE)")
            ok = False
        if u.is_active:
            print(f"  [FAIL] unit {uid} is_active=True (expected False)")
            ok = False
        if u.windfarm_id is not None:
            print(f"  [FAIL] unit {uid} has windfarm_id={u.windfarm_id} (expected NULL)")
            ok = False
        if u.code != code:
            print(f"  [FAIL] unit {uid} code={u.code} (expected {code})")
            ok = False
        else:
            print(f"  [ OK ] unit {uid} '{u.name}' code={u.code} active={u.is_active} wf={u.windfarm_id}")

    # 2. gen_data row counts roughly match expectation.
    for uid, _, _, exp_rows in UNITS:
        rs = await db.execute(text("""
            SELECT COUNT(*) AS n, COUNT(DISTINCT windfarm_id) AS distinct_wfs,
                   BOOL_OR(windfarm_id IS NOT NULL) AS any_non_null_wf
            FROM generation_data WHERE generation_unit_id = :u
        """), {"u": uid})
        r = rs.first()
        drift = abs(r.n - exp_rows)
        rows_ok = drift <= max(100, exp_rows * 0.05)
        wf_ok = not r.any_non_null_wf  # all rows should have wf_id=NULL
        if not wf_ok:
            print(f"  [FAIL] unit {uid} has {r.distinct_wfs} distinct wf ids among rows "
                  f"(expected all NULL) — would be unsafe to delete blindly")
            ok = False
        elif not rows_ok:
            print(f"  [WARN] unit {uid} gen_data rows={r.n:,} (expected ~{exp_rows:,}, drift {drift:,})")
        else:
            print(f"  [ OK ] unit {uid} gen_data rows={r.n:,} all with windfarm_id=NULL")

    # 3. Confirm there's no FK reference anywhere we'd miss.
    for tbl, col in [
        ("generation_unit_mapping", "generation_unit_id"),
        ("data_anomalies", "generation_unit_id"),
    ]:
        rs = await db.execute(
            text(f"SELECT COUNT(*) AS n FROM {tbl} WHERE {col} = ANY(:ids)"),
            {"ids": UNIT_IDS},
        )
        n = rs.scalar()
        if n != 0:
            print(f"  [FAIL] {tbl}.{col} has {n} rows referencing our units — would fail FK")
            ok = False
        else:
            print(f"  [ OK ] {tbl}: no referencing rows")

    return ok


async def run_delete(db) -> dict:
    counts = {"generation_data": 0, "generation_units": 0}
    banner("DELETE")

    # Step 1 — generation_data
    rs = await db.execute(text("""
        DELETE FROM generation_data WHERE generation_unit_id = ANY(:ids)
    """), {"ids": UNIT_IDS})
    counts["generation_data"] = rs.rowcount

    # Step 2 — generation_units
    rs = await db.execute(text("""
        DELETE FROM generation_units WHERE id = ANY(:ids)
    """), {"ids": UNIT_IDS})
    counts["generation_units"] = rs.rowcount

    print(f"  generation_data:  {counts['generation_data']:>8,} rows deleted")
    print(f"  generation_units: {counts['generation_units']:>8,} rows deleted")
    return counts


async def postcheck(db) -> bool:
    banner("POST-CHECK")
    ok = True
    rs = await db.execute(text("""
        SELECT COUNT(*) AS n FROM generation_units WHERE id = ANY(:ids)
    """), {"ids": UNIT_IDS})
    n = rs.scalar()
    print(f"  generation_units remaining for our 3 ids: {n} (expected 0)")
    if n != 0:
        ok = False
    rs = await db.execute(text("""
        SELECT COUNT(*) AS n FROM generation_data WHERE generation_unit_id = ANY(:ids)
    """), {"ids": UNIT_IDS})
    n = rs.scalar()
    print(f"  generation_data remaining for our 3 unit ids: {n} (expected 0)")
    if n != 0:
        ok = False

    # Raw should still exist
    rs = await db.execute(text("""
        SELECT identifier, COUNT(*) AS n FROM generation_data_raw
        WHERE source='NVE' AND identifier IN ('1','23','24')
        GROUP BY 1 ORDER BY 1
    """))
    print(f"\n  Raw preservation (not deleted):")
    for r in rs:
        print(f"    identifier={r.identifier}: {r.n:,} raw rows retained")
    return ok


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true", help="Actually commit deletions")
    args = parser.parse_args()

    S = get_session_factory()
    async with S() as db:
        ok = await precheck(db)
        if not ok:
            print("\n*** PRE-CHECK FAILED — aborting without changes ***")
            return 1

        await run_delete(db)
        await postcheck(db)

        if args.execute:
            await db.commit()
            print("\n*** COMMITTED ***")
        else:
            await db.rollback()
            print("\n*** DRY-RUN — rolled back. Re-run with --execute to commit. ***")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)
