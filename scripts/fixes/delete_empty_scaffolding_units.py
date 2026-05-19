"""Delete 'empty scaffolding' inactive generation units (Bucket B from the
inactive-units audit).

Candidate definition — each unit must satisfy ALL of:
  - is_active = FALSE
  - Zero rows in generation_data referencing the unit
  - Zero rows in data_anomalies referencing the unit
  - Zero rows in generation_unit_mapping referencing the unit
  - Not in the previously-handled list (mislinks, RCBKO, junk, NVE Cat D)

Expected: 299 units (NVE 210, ENERGISTYRELSEN 56, ENTSOE 31, EIA 1, ELEXON 1).

The 2 units excluded by the active-mapping rule (12367 Humber Gateway,
12369 Hywind) are left alone for separate investigation.

Tables touched:
  generation_units    DELETE rows                                ~299

Tables NOT touched: everything else.

Run:
    poetry run python scripts/fixes/delete_empty_scaffolding_units.py            # dry-run
    poetry run python scripts/fixes/delete_empty_scaffolding_units.py --execute  # commit
"""
import argparse
import asyncio
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


HANDLED = {12385, 12328, 12361, 12346, 12348, 12349, 12350, 12351,
           12806, 12388, 12389, 12797, 12801, 12802}


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def select_candidates(db) -> list[int]:
    """Return ids of units that satisfy ALL safety criteria."""
    rs = await db.execute(text("""
        SELECT gu.id, gu.source, gu.name
        FROM generation_units gu
        WHERE gu.is_active = FALSE
          AND gu.id <> ALL(:handled)
          AND NOT EXISTS (SELECT 1 FROM generation_data       gd  WHERE gd.generation_unit_id  = gu.id)
          AND NOT EXISTS (SELECT 1 FROM data_anomalies        da  WHERE da.generation_unit_id  = gu.id)
          AND NOT EXISTS (SELECT 1 FROM generation_unit_mapping gum WHERE gum.generation_unit_id = gu.id)
        ORDER BY gu.id
    """), {"handled": list(HANDLED)})
    return list(rs)


async def precheck(db) -> tuple[bool, list]:
    banner("PRE-CHECK — selecting safe-to-delete candidates")
    cands = await select_candidates(db)
    print(f"  Candidates passing all safety filters: {len(cands)}")

    by_src = defaultdict(int)
    for c in cands:
        by_src[c.source] += 1
    print(f"\n  By source:")
    for k, v in sorted(by_src.items()):
        print(f"    {k}: {v}")

    # Re-verify no FK refs (paranoid double-check after candidate selection)
    cand_ids = [c.id for c in cands]
    if not cand_ids:
        print("  Nothing to do.")
        return False, []

    for tbl, col in [
        ("generation_data", "generation_unit_id"),
        ("data_anomalies", "generation_unit_id"),
        ("generation_unit_mapping", "generation_unit_id"),
    ]:
        rs = await db.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} = ANY(:ids)"),
            {"ids": cand_ids},
        )
        n = rs.scalar()
        if n != 0:
            print(f"  [FAIL] {tbl}.{col} unexpectedly has {n} rows for candidates")
            return False, []
        print(f"  [ OK ] {tbl}: 0 refs")

    return True, cands


async def run_delete(db, ids: list[int]) -> int:
    banner("DELETE")
    rs = await db.execute(
        text("DELETE FROM generation_units WHERE id = ANY(:ids)"),
        {"ids": ids},
    )
    print(f"  generation_units: {rs.rowcount:,} rows deleted")
    return rs.rowcount


async def postcheck(db, ids: list[int]) -> bool:
    banner("POST-CHECK")
    rs = await db.execute(
        text("SELECT COUNT(*) FROM generation_units WHERE id = ANY(:ids)"),
        {"ids": ids},
    )
    remaining = rs.scalar()
    print(f"  remaining candidate units: {remaining} (expected 0)")
    return remaining == 0


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true", help="Actually commit deletions")
    args = parser.parse_args()

    S = get_session_factory()
    async with S() as db:
        ok, cands = await precheck(db)
        if not ok or not cands:
            print("\n*** PRE-CHECK FAILED or empty — aborting without changes ***")
            return 1

        ids = [c.id for c in cands]
        await run_delete(db, ids)
        await postcheck(db, ids)

        if args.execute:
            await db.commit()
            print("\n*** COMMITTED ***")
        else:
            await db.rollback()
            print("\n*** DRY-RUN — rolled back. Re-run with --execute to commit. ***")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)
