"""Batch A cleanup — four low-risk changes in one transaction:

  1. UPDATE generation_units SET is_active=TRUE WHERE id=12560 (Kjøllefjord Phase 2)
  2. UPDATE generation_units SET is_active=TRUE WHERE id=10103 (Causeymire)
  3. DELETE FROM generation_units WHERE id=12806 ('DELETE ME' ELEXON junk)
  4. Read-only audit: D1/D2 inactive-unit mapping rows — should all be is_active=False.

Steps 1+2 are pure metadata flips (no data movement). Step 3 deletes a single
zero-data row. Step 4 surfaces any mapping rows that could allow a daily cron
to write to a historical-only unit.

Run:
    poetry run python scripts/fixes/batch_a_cleanup.py            # dry-run
    poetry run python scripts/fixes/batch_a_cleanup.py --execute  # commit
"""
import argparse
import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


FLIP_ACTIVE = [
    # (unit_id, expected_source, expected_name_substring, expected_min_rows)
    (12560, "NVE",    "Kjøllefjord",  150_000),
    (10103, "ELEXON", "Causeymire",    40_000),
]
DELETE_IDS = [12806]


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


def tokens(s: str) -> set:
    s = (s or "").lower()
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    return {w for w in s.split() if len(w) >= 4 and w not in {"wind", "farm", "park", "phase"}}


async def precheck(db) -> bool:
    banner("PRE-CHECK")
    ok = True

    # 1+2. flip-active candidates
    for uid, exp_src, exp_name, exp_rows in FLIP_ACTIVE:
        rs = await db.execute(text("""
            SELECT id, source, name, is_active, windfarm_id,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
            FROM generation_units gu WHERE id = :u
        """), {"u": uid})
        u = rs.first()
        if not u:
            print(f"  [FAIL] unit {uid} not found"); ok = False; continue
        if u.source != exp_src:
            print(f"  [FAIL] unit {uid} source={u.source} expected {exp_src}"); ok = False
        if exp_name not in u.name:
            print(f"  [FAIL] unit {uid} name='{u.name}' (expected to contain {exp_name!r})"); ok = False
        if u.is_active:
            print(f"  [SKIP] unit {uid} already is_active=True"); ok = False
        if u.rows < exp_rows:
            print(f"  [WARN] unit {uid} only {u.rows:,} rows (expected ≥{exp_rows:,})")
        if ok:
            # Confirm no active sibling on same (wf, source) — defining feature of D5
            rs = await db.execute(text("""
                SELECT COUNT(*) FROM generation_units gu2
                JOIN generation_data gd ON gd.generation_unit_id = gu2.id
                WHERE gu2.is_active = TRUE
                  AND gu2.source = :src
                  AND gd.windfarm_id = :w
            """), {"src": u.source, "w": u.windfarm_id})
            n = rs.scalar()
            if n > 0:
                print(f"  [WARN] unit {uid}: an active sibling already exists on "
                      f"(wf={u.windfarm_id}, src={u.source}) — flip may double-up")
            print(f"  [ OK ] unit {uid} '{u.name[:30]}' src={u.source} rows={u.rows:,} "
                  f"wf={u.windfarm_id} (will flip → active=True)")

    # 3. delete candidate
    for uid in DELETE_IDS:
        rs = await db.execute(text("""
            SELECT id, source, name, is_active,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
            FROM generation_units gu WHERE id = :u
        """), {"u": uid})
        u = rs.first()
        if not u:
            print(f"  [FAIL] unit {uid} not found"); ok = False; continue
        # Verify no FK references
        for tbl, col in [
            ("generation_data", "generation_unit_id"),
            ("data_anomalies", "generation_unit_id"),
            ("generation_unit_mapping", "generation_unit_id"),
        ]:
            n = (await db.execute(
                text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} = :u"), {"u": uid})).scalar()
            if n != 0:
                print(f"  [FAIL] {tbl}.{col}: {n} refs to unit {uid}"); ok = False
        print(f"  [ OK ] unit {uid} '{u.name}' src={u.source} rows={u.rows} "
              f"(zero FK refs — will DELETE)")

    return ok


async def audit_d1_d2_mappings(db):
    """Read-only audit: for every inactive unit that has gen_data (D1+D2 + a few stragglers),
    list the is_active flag on its generation_unit_mapping rows. Any active mapping
    is a yellow flag (daily cron could write here)."""
    banner("D1/D2 mapping audit (read-only)")
    rs = await db.execute(text("""
        SELECT gu.id, gu.source, gu.name,
               (SELECT COUNT(*) FROM generation_unit_mapping gum
                  WHERE gum.generation_unit_id = gu.id) AS total_maps,
               (SELECT COUNT(*) FROM generation_unit_mapping gum
                  WHERE gum.generation_unit_id = gu.id AND gum.is_active = TRUE) AS active_maps
        FROM generation_units gu
        WHERE gu.is_active = FALSE
          AND EXISTS (SELECT 1 FROM generation_data WHERE generation_unit_id = gu.id)
        ORDER BY active_maps DESC, gu.source, gu.id
    """))
    rows = list(rs)
    active_count = sum(1 for r in rows if r.active_maps > 0)
    nomap_count = sum(1 for r in rows if r.total_maps == 0)
    print(f"  inactive units with gen_data: {len(rows)}")
    print(f"  ... with ACTIVE mapping row(s): {active_count}  (potential cron-write risk)")
    print(f"  ... with NO mapping row at all: {nomap_count}  (data is orphaned from ingestion)")
    print(f"  ... with only inactive mapping(s): {len(rows) - active_count - nomap_count}  (safe)")
    if active_count > 0:
        print("\n  Units with active mappings (yellow flags):")
        for r in rows:
            if r.active_maps > 0:
                print(f"    {r.id:>5} {r.source:<8} '{r.name[:40]:<42}' "
                      f"maps={r.total_maps} active={r.active_maps}")


async def run_changes(db):
    banner("APPLY CHANGES")
    flipped = 0
    for uid, *_ in FLIP_ACTIVE:
        rs = await db.execute(
            text("UPDATE generation_units SET is_active = TRUE WHERE id = :u AND is_active = FALSE"),
            {"u": uid},
        )
        if rs.rowcount:
            print(f"  unit {uid}: is_active False → True")
            flipped += rs.rowcount

    deleted = 0
    for uid in DELETE_IDS:
        rs = await db.execute(
            text("DELETE FROM generation_units WHERE id = :u"), {"u": uid},
        )
        if rs.rowcount:
            print(f"  unit {uid}: DELETED")
            deleted += rs.rowcount

    print(f"\n  Flipped active: {flipped}    Deleted: {deleted}")


async def postcheck(db) -> bool:
    banner("POST-CHECK")
    ok = True
    for uid, *_ in FLIP_ACTIVE:
        rs = await db.execute(text("SELECT is_active FROM generation_units WHERE id = :u"), {"u": uid})
        r = rs.first()
        active = r.is_active if r else None
        print(f"  unit {uid}: is_active = {active} (expected True)")
        if active is not True:
            ok = False
    for uid in DELETE_IDS:
        rs = await db.execute(text("SELECT 1 FROM generation_units WHERE id = :u"), {"u": uid})
        exists = rs.first() is not None
        print(f"  unit {uid}: exists = {exists} (expected False)")
        if exists:
            ok = False
    return ok


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    S = get_session_factory()
    async with S() as db:
        ok = await precheck(db)
        await audit_d1_d2_mappings(db)
        if not ok:
            print("\n*** PRE-CHECK had failures — aborting before changes ***")
            return 1

        await run_changes(db)
        post_ok = await postcheck(db)
        if not post_ok:
            print("\n*** POST-CHECK FAILED — rolling back regardless ***")
            await db.rollback()
            return 1

        if args.execute:
            await db.commit()
            print("\n*** COMMITTED ***")
        else:
            await db.rollback()
            print("\n*** DRY-RUN — rolled back. Re-run with --execute to commit. ***")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)
