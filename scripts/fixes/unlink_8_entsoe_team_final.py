"""Unlink the 8 ENTSOE units the team is still flagging.

Background:
  These are the 8 ENTSOE mislinks we reconnected to their correct windfarms on
  2026-05-12. The team has since clarified they only want ELEXON on the chart
  for these UK offshore farms — ELEXON has full coverage; ENTSOE adds essentially
  no unique data (except for EAOne pre-COD commissioning, which we preserve by
  unlinking rather than deleting).

  Identical pattern to the 28 D1 unlinks shipped on 2026-05-19.

Effects per unit:
  - UPDATE generation_data SET windfarm_id = NULL
  - UPDATE generation_units SET windfarm_id = NULL
  - UPDATE generation_unit_mapping SET is_active = FALSE (where active)

Plus once: DELETE performance_summaries on the 5 affected windfarms.

Run:
    poetry run python scripts/fixes/unlink_8_entsoe_team_final.py            # dry-run
    poetry run python scripts/fixes/unlink_8_entsoe_team_final.py --execute  # commit
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


UNITS = [
    # (unit_id, current_wf, label)
    (12328, 7350, "ABRB0-1 → Aberdeen"),
    (12346, 7371, "East Anglia One"),
    (12348, 7373, "Galloper GAOFO-1"),
    (12349, 7373, "Galloper GAOFO-2"),
    (12350, 7373, "Galloper GAOFO-3"),
    (12351, 7373, "Galloper GAOFO-4"),
    (12361, 7384, "Hornsea 1"),
    (12385, 7404, "Ormonde Eng Ltd"),
]
UNIT_IDS = [u for u, *_ in UNITS]
AFFECTED_WFS = sorted({wf for _, wf, _ in UNITS})


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def precheck(db) -> bool:
    banner("PRE-CHECK")
    ok = True
    rs = await db.execute(text("""
        SELECT id, source, name, is_active, windfarm_id,
               (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n_rows
        FROM generation_units gu WHERE id = ANY(:ids) ORDER BY id
    """), {"ids": UNIT_IDS})
    found = {r.id: r for r in rs}
    for uid, expected_wf, label in UNITS:
        u = found.get(uid)
        if not u:
            print(f"  [FAIL] unit {uid} not found"); ok = False; continue
        if u.windfarm_id != expected_wf:
            print(f"  [WARN] unit {uid}: current wf={u.windfarm_id} (expected {expected_wf})")
        if u.source != "ENTSOE":
            print(f"  [FAIL] unit {uid}: source={u.source} (expected ENTSOE)"); ok = False
            continue
        print(f"  [ OK ] unit {uid:>5} '{u.name[:32]:<34}' rows={u.n_rows:>6,} "
              f"current wf={u.windfarm_id} → will be set NULL")
    print(f"\n  Affected windfarms (perf_summaries cleanup): {AFFECTED_WFS}")
    return ok


async def run_unlink(db) -> dict:
    banner("UNLINK")
    counts = {"gen_data_updated": 0, "units_updated": 0, "mappings_deactivated": 0}
    for i, (uid, _, label) in enumerate(UNITS, 1):
        rs = await db.execute(text("""
            UPDATE generation_data SET windfarm_id = NULL
            WHERE generation_unit_id = :u AND windfarm_id IS NOT NULL
        """), {"u": uid})
        nd = rs.rowcount
        rs = await db.execute(text("""
            UPDATE generation_units SET windfarm_id = NULL
            WHERE id = :u AND windfarm_id IS NOT NULL
        """), {"u": uid})
        nu = rs.rowcount
        rs = await db.execute(text("""
            UPDATE generation_unit_mapping SET is_active = FALSE
            WHERE generation_unit_id = :u AND is_active = TRUE
        """), {"u": uid})
        nm = rs.rowcount
        counts["gen_data_updated"]    += nd
        counts["units_updated"]       += nu
        counts["mappings_deactivated"] += nm
        print(f"  [{i}/{len(UNITS)}] unit {uid:>5} ({label[:32]:<34}): gd={nd:>6,}  unit={nu}  map={nm}")
    print(f"\n  TOTAL gen_data rows updated:        {counts['gen_data_updated']:>8,}")
    print(f"  TOTAL unit rows updated:            {counts['units_updated']:>8,}")
    print(f"  TOTAL mappings deactivated:         {counts['mappings_deactivated']:>8,}")
    return counts


async def clear_perf_summaries(db) -> int:
    banner("Invalidate performance_summaries on affected windfarms")
    rs = await db.execute(text("""
        DELETE FROM performance_summaries WHERE windfarm_id = ANY(:ids)
    """), {"ids": AFFECTED_WFS})
    print(f"  performance_summaries deleted: {rs.rowcount:,} rows across {len(AFFECTED_WFS)} windfarms")
    return rs.rowcount


async def postcheck(db) -> bool:
    banner("POST-CHECK")
    ok = True
    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_units WHERE id = ANY(:ids) AND windfarm_id IS NOT NULL
    """), {"ids": UNIT_IDS})
    n = rs.scalar()
    print(f"  Units still with non-NULL windfarm_id:           {n} (expected 0)")
    if n != 0: ok = False
    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_data
        WHERE generation_unit_id = ANY(:ids) AND windfarm_id IS NOT NULL
    """), {"ids": UNIT_IDS})
    n = rs.scalar()
    print(f"  gen_data still with non-NULL windfarm_id:        {n} (expected 0)")
    if n != 0: ok = False
    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_unit_mapping
        WHERE generation_unit_id = ANY(:ids) AND is_active = TRUE
    """), {"ids": UNIT_IDS})
    n = rs.scalar()
    print(f"  Active mappings still on these units:            {n} (expected 0)")
    if n != 0: ok = False
    return ok


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    S = get_session_factory()
    async with S() as db:
        if not await precheck(db):
            print("\n*** PRE-CHECK FAILED — aborting ***")
            return 1
        await run_unlink(db)
        await clear_perf_summaries(db)
        if not await postcheck(db):
            print("\n*** POST-CHECK FAILED — rolling back ***")
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
