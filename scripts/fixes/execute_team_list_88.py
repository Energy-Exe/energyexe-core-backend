"""Execute the team's 'Unlink and delete generation units' list, minus the 8
units we already reconnected to their correct windfarms (those should stay).

Source: Prioritisation 2026-05-18.docx (first table). 96 rows total → 88 actions.

Action breakdown:
  UNLINK only (28 units):
    Set generation_units.windfarm_id = NULL
    Set generation_data.windfarm_id  = NULL for that unit's rows
    Flip generation_unit_mapping.is_active = FALSE for that unit's mappings
    (Keep unit row + data; data becomes invisible in UI but accessible via unit-id.)

  UNLINK AND DELETE (60 units):
    DELETE generation_unit_mapping WHERE generation_unit_id IN (...)
    DELETE data_anomalies          WHERE generation_unit_id IN (...)
    DELETE generation_data         WHERE generation_unit_id IN (...)
    DELETE generation_units        WHERE id IN (...)
    (Raw NVE / ELEXON data in generation_data_raw is retained — reversible.)

Performance_summaries on every affected windfarm are also deleted so MTD/QTD/YTD
caches regenerate cleanly from the daily pipeline.

EXCLUDED from this run (team's list, but our prior fix already handled them
by reconnecting to the correct windfarm, not unlinking):
  12328 ABRB0-1            (now correctly on wf 7350 Aberdeen)
  12346 East Anglia One    (now correctly on wf 7371 East Anglia One)
  12348 Galloper GAOFO-1   (now correctly on wf 7373 Galloper)
  12349 Galloper GAOFO-2   (    ”)
  12350 Galloper GAOFO-3   (    ”)
  12351 Galloper GAOFO-4   (    ”)
  12361 Hornsea 1          (now correctly on wf 7384 Hornsea 1)
  12385 Ormonde Eng Ltd    (now correctly on wf 7404 Ormonde)

Run:
    poetry run python scripts/fixes/execute_team_list_88.py            # dry-run
    poetry run python scripts/fixes/execute_team_list_88.py --execute  # commit
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


# 28 units to UNLINK (detach windfarm_id, keep unit + data)
UNLINK_IDS = [
    12329, 12335, 12334, 10120, 12355, 12356, 12357, 12358, 12359, 12360,
    12371, 12372, 12373, 12374, 12375, 12386, 12387, 12390, 12391, 12398,
    12399, 12402, 12403, 12404, 12405, 12406, 12407, 12408,
]

# 60 units to UNLINK + DELETE
DELETE_IDS = [
    12430, 12436, 12439, 12445, 12451, 12453, 12454,                          # Buheii..Gismarvik
    12479, 12480, 12481, 12484, 12485, 12487, 12490,                          # Guleslettene x7
    12496, 12497, 12500, 12505, 12507, 12512,                                 # Haram, Harbaksfjellet, Havøygavlen, Hitra
    12545, 12546, 12553, 12555, 12556, 12559, 12562, 12565,                   # Hundhammerfjellet, Kjølberget x3, Kjøllefjord P1, Kvenndalsfjellet, Kvitfjell
    12370,                                                                     # Lincs LNCSO-1 (ENTSOE, only one in delete list)
    12573, 12575, 12576, 12579, 12581, 12597, 12599,                          # Lutelandet x3, Måkaknuten x3, Marker
    12632, 12634, 12658, 12690, 12691, 12692, 12693,                          # Odal x2, Øyfjellet x5
    12716, 12718, 12720, 12723, 12726,                                         # Raudfjell x2, Roan, Skinansfjellet, Skomakerfjellet
    12738, 12741, 12744, 12747, 12749, 12752, 12760, 12763, 12767,            # Sørmarkfjellet x3, Stigafjellet x2, Stokkfjellet x2, Storheia, Tellenes
    12770, 12775, 12803,                                                       # Tonstad x2, Valsneset testpark
]

EXCLUDED_ALREADY_FIXED = {12328, 12346, 12348, 12349, 12350, 12351, 12361, 12385}


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def precheck(db) -> tuple[bool, set[int]]:
    banner("PRE-CHECK")
    ok = True
    assert len(UNLINK_IDS) == 28, f"UNLINK_IDS has {len(UNLINK_IDS)} (expected 28)"
    assert len(DELETE_IDS) == 60, f"DELETE_IDS has {len(DELETE_IDS)} (expected 60)"
    assert not (set(UNLINK_IDS) & set(DELETE_IDS)), "UNLINK and DELETE overlap"
    assert not (set(UNLINK_IDS) & EXCLUDED_ALREADY_FIXED), "UNLINK includes excluded"
    assert not (set(DELETE_IDS) & EXCLUDED_ALREADY_FIXED), "DELETE includes excluded"
    print(f"  Counts: UNLINK={len(UNLINK_IDS)}  DELETE={len(DELETE_IDS)}  total={len(UNLINK_IDS)+len(DELETE_IDS)}")
    print(f"  Excluded (already reconnected): {sorted(EXCLUDED_ALREADY_FIXED)}")

    # Each candidate must exist
    all_ids = UNLINK_IDS + DELETE_IDS
    rs = await db.execute(text("""
        SELECT id FROM generation_units WHERE id = ANY(:ids)
    """), {"ids": all_ids})
    found = {r.id for r in rs}
    missing = set(all_ids) - found
    if missing:
        print(f"  [FAIL] units not in DB: {sorted(missing)}")
        ok = False
    else:
        print(f"  [ OK ] all {len(all_ids)} units present in DB")

    # No UNLINK candidate should already have NULL windfarm_id (else nothing to do)
    rs = await db.execute(text("""
        SELECT id, name FROM generation_units WHERE id = ANY(:ids) AND windfarm_id IS NULL
    """), {"ids": UNLINK_IDS})
    null_wf = list(rs)
    if null_wf:
        print(f"  [WARN] {len(null_wf)} UNLINK candidates already have windfarm_id=NULL:")
        for r in null_wf:
            print(f"         {r.id} '{r.name}'")

    # Collect set of affected windfarm_ids (for perf_summaries cleanup later)
    rs = await db.execute(text("""
        SELECT DISTINCT windfarm_id FROM generation_units
        WHERE id = ANY(:ids) AND windfarm_id IS NOT NULL
    """), {"ids": all_ids})
    affected_wfs = {r.windfarm_id for r in rs}
    rs = await db.execute(text("""
        SELECT DISTINCT windfarm_id FROM generation_data
        WHERE generation_unit_id = ANY(:ids) AND windfarm_id IS NOT NULL
    """), {"ids": all_ids})
    affected_wfs |= {r.windfarm_id for r in rs}
    print(f"\n  Affected windfarms (for perf_summaries cleanup): {len(affected_wfs)}")
    print(f"    {sorted(affected_wfs)}")

    # FK refs that must be handled
    for tbl, col in [
        ("generation_unit_mapping", "generation_unit_id"),
        ("data_anomalies", "generation_unit_id"),
    ]:
        rs = await db.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} = ANY(:ids)"),
                              {"ids": DELETE_IDS})
        n = rs.scalar()
        print(f"  DELETE-targets referenced in {tbl}: {n} rows (will be cleared)")

    return ok, affected_wfs


async def run_unlink(db) -> dict:
    banner("UNLINK (detach windfarm_id; keep unit + data) — batched per unit")
    counts = {"gen_data_updated": 0, "units_updated": 0, "mappings_deactivated": 0}

    # Per-unit loop — keeps each UPDATE small enough to dodge asyncpg statement timeout
    for i, uid in enumerate(UNLINK_IDS, 1):
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

        counts["gen_data_updated"] += nd
        counts["units_updated"]     += nu
        counts["mappings_deactivated"] += nm
        print(f"  [{i:>2}/{len(UNLINK_IDS)}] unit {uid:>5}: gd={nd:>7,}  unit={nu}  map={nm}")

    print(f"\n  TOTAL gen_data rows updated:        {counts['gen_data_updated']:>10,}")
    print(f"  TOTAL unit rows updated:            {counts['units_updated']:>10,}")
    print(f"  TOTAL mappings deactivated:         {counts['mappings_deactivated']:>10,}")
    return counts


async def run_delete(db) -> dict:
    banner("DELETE (remove unit + data; preserve raw) — batched per unit")
    counts = {"mappings": 0, "anomalies": 0, "gen_data": 0, "units": 0}

    for i, uid in enumerate(DELETE_IDS, 1):
        rs = await db.execute(text("DELETE FROM generation_unit_mapping WHERE generation_unit_id = :u"),
                              {"u": uid})
        nm = rs.rowcount
        rs = await db.execute(text("DELETE FROM data_anomalies WHERE generation_unit_id = :u"),
                              {"u": uid})
        na = rs.rowcount
        rs = await db.execute(text("DELETE FROM generation_data WHERE generation_unit_id = :u"),
                              {"u": uid})
        ng = rs.rowcount
        rs = await db.execute(text("DELETE FROM generation_units WHERE id = :u"),
                              {"u": uid})
        nu = rs.rowcount

        counts["mappings"]  += nm
        counts["anomalies"] += na
        counts["gen_data"]  += ng
        counts["units"]     += nu
        print(f"  [{i:>2}/{len(DELETE_IDS)}] unit {uid:>5}: map={nm}  anom={na}  gd={ng:>7,}  unit={nu}")

    print(f"\n  TOTAL mappings  deleted: {counts['mappings']:>10,}")
    print(f"  TOTAL anomalies deleted: {counts['anomalies']:>10,}")
    print(f"  TOTAL gen_data  deleted: {counts['gen_data']:>10,}")
    print(f"  TOTAL units     deleted: {counts['units']:>10,}")
    return counts


async def clear_perf_summaries(db, wf_ids: set[int]) -> int:
    banner("Invalidate performance_summaries on affected windfarms")
    if not wf_ids:
        print("  (no affected windfarms)")
        return 0
    rs = await db.execute(text("""
        DELETE FROM performance_summaries WHERE windfarm_id = ANY(:ids)
    """), {"ids": list(wf_ids)})
    print(f"  performance_summaries deleted: {rs.rowcount:,} rows across {len(wf_ids)} windfarms")
    return rs.rowcount


async def postcheck(db) -> bool:
    banner("POST-CHECK")
    ok = True
    # Unlinked units must have NULL windfarm_id
    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_units
        WHERE id = ANY(:ids) AND windfarm_id IS NOT NULL
    """), {"ids": UNLINK_IDS})
    n = rs.scalar()
    print(f"  UNLINK units still with non-NULL windfarm_id: {n} (expected 0)")
    if n != 0:
        ok = False

    # Unlinked units' gen_data must have NULL windfarm_id
    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_data
        WHERE generation_unit_id = ANY(:ids) AND windfarm_id IS NOT NULL
    """), {"ids": UNLINK_IDS})
    n = rs.scalar()
    print(f"  UNLINK gen_data still with non-NULL windfarm_id: {n} (expected 0)")
    if n != 0:
        ok = False

    # Deleted units must be gone
    rs = await db.execute(text("SELECT COUNT(*) FROM generation_units WHERE id = ANY(:ids)"),
                          {"ids": DELETE_IDS})
    n = rs.scalar()
    print(f"  DELETE units still present:                   {n} (expected 0)")
    if n != 0:
        ok = False
    rs = await db.execute(text("SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = ANY(:ids)"),
                          {"ids": DELETE_IDS})
    n = rs.scalar()
    print(f"  DELETE gen_data still present:                {n} (expected 0)")
    if n != 0:
        ok = False
    return ok


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    S = get_session_factory()
    async with S() as db:
        ok, affected_wfs = await precheck(db)
        if not ok:
            print("\n*** PRE-CHECK FAILED — aborting ***")
            return 1

        await run_unlink(db)
        await run_delete(db)
        await clear_perf_summaries(db, affected_wfs)
        post_ok = await postcheck(db)
        if not post_ok:
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
