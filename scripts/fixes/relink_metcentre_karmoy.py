"""Re-attach unit 12799 (METCentre Karmoy, NVE) to windfarm 8767 — reverses last week's unlink.

Background:
  On 2026-06-04 scripts/fixes/unlink_7_stamped_units.py cleared the windfarm stamp from 7 units
  believed to be over-counting. Unit 12799 (METCentre Karmoy, NVE) was one of them — treated as a
  "phantom test-centre unit". That call is now reversed: 12799 actually holds ALL of windfarm
  8767's real generation history (142,281 rows, 2009 -> 2025), and removing the stamp made the data
  disappear from the platform.

  The high-traffic surfaces (the /generation/windfarm/{id}/statistics card, the performance
  pipeline loader, opportunity detection, exports) read generation_data.windfarm_id DIRECTLY — not
  the generation_units.windfarm_id link shown in the admin panel. Unit 12799 is currently both
  unlinked at the unit level AND un-stamped, so nothing is attributed to 8767 and the page is empty.
  (The admin-linked units 12789/12790 "Phase 1/Phase 2" carry no data.)

Effects (unit 12799 only):
  - UPDATE generation_data  SET windfarm_id = 8767  WHERE windfarm_id IS NULL  (the ~142,281 rows)
  - UPDATE generation_units SET windfarm_id = 8767                             (1 row — fixes the
      report-service JOIN and keeps future NVE daily ingests stamping correctly:
      process_generation_data_daily.py copies gu.windfarm_id onto new rows)

  Not touched: units 12789/12790 (correctly linked, just empty), generation_unit_mapping (not part
  of the stamp path; 12799 has no mapping row), structural_constraint_flags (analyst-owned).

  Derived analytics (performance_summaries, opportunities, power_curve_bins, ...) were deleted for
  8767 by the unlink and are repopulated by the nightly pipeline now that 8767 has data again.

Run:
    poetry run python scripts/fixes/relink_metcentre_karmoy.py            # dry-run (rolls back)
    poetry run python scripts/fixes/relink_metcentre_karmoy.py --execute  # commit
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from app.core.config import get_settings


UNIT_ID = 12799      # METCentre Karmoy (NVE)
WINDFARM_ID = 8767   # METCentre Karmoy
SOURCE = "NVE"
BATCH_SIZE = 20_000  # re-stamp in chunks: keeps each statement well under command_timeout


def get_session_factory():
    """Dedicated engine with a long command_timeout — the one-shot ~142k-row re-stamp UPDATE
    blows past the app's default 180s asyncpg command_timeout over remote RDS."""
    settings = get_settings()
    url = settings.database_url_async
    kwargs = {"echo": False, "future": True}
    if "sqlite" in url:
        from sqlalchemy.pool import StaticPool
        kwargs["poolclass"] = StaticPool
        kwargs["connect_args"] = {"check_same_thread": False}
    else:
        kwargs["connect_args"] = {
            "server_settings": {"application_name": "relink-metcentre-karmoy"},
            "command_timeout": 1800,  # 30 min ceiling (vs default 180s)
            "ssl": "require",
        }
    engine = create_async_engine(url, **kwargs)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def windfarm_totals(db) -> tuple:
    """Net generation + row count attributed to windfarm 8767 (mirrors the stats endpoint)."""
    rs = await db.execute(text("""
        SELECT COUNT(*) AS rows,
               COALESCE(SUM(generation_mwh), 0) / 1000.0 AS gwh
        FROM generation_data
        WHERE windfarm_id = :wf
    """), {"wf": WINDFARM_ID})
    r = rs.first()
    return (r.rows, float(r.gwh))


async def precheck(db) -> bool:
    banner("PRE-CHECK")
    ok = True

    # 1. Target windfarm exists.
    rs = await db.execute(text("SELECT id, name FROM windfarms WHERE id = :w"), {"w": WINDFARM_ID})
    w = rs.first()
    if w is None:
        print(f"  [FAIL] target windfarm {WINDFARM_ID} not found"); ok = False
    else:
        print(f"  [ OK ] target wf {WINDFARM_ID} '{w.name}' exists")

    # 2. Unit exists, is NVE, is active.
    rs = await db.execute(text("""
        SELECT id, name, source, is_active, windfarm_id
        FROM generation_units WHERE id = :u
    """), {"u": UNIT_ID})
    u = rs.first()
    if u is None:
        print(f"  [FAIL] unit {UNIT_ID} not found"); ok = False
    else:
        if u.source != SOURCE:
            print(f"  [FAIL] unit {UNIT_ID}: source={u.source} (expected {SOURCE})"); ok = False
        if not u.is_active:
            print(f"  [FAIL] unit {UNIT_ID}: is_active=False (expected active operational unit)"); ok = False
        link = u.windfarm_id if u.windfarm_id is not None else "NULL"
        print(f"  [ OK ] unit {UNIT_ID} src={u.source} '{u.name[:30]}' "
              f"active={u.is_active} unit-level windfarm_id={link} -> will set {WINDFARM_ID}")

    # 3. Rows on the unit that are NULL-stamped (these get re-stamped) vs already on 8767.
    rs = await db.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM generation_data
            WHERE generation_unit_id = :u AND windfarm_id IS NULL AND source = :src) AS null_rows,
          (SELECT COUNT(*) FROM generation_data
            WHERE generation_unit_id = :u AND windfarm_id IS NOT NULL AND windfarm_id <> :w) AS other_rows
    """), {"u": UNIT_ID, "src": SOURCE, "w": WINDFARM_ID})
    r = rs.first()
    print(f"  unit {UNIT_ID}: NULL-stamped rows to re-stamp = {r.null_rows:,}  "
          f"(rows stamped to a DIFFERENT windfarm = {r.other_rows:,}, expected 0)")
    if r.null_rows == 0:
        print(f"  [WARN] no NULL-stamped rows found — already re-stamped?")
    if r.other_rows != 0:
        print(f"  [WARN] {r.other_rows:,} rows point to another windfarm — left untouched (script only "
              f"re-stamps NULL rows)")

    before = await windfarm_totals(db)
    print(f"\n  Windfarm {WINDFARM_ID} totals BEFORE: {before[1]:,.1f} GWh   {before[0]:,} rows")
    return ok


async def run_relink(db) -> dict:
    banner("RE-LINK (restore windfarm stamp)")
    counts = {}

    # Batched re-stamp: each statement updates at most BATCH_SIZE rows so it stays well under
    # command_timeout. Updated rows drop out of the NULL predicate, so the loop converges.
    total = 0
    while True:
        rs = await db.execute(text("""
            WITH batch AS (
                SELECT id FROM generation_data
                WHERE generation_unit_id = :u AND windfarm_id IS NULL AND source = :src
                LIMIT :n
            )
            UPDATE generation_data g SET windfarm_id = :w, updated_at = now()
            FROM batch WHERE g.id = batch.id
        """), {"w": WINDFARM_ID, "u": UNIT_ID, "src": SOURCE, "n": BATCH_SIZE})
        n = rs.rowcount
        total += n
        if n:
            print(f"    ... re-stamped {total:,} rows", flush=True)
        if n < BATCH_SIZE:
            break
    counts["gen_data"] = total

    rs = await db.execute(text("""
        UPDATE generation_units SET windfarm_id = :w, updated_at = now()
        WHERE id = :u AND windfarm_id IS DISTINCT FROM :w
    """), {"w": WINDFARM_ID, "u": UNIT_ID})
    counts["units"] = rs.rowcount

    print(f"  generation_data rows re-stamped:  {counts['gen_data']:>9,}")
    print(f"  generation_units rows linked:     {counts['units']:>9,}  (expected 1)")
    return counts


async def postcheck(db) -> bool:
    banner("POST-CHECK")
    ok = True

    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_data
        WHERE generation_unit_id = :u AND windfarm_id IS NULL AND source = :src
    """), {"u": UNIT_ID, "src": SOURCE})
    n = rs.scalar()
    print(f"  unit {UNIT_ID} rows still NULL-stamped:   {n} (expected 0)")
    if n != 0:
        ok = False

    rs = await db.execute(text("SELECT windfarm_id FROM generation_units WHERE id = :u"), {"u": UNIT_ID})
    link = rs.scalar()
    print(f"  unit {UNIT_ID} unit-level windfarm_id:    {link} (expected {WINDFARM_ID})")
    if link != WINDFARM_ID:
        ok = False

    after = await windfarm_totals(db)
    print(f"\n  Windfarm {WINDFARM_ID} totals AFTER: {after[1]:,.1f} GWh   {after[0]:,} rows")
    if after[0] == 0:
        print("  [FAIL] windfarm 8767 still has 0 rows"); ok = False
    return ok


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true", help="Commit changes (default: dry-run rollback)")
    args = parser.parse_args()

    S = get_session_factory()
    async with S() as db:
        if not await precheck(db):
            print("\n*** PRE-CHECK FAILED — aborting without changes ***")
            return 1
        await run_relink(db)
        if not await postcheck(db):
            print("\n*** POST-CHECK FAILED — rolling back ***")
            await db.rollback()
            return 1
        if args.execute:
            await db.commit()
            print("\n*** COMMITTED ***")
            async with S() as db2:
                banner("POST-COMMIT VERIFICATION (fresh session)")
                if not await postcheck(db2):
                    print("\n!!! POST-COMMIT VERIFICATION FAILED — investigate immediately !!!")
                    return 2
        else:
            await db.rollback()
            print("\n*** DRY-RUN — rolled back. Re-run with --execute to commit. ***")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)
