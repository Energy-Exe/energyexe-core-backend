"""Remove the windfarm stamp from 7 unlinked-but-still-stamped generation units.

Background:
  A teammate flagged 7 generation units that are already unlinked at the unit level
  (generation_units.windfarm_id IS NULL) but whose historical generation_data rows still
  carry a windfarm_id "stamp". The high-traffic rollups (the /generation/windfarm/{id}/
  statistics card, the performance pipeline loader, opportunity detection, exports) read
  generation_data.windfarm_id DIRECTLY, so the stamp over-counts those windfarms' power
  generation and capacity factors. Clearing the stamp is the operative fix — the on-the-fly
  rollups correct themselves on the next request; no source re-aggregation is needed.

  Same pattern as scripts/fixes/unlink_8_entsoe_team_final.py, with two differences:
    - mixed source (ENTSOE + NVE), and
    - the units are already unit-level NULL, so the stamp lives only in generation_data.

Effects per unit:
  - UPDATE generation_data  SET windfarm_id = NULL   (the fix)
  - UPDATE generation_units SET windfarm_id = NULL   (expected 0 rows — already NULL — kept for safety)
  - UPDATE generation_unit_mapping SET is_active = FALSE  (the 5 ENTSOE units only)

Plus once: DELETE the contaminated performance-pipeline outputs on the 4 affected windfarms
  (performance_summaries, performance_anomalies, power_curve_bins, degradation_results,
   generation_concentration_summaries, constraint_loss_summaries, opportunities).
  structural_constraint_flags is PRESERVED (analyst-owned). The nightly pipeline repopulates
  correct analytics for the 3 windfarms that retain real data; METCentre Karmøy (8767) stays
  empty (all of its stamped data was this single unit — intended).

Run:
    poetry run python scripts/fixes/unlink_7_stamped_units.py            # dry-run (rolls back)
    poetry run python scripts/fixes/unlink_7_stamped_units.py --execute  # commit
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


UNITS = [
    # (unit_id, stamped_wf, label)  — all already generation_units.windfarm_id IS NULL
    (12352, 7376, "Greater Gabbard GRGBW-1 (ENTSOE, inactive)"),
    (12353, 7376, "Greater Gabbard GRGBW-2 (ENTSOE, inactive)"),
    (12354, 7376, "Greater Gabbard GRGBW-3 (ENTSOE, inactive)"),
    (12388, 7407, "Rentel RCBKO-1 (ENTSOE, inactive)"),
    (12389, 7407, "Rentel RCBKO-2 (ENTSOE, inactive)"),
    (12799, 8767, "METCentre Karmoy (NVE, ACTIVE/operational)"),
    (12800, 7226, "Vikna -> Ytre Vikna (NVE, decommissioned)"),
]
UNIT_IDS = [u for u, *_ in UNITS]
AFFECTED_WFS = sorted({wf for _, wf, _ in UNITS})  # [7226, 7376, 7407, 8767]
ALLOWED_SOURCES = {"ENTSOE", "NVE"}

# Pipeline-derived tables keyed by windfarm_id that are contaminated by the over-count.
# structural_constraint_flags is intentionally EXCLUDED (analyst-owned / confirmed flags).
DERIVED_TABLES = [
    "performance_summaries",
    "performance_anomalies",
    "power_curve_bins",
    "degradation_results",
    "generation_concentration_summaries",
    "constraint_loss_summaries",
    "opportunities",
]


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def windfarm_totals(db) -> dict:
    """Net generation + row count per affected windfarm (mirrors the stats-endpoint attribution)."""
    rs = await db.execute(text("""
        SELECT windfarm_id,
               COUNT(*) AS rows,
               COALESCE(SUM(generation_mwh), 0) / 1000.0 AS gwh
        FROM generation_data
        WHERE windfarm_id = ANY(:ids)
        GROUP BY windfarm_id
    """), {"ids": AFFECTED_WFS})
    return {r.windfarm_id: (r.rows, float(r.gwh)) for r in rs}


async def precheck(db) -> bool:
    banner("PRE-CHECK")
    ok = True
    # Per-unit lookup (the expected windfarm differs per unit, so query individually).
    found = {}
    for uid, exp_wf, _ in UNITS:
        rs = await db.execute(text("""
            SELECT gu.id, gu.source, gu.name, gu.is_active, gu.windfarm_id,
                   (SELECT COUNT(*) FROM generation_data gd
                     WHERE gd.generation_unit_id = gu.id AND gd.windfarm_id = :exp) AS stamped_rows
            FROM generation_units gu WHERE gu.id = :uid
        """), {"uid": uid, "exp": exp_wf})
        row = rs.first()
        found[uid] = row

    for uid, exp_wf, label in UNITS:
        u = found.get(uid)
        if u is None:
            print(f"  [FAIL] unit {uid} not found"); ok = False; continue
        if u.source not in ALLOWED_SOURCES:
            print(f"  [FAIL] unit {uid}: source={u.source} (expected one of {sorted(ALLOWED_SOURCES)})")
            ok = False; continue
        if u.windfarm_id is not None:
            # Not fatal — the script still clears it (step 2) — but flag the surprise.
            print(f"  [WARN] unit {uid}: unit-level windfarm_id={u.windfarm_id} (expected NULL); will be cleared too")
        if u.stamped_rows == 0:
            print(f"  [WARN] unit {uid}: 0 generation_data rows stamped to wf {exp_wf} (already clean?)")
        print(f"  [ OK ] unit {uid:>5} src={u.source:<6} '{u.name[:30]:<32}' "
              f"active={str(u.is_active):<5} stamped→wf {exp_wf}: {u.stamped_rows:>7,} rows")

    # Unit 12799 is active/operational — must remain so and must not be deleted.
    u12799 = found.get(12799)
    if u12799 is not None and not u12799.is_active:
        print("  [FAIL] unit 12799 expected is_active=True (active operational NVE unit)"); ok = False

    print(f"\n  Affected windfarms (derived-table cleanup): {AFFECTED_WFS}")
    print("\n  Windfarm totals BEFORE (net GWh / rows):")
    before = await windfarm_totals(db)
    for wf in AFFECTED_WFS:
        rows, gwh = before.get(wf, (0, 0.0))
        print(f"    wf {wf}: {gwh:>12,.1f} GWh   {rows:>9,} rows")
    return ok


async def run_unlink(db) -> dict:
    banner("UNLINK (remove windfarm stamp)")
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
        counts["gen_data_updated"]     += nd
        counts["units_updated"]        += nu
        counts["mappings_deactivated"] += nm
        print(f"  [{i}/{len(UNITS)}] unit {uid:>5} ({label[:34]:<36}): gd={nd:>7,}  unit={nu}  map={nm}")
    print(f"\n  TOTAL gen_data rows un-stamped:     {counts['gen_data_updated']:>9,}")
    print(f"  TOTAL unit rows updated:            {counts['units_updated']:>9,}  (expected 0 — already NULL)")
    print(f"  TOTAL mappings deactivated:         {counts['mappings_deactivated']:>9,}  (expected 5 — ENTSOE only)")
    return counts


async def clear_derived_tables(db) -> dict:
    banner("Invalidate contaminated performance-pipeline outputs on affected windfarms")
    counts = {}
    for tbl in DERIVED_TABLES:
        rs = await db.execute(
            text(f"DELETE FROM {tbl} WHERE windfarm_id = ANY(:ids)"),
            {"ids": AFFECTED_WFS},
        )
        counts[tbl] = rs.rowcount
        print(f"  {tbl:<38} deleted: {rs.rowcount:>8,}")
    print("  structural_constraint_flags            PRESERVED (analyst-owned)")
    return counts


async def postcheck(db) -> bool:
    banner("POST-CHECK")
    ok = True

    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_data
        WHERE generation_unit_id = ANY(:ids) AND windfarm_id IS NOT NULL
    """), {"ids": UNIT_IDS})
    n = rs.scalar()
    print(f"  gen_data still stamped on these units:           {n} (expected 0)")
    if n != 0: ok = False

    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_units WHERE id = ANY(:ids) AND windfarm_id IS NOT NULL
    """), {"ids": UNIT_IDS})
    n = rs.scalar()
    print(f"  units still with non-NULL windfarm_id:           {n} (expected 0)")
    if n != 0: ok = False

    rs = await db.execute(text("""
        SELECT COUNT(*) FROM generation_unit_mapping
        WHERE generation_unit_id = ANY(:ids) AND is_active = TRUE
    """), {"ids": UNIT_IDS})
    n = rs.scalar()
    print(f"  active mappings still on these units:            {n} (expected 0)")
    if n != 0: ok = False

    for tbl in DERIVED_TABLES:
        rs = await db.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE windfarm_id = ANY(:ids)"),
            {"ids": AFFECTED_WFS},
        )
        n = rs.scalar()
        if n != 0:
            print(f"  [FAIL] {tbl} still has {n} rows for affected windfarms (expected 0)")
            ok = False

    # Unit 12799 must survive intact: still active, rows present, all NULL-stamped now.
    rs = await db.execute(text("""
        SELECT gu.is_active,
               (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = 12799) AS total_rows,
               (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = 12799 AND windfarm_id IS NOT NULL) AS stamped
        FROM generation_units gu WHERE gu.id = 12799
    """))
    r = rs.first()
    if r is not None:
        print(f"  unit 12799: is_active={r.is_active} total_rows={r.total_rows:,} stamped={r.stamped} "
              f"(expected active=True, stamped=0)")
        if not r.is_active or r.stamped != 0:
            ok = False

    print("\n  Windfarm totals AFTER (net GWh / rows):")
    after = await windfarm_totals(db)
    for wf in AFFECTED_WFS:
        rows, gwh = after.get(wf, (0, 0.0))
        tag = "  <- now empty" if rows == 0 else ""
        print(f"    wf {wf}: {gwh:>12,.1f} GWh   {rows:>9,} rows{tag}")
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
        await run_unlink(db)
        await clear_derived_tables(db)
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
