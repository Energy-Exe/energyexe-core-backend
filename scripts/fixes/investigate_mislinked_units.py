"""Deep investigation of mislinked generation units.

Triggered by Oliver Stephenson's Teams message flagging "Ormonde Eng Ltd"
(ENTSOE) appearing in Hornsea 2's pre-2022 chart. Ormonde is a separate
UK offshore farm — it should not be attached to Hornsea 2 at all.

From a quick scan of the CSV, at least three CSV rows have unit names that
obviously do not match their windfarm_name column:
  - 48W00000OMNDO-1J 'Ormonde Eng Ltd'   → windfarm_name 'Hornsea 2'
  - 48W00000HOWAO-1M 'Hornsea 1'         → windfarm_name 'Hollandse Kust Zuid'
  - 48W000000EAAO-1R 'East Anglia One'   → windfarm_name 'Dudgeon'

This script:
  1. Lists every ENTSOE inactive unit with its DB windfarm_id, the windfarm's
     actual name, and the unit's own name. Flags rows where the unit name
     contains a string that's NOT a substring of the windfarm's name (i.e.
     suspected mislink).
  2. For each suspected mislink, looks for a windfarm whose name matches the
     unit name (this is the "right" target).
  3. Checks generation_data: are the row-level windfarm_id values consistent
     with the unit's (wrong) windfarm_id, or do they follow a different
     windfarm_id?
  4. Same audit for ELEXON and NVE inactive units.
  5. Quantifies user-visible impact (gen MWh that appears on the wrong
     windfarm's reports).

Run:
    poetry run python scripts/fixes/investigate_mislinked_units.py
"""
import asyncio
import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory

CSV_PATH = "/Users/mdfaisal/Downloads/inactive_generation_units_with_source.csv"

# Normalisation helpers for fuzzy name match
def norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # strip common ENTSOE/elexon suffixes
    s = re.sub(r"\b(wind farm|windfarm|offshore|eng ltd|w/f|wf|ext|phase \d+)\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    # strip trailing identifier tokens like "laryo 1", "gymro 15"
    return s


def core_tokens(name: str) -> set:
    """Words >= 4 chars from unit name minus stopwords/suffixes."""
    n = norm(name)
    return {w for w in n.split() if len(w) >= 4 and w not in {"wind", "farm", "park"}}


async def main():
    # Load CSV
    csv_rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            csv_rows.append(r)

    S = get_session_factory()
    async with S() as db:
        # --- A. Pull every inactive unit row matched in DB with its current
        #         windfarm name, plus the data attribution.
        print("=" * 100)
        print("A. ENTSOE inactive units — windfarm-name vs unit-name comparison")
        print("=" * 100)

        rs = await db.execute(text("""
            SELECT gu.id, gu.name AS unit_name, gu.code, gu.source,
                   gu.windfarm_id, wf.name AS wf_name,
                   gu.is_active,
                   gu.capacity_mw::float AS cap,
                   gu.start_date,
                   (SELECT COUNT(*) FROM generation_data gd WHERE gd.generation_unit_id = gu.id) AS rows,
                   (SELECT SUM(generation_mwh)::float FROM generation_data gd WHERE gd.generation_unit_id = gu.id) AS gen_mwh
            FROM generation_units gu
            LEFT JOIN windfarms wf ON wf.id = gu.windfarm_id
            WHERE gu.source = 'ENTSOE' AND gu.is_active = false
            ORDER BY gu.id
        """))
        entsoe_units = list(rs)

        suspected = []
        ok_names = []
        for u in entsoe_units:
            unit_tokens = core_tokens(u.unit_name)
            wf_tokens = core_tokens(u.wf_name or "")
            shared = unit_tokens & wf_tokens
            if not shared:
                suspected.append(u)
            else:
                ok_names.append(u)

        print(f"\nTotal ENTSOE inactive units: {len(entsoe_units)}")
        print(f"Name match OK:  {len(ok_names)}")
        print(f"Suspected MISLINK (no shared core token): {len(suspected)}")

        for u in suspected:
            print(f"\n  unit_id={u.id}  '{u.unit_name}' (code {u.code}, cap {u.cap or 0:.1f} MW)")
            print(f"    → currently attached to windfarm id={u.windfarm_id} '{u.wf_name}'")
            print(f"    → rows: {u.rows:,}  gen: {(u.gen_mwh or 0):,.0f} MWh")

            # Find best-match windfarm by name token overlap
            unit_tokens = core_tokens(u.unit_name)
            if unit_tokens:
                wf_rs = await db.execute(text("""
                    SELECT id, name FROM windfarms
                    WHERE lower(name) ~ :pat
                    ORDER BY length(name)
                    LIMIT 8
                """), {"pat": "|".join(re.escape(t) for t in unit_tokens)})
                candidates = list(wf_rs)
                if candidates:
                    print(f"    candidate true windfarms:")
                    for c in candidates:
                        print(f"      id={c.id}  '{c.name}'")
                else:
                    print(f"    no windfarm name contains any of: {unit_tokens}")

        # --- B. For each suspected mislink, check generation_data.windfarm_id
        print("\n" + "=" * 100)
        print("B. generation_data row-level windfarm_id distribution per mislinked unit")
        print("=" * 100)
        for u in suspected:
            rs = await db.execute(text("""
                SELECT gd.windfarm_id, wf.name AS wf_name,
                       COUNT(*) AS rows,
                       SUM(gd.generation_mwh)::float AS gen,
                       MIN(gd.hour) AS first_hr,
                       MAX(gd.hour) AS last_hr
                FROM generation_data gd
                LEFT JOIN windfarms wf ON wf.id = gd.windfarm_id
                WHERE gd.generation_unit_id = :uid
                GROUP BY gd.windfarm_id, wf.name
                ORDER BY rows DESC
            """), {"uid": u.id})
            print(f"\n  Unit {u.id} '{u.unit_name}':")
            for r in rs:
                print(f"    wf_id={r.windfarm_id} '{r.wf_name}': "
                      f"{r.rows:,} rows, {(r.gen or 0):,.0f} MWh, "
                      f"{str(r.first_hr)[:10]} → {str(r.last_hr)[:10]}")

        # --- C. Apply same audit to ELEXON and NVE inactive units (only check
        #         token overlap; skip detail unless suspected)
        print("\n" + "=" * 100)
        print("C. ELEXON / NVE inactive units — same token-mismatch scan")
        print("=" * 100)
        for src in ("ELEXON", "NVE"):
            rs = await db.execute(text("""
                SELECT gu.id, gu.name AS unit_name, gu.code,
                       gu.windfarm_id, wf.name AS wf_name,
                       gu.capacity_mw::float AS cap,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows,
                       (SELECT SUM(generation_mwh)::float FROM generation_data WHERE generation_unit_id = gu.id) AS gen_mwh
                FROM generation_units gu
                LEFT JOIN windfarms wf ON wf.id = gu.windfarm_id
                WHERE gu.source = :src AND gu.is_active = false
                ORDER BY gu.id
            """), {"src": src})
            units = list(rs)
            n_suspected = 0
            details = []
            for u in units:
                # For NVE phase units like "Frøya Phase 1", strip the phase suffix before comparing.
                stripped = re.sub(r"\s+phase\s+\d+( decom\d*)?$", "", (u.unit_name or "").lower())
                unit_tokens = core_tokens(stripped)
                wf_tokens = core_tokens(u.wf_name or "")
                shared = unit_tokens & wf_tokens
                if not shared and unit_tokens:
                    n_suspected += 1
                    details.append(u)
            print(f"\n  {src}: {len(units)} total inactive, {n_suspected} suspected mislink")
            for u in details[:30]:
                print(f"    unit_id={u.id} '{u.unit_name}' (code {u.code}, cap {u.cap or 0:.1f}) "
                      f"→ wf={u.windfarm_id} '{u.wf_name}'  "
                      f"rows={u.rows:,} gen={(u.gen_mwh or 0):,.0f}")

        # --- D. Active-units sanity check: any active unit whose name token
        #         doesn't intersect its windfarm's name? (broader audit)
        print("\n" + "=" * 100)
        print("D. ACTIVE units with name/windfarm mismatch (broader audit)")
        print("=" * 100)
        rs = await db.execute(text("""
            SELECT gu.id, gu.name AS unit_name, gu.code, gu.source,
                   gu.windfarm_id, wf.name AS wf_name,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
            FROM generation_units gu
            JOIN windfarms wf ON wf.id = gu.windfarm_id
            WHERE gu.is_active = true
              AND gu.source IN ('ENTSOE', 'ELEXON', 'EIA')
        """))
        active_units = list(rs)
        active_suspect = 0
        for u in active_units:
            ut = core_tokens(u.unit_name)
            wt = core_tokens(u.wf_name or "")
            if ut and not (ut & wt):
                active_suspect += 1
                if active_suspect <= 30:
                    print(f"  id={u.id} '{u.unit_name}' src={u.source} "
                          f"→ wf={u.windfarm_id} '{u.wf_name}'  rows={u.rows:,}")
        print(f"\n  Total active units with name/windfarm mismatch: {active_suspect}")


asyncio.run(main())
