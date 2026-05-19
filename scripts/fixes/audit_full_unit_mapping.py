"""Final audit: dump ALL generation_unit_mapping rows joined to
generation_units and windfarms, so a human can eyeball every (unit_name,
windfarm_name) pair. Sort by source then by windfarm name.

Also print mapping rows where source_identifier looks like a windfarm name
that doesn't match the assigned windfarm.
"""
import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


def norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def tokens(s: str) -> set:
    return {w for w in norm(s).split() if len(w) >= 4 and w not in {"wind", "farm", "park"}}


async def main():
    S = get_session_factory()
    async with S() as db:
        rs = await db.execute(text("""
            SELECT m.id AS map_id, m.source, m.source_identifier, m.generation_unit_id,
                   m.windfarm_id, m.is_active AS map_active,
                   gu.name AS unit_name, gu.is_active AS unit_active,
                   wf.name AS wf_name,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = m.generation_unit_id) AS rows,
                   (SELECT SUM(generation_mwh)::float FROM generation_data WHERE generation_unit_id = m.generation_unit_id) AS gen
            FROM generation_unit_mapping m
            LEFT JOIN generation_units gu ON gu.id = m.generation_unit_id
            LEFT JOIN windfarms wf ON wf.id = m.windfarm_id
            ORDER BY m.source, wf.name, m.source_identifier
        """))
        all_rows = list(rs)
        print(f"Total mapping rows: {len(all_rows)}")
        by_source = {}
        for r in all_rows:
            by_source.setdefault(r.source, []).append(r)
        for s, rs_ in by_source.items():
            print(f"  {s}: {len(rs_)}")

        # Sense-check: print every row where source_identifier doesn't share a
        # token with windfarm name AND the unit has data
        print("\n" + "=" * 100)
        print("FLAGGED: mapping rows where identifier tokens don't intersect windfarm tokens")
        print("=" * 100)
        flagged = []
        for r in all_rows:
            id_tokens = tokens(r.source_identifier or "")
            wf_tokens = tokens(r.wf_name or "")
            if id_tokens and wf_tokens and not (id_tokens & wf_tokens):
                # Strip ENTSOE prefix
                ident = re.sub(r"^ENTSOE:|^NVE:|^ELEXON:|^EIA:", "", r.source_identifier or "")
                id_tokens2 = tokens(ident)
                if id_tokens2 & wf_tokens:
                    continue  # match after stripping
                flagged.append(r)
        print(f"  {len(flagged)} flagged")
        print(f"\n  {'src':<8}{'unit_name':<38}{'identifier':<48}{'wf':<28}{'rows':>9}{'gen MWh':>14}")
        for r in flagged:
            print(f"  {r.source:<8}{(r.unit_name or '')[:36]:<38}"
                  f"{(r.source_identifier or '')[:46]:<48}"
                  f"{(r.wf_name or '')[:26]:<28}"
                  f"{r.rows:>9,}{(r.gen or 0):>14,.0f}")

        # Also show ENTSOE mapping rows where unit_name CLEARLY indicates a different
        # windfarm than the one assigned.
        print("\n" + "=" * 100)
        print("ENTSOE unit_name vs assigned windfarm — token mismatches")
        print("=" * 100)
        for r in all_rows:
            if r.source != "ENTSOE":
                continue
            ut = tokens(r.unit_name or "")
            wt = tokens(r.wf_name or "")
            if ut and wt and not (ut & wt) and r.rows > 0:
                print(f"  unit {r.generation_unit_id} '{r.unit_name}' → wf {r.windfarm_id} '{r.wf_name}'"
                      f"  rows={r.rows:,} gen={(r.gen or 0):,.0f}")

        # Final: show what the "right" windfarm would be for each of the 8 confirmed
        # mislinked units, based on name-match candidates.
        print("\n" + "=" * 100)
        print("Proposed remediation: 8 confirmed-mislinked ENTSOE units")
        print("=" * 100)
        fix_table = [
            (12385, "Ormonde Eng Ltd",                   7385, "Hornsea 2",            7404, "Ormonde"),
            (12328, "ABRB0-1",                           7359, "Beatrice",             7350, "Aberdeen"),
            (12361, "Hornsea 1",                         7380, "Hollandse Kust Zuid",  7384, "Hornsea 1"),
            (12346, "East Anglia One",                   7370, "Dudgeon",              7371, "East Anglia One"),
            (12348, "Galloper Offshore Wind Farm GAOFO-1", 7374, "Gode Wind 1&2",      7373, "Galloper"),
            (12349, "Galloper Offshore Wind Farm GAOFO-2", 7374, "Gode Wind 1&2",      7373, "Galloper"),
            (12350, "Galloper Offshore Wind Farm GAOFO-3", 7374, "Gode Wind 1&2",      7373, "Galloper"),
            (12351, "Galloper Offshore Wind Farm GAOFO-4", 7374, "Gode Wind 1&2",      7373, "Galloper"),
        ]
        for uid, uname, wrong_wf, wrong_name, right_wf, right_name in fix_table:
            rs = await db.execute(text("""
                SELECT COUNT(*) AS rows, SUM(generation_mwh)::float AS gen
                FROM generation_data WHERE generation_unit_id = :u
            """), {"u": uid})
            r = rs.first()
            print(f"  unit {uid} {uname!r}")
            print(f"    {wrong_wf} {wrong_name!r} -> {right_wf} {right_name!r}")
            print(f"    impacted rows={r.rows:,} gen={(r.gen or 0):,.0f} MWh")


asyncio.run(main())
