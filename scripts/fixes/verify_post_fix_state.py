"""Read-only verification of the post-fix state for the 10 windfarms on the
team checklist. Shows what /comparison and /windfarms/* charts now return."""
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


CHECKS = [
    # (wf_id, wf_name, period_label, start, end, expectation_after_fix)
    (7385, "Hornsea 2",            "2014-2021", "2014-01-01", "2022-01-01", "flat zero (commissioned 2022)"),
    (7370, "Dudgeon",              "2019-2021", "2019-01-01", "2022-01-01", "ELEXON only"),
    (7380, "Hollandse Kust Zuid",  "2019-2021", "2019-01-01", "2022-01-01", "flat zero (commissioned 2023)"),
    (7374, "Gode Wind 1&2",        "Jan-May 2021", "2021-01-01", "2021-06-01", "flat zero (German farm)"),
    (7359, "Beatrice",             "2019-2021", "2019-01-01", "2022-01-01", "ELEXON only"),
    (7404, "Ormonde",              "2014-2021", "2014-01-01", "2022-01-01", "ELEXON + new ENTSOE parallel"),
    (7384, "Hornsea 1",            "2019-2020", "2019-01-01", "2021-01-01", "ELEXON + new ENTSOE 2019 ramp"),
    (7371, "East Anglia One",      "2019",         "2019-01-01", "2020-01-01", "ENTSOE commissioning curve"),
    (7350, "Aberdeen",             "2019-2021", "2019-01-01", "2022-01-01", "ELEXON + new ENTSOE parallel"),
    (7373, "Galloper",             "Jan-May 2021", "2021-01-01", "2021-06-01", "ELEXON + ENTSOE near-identical"),
]


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def main():
    S = get_session_factory()
    async with S() as db:
        banner("POST-FIX state: per windfarm, per source rows and gen in the team-checklist period")
        print(f"\n  {'#':>3}{'wf_id':>7}  {'wf_name':<26}{'period':<14}{'source':<9}"
              f"{'rows':>10}{'gen MWh':>14}  expected after fix")
        print("  " + "-" * 130)
        for i, (wf, name, label, start, end, expect) in enumerate(CHECKS, 1):
            s_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
            e_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
            rs = await db.execute(text("""
                SELECT gd.source, gu.is_active, COUNT(*) AS n,
                       SUM(gd.generation_mwh)::float AS gen
                FROM generation_data gd
                JOIN generation_units gu ON gu.id = gd.generation_unit_id
                WHERE gd.windfarm_id = :w
                  AND gd.hour >= :s
                  AND gd.hour <  :e
                GROUP BY 1, 2
                ORDER BY 1
            """), {"w": wf, "s": s_dt, "e": e_dt})
            rows = list(rs)
            if not rows:
                print(f"  {i:>3}{wf:>7}  {name[:24]:<26}{label:<14}{'(empty)':<9}"
                      f"{'-':>10}{'-':>14}  {expect}")
            for j, r in enumerate(rows):
                marker = f"{i:>3}{wf:>7}  {name[:24]:<26}{label:<14}" if j == 0 else " " * 53
                print(f"  {marker}{r.source:<9}{r.n:>10,}{(r.gen or 0):>14,.0f}"
                      + (f"  {expect}" if j == 0 else ""))


asyncio.run(main())
