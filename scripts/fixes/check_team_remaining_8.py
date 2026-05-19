"""Inspect the 8 reconnected ENTSOE units the team is still flagging.

Their CSV shows the units with empty status/capacity/dates columns. Most likely
they're being flagged because metadata is NULL on the unit row, not because of
the windfarm attribution (which we fixed on 2026-05-12).

For each unit, report:
  - is_active, windfarm_id, attached windfarm name (confirm reattachment held)
  - capacity_mw, start_date, end_date, first_power_date, commercial_operational_date
  - generation_data row count and date range
  - active sibling units on the same windfarm (so we can copy metadata from them)
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


UNITS = [
    # (unit_id, expected_correct_wf, label)
    (12328, 7350, "ABRB0-1 → Aberdeen"),
    (12346, 7371, "East Anglia One"),
    (12348, 7373, "Galloper GAOFO-1"),
    (12349, 7373, "Galloper GAOFO-2"),
    (12350, 7373, "Galloper GAOFO-3"),
    (12351, 7373, "Galloper GAOFO-4"),
    (12361, 7384, "Hornsea 1"),
    (12385, 7404, "Ormonde Eng Ltd"),
]


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def main():
    S = get_session_factory()
    async with S() as db:
        for uid, expected_wf, label in UNITS:
            banner(f"unit {uid} ({label})")

            rs = await db.execute(text("""
                SELECT gu.id, gu.source, gu.code, gu.name, gu.is_active,
                       gu.capacity_mw, gu.windfarm_id,
                       gu.start_date, gu.end_date, gu.first_power_date,
                       gu.commercial_operational_date,
                       w.name AS wf_name, w.status AS wf_status,
                       w.nameplate_capacity_mw AS wf_cap,
                       w.first_power_date AS wf_fpd,
                       w.commercial_operational_date AS wf_cod,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n_rows,
                       (SELECT MIN(hour) FROM generation_data WHERE generation_unit_id = gu.id) AS lo,
                       (SELECT MAX(hour) FROM generation_data WHERE generation_unit_id = gu.id) AS hi,
                       (SELECT SUM(generation_mwh)::float FROM generation_data WHERE generation_unit_id = gu.id) AS gen
                FROM generation_units gu
                LEFT JOIN windfarms w ON w.id = gu.windfarm_id
                WHERE gu.id = :u
            """), {"u": uid})
            u = rs.first()
            ok_wf = "✓" if u.windfarm_id == expected_wf else "✗"
            print(f"  attribution: wf={u.windfarm_id} '{u.wf_name}' (expected {expected_wf}) {ok_wf}")
            print(f"  is_active={u.is_active}  source={u.source}  code={u.code}")
            print(f"  UNIT metadata:")
            print(f"    capacity_mw={u.capacity_mw}  start_date={u.start_date}  end_date={u.end_date}")
            print(f"    first_power_date={u.first_power_date}  commercial_operational_date={u.commercial_operational_date}")
            print(f"  WINDFARM metadata (target):")
            print(f"    status={u.wf_status}  cap={u.wf_cap}  fpd={u.wf_fpd}  cod={u.wf_cod}")
            print(f"  generation_data: {u.n_rows:,} rows  ({u.lo} → {u.hi})  gen={(u.gen or 0):,.0f} MWh")

            # Active sibling units on same windfarm (to compare metadata)
            rs = await db.execute(text("""
                SELECT id, source, name, is_active, capacity_mw,
                       start_date, end_date, first_power_date, commercial_operational_date,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n
                FROM generation_units gu
                WHERE windfarm_id = :w AND id <> :u
                ORDER BY is_active DESC, id
            """), {"w": expected_wf, "u": uid})
            siblings = list(rs)
            print(f"\n  Siblings on wf {expected_wf} ({len(siblings)} units):")
            for s in siblings[:8]:
                print(f"    id={s.id:>5} src={s.source:<8} active={s.is_active} cap={s.capacity_mw} "
                      f"start={s.start_date} fpd={s.first_power_date} cod={s.commercial_operational_date} "
                      f"rows={s.n:,}  '{s.name[:30]}'")


asyncio.run(main())
