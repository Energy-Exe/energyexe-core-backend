"""Delete the 12 inactive Raggovidda orphan units (12696-12707).

Why these are safe to delete:
- 12696..12706: 11 NVE code='46' inactive units modelling the 2021-2022 ramp-up
  of the second windfarm. The 2nd windfarm's actual NVE code is 1090, so
  these units carry the wrong code and were never canonical.
- 12707: 'Raggovidda 2 - Do not use' (windfarm_id=NULL, is_active=False).
- Verified zero referencing rows in: generation_data, data_anomalies,
  generation_unit_mapping for all 12 ids.
- Active units 12695 (Raggovidda, code 46, 45 MW) and 12805 (Raggovidda 2,
  code 1090, 51.6 MW) carry all real attribution.

Run:
    poetry run python scripts/fixes/delete_raggovidda_phase_units.py            # dry run
    poetry run python scripts/fixes/delete_raggovidda_phase_units.py --execute
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory

PHASE_UNIT_IDS = list(range(12696, 12708))  # 12696..12707 = 12 units (11 phases + 12707 'Do not use')
CHILD_TABLES = [
    ('generation_data', 'generation_unit_id'),
    ('data_anomalies', 'generation_unit_id'),
    ('generation_unit_mapping', 'generation_unit_id'),
]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true', help='Actually delete')
    args = parser.parse_args()

    S = get_session_factory()
    async with S() as db:
        rs = await db.execute(text("""
            SELECT id, name, source, code, capacity_mw::float AS cap, is_active,
                   windfarm_id, start_date, end_date
            FROM generation_units
            WHERE id = ANY(:ids)
            ORDER BY id
        """), {'ids': PHASE_UNIT_IDS})
        units = list(rs)

        if len(units) != len(PHASE_UNIT_IDS):
            found_ids = {u.id for u in units}
            missing = [i for i in PHASE_UNIT_IDS if i not in found_ids]
            print(f"WARN: missing units {missing} — already deleted?")

        print(f"\nUnits targeted for deletion ({len(units)}):")
        for u in units:
            print(f"  id={u.id} {u.name!r:38} cap={u.cap:5.2f} act={u.is_active} "
                  f"wf={u.windfarm_id} start={u.start_date} end={u.end_date}")

        print(f"\nReferencing-row checks:")
        any_blocking = False
        for tbl, col in CHILD_TABLES:
            rs = await db.execute(text(f"""
                SELECT COUNT(*) AS n FROM {tbl} WHERE {col} = ANY(:ids)
            """), {'ids': PHASE_UNIT_IDS})
            n = rs.scalar()
            print(f"  {tbl}: {n} rows reference these units")
            if n > 0:
                any_blocking = True

        if any_blocking:
            print("\nABORT: child rows exist; would fail FK constraint. Investigate before deleting.")
            return

        if not args.execute:
            print("\nDry run only. Re-run with --execute to delete.")
            return

        rs = await db.execute(text("""
            DELETE FROM generation_units WHERE id = ANY(:ids)
        """), {'ids': PHASE_UNIT_IDS})
        await db.commit()
        print(f"\nDeleted {rs.rowcount} rows from generation_units.")


asyncio.run(main())
