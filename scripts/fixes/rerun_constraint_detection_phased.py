"""Re-run Module 1b structural-constraint detection with capacity-aware
normalisation for windfarms whose flags were phase build-out artifacts.

Background
----------
The detector normalised output by static nameplate capacity, so a phased
windfarm producing normally during its build-out (only part of its final
capacity installed) looked systematically low vs the leave-one-year-out
reference (which was dominated by the later full-capacity years) and was
flagged as a structural constraint.

The fix (power_curve_service._load_hourly_data include_capacity_norm=True)
normalises by the capacity ONLINE each hour (p_pu_cap), so build-out is no
longer mistaken for a constraint, while a genuine suppression during that
period (units online but output truncated) still drops below the per-bin
reference and is detected.

This script replays detection ONLY (Module 1b) for the affected farms using
p_pu_cap. detect_constraints(replace_existing=True) deletes prior
pending_review auto-detections and re-inserts only what the fixed detector
finds; confirmed and dismissed flags are preserved.

Usage
-----
    poetry run python scripts/fixes/rerun_constraint_detection_phased.py            # dry-run (rolled back)
    poetry run python scripts/fixes/rerun_constraint_detection_phased.py --commit   # apply
"""

import argparse
import asyncio
import sys

import numpy as np

sys.path.insert(0, ".")

# 13 of the 14 phase-artifact farms. 8767 (METCentre Karmoy) is excluded: it has
# 0% per-hour capacity coverage AND no usable registered unit capacity, so the
# capacity-aware fix falls back to nameplate there (no improvement), and its only
# artifact flag is already dismissed. Re-running it would risk re-creating a
# pending artifact, so we leave its dismissed flag untouched.
AFFECTED = [7190, 7200, 7201, 7206, 7213, 7245, 7254, 7259, 7272, 7299, 7396, 7429, 7434]


async def _status_counts(db, wf_id):
    from sqlalchemy import text

    rows = (
        await db.execute(
            text(
                "SELECT review_status, count(*) FROM structural_constraint_flags "
                "WHERE windfarm_id=:w GROUP BY review_status"
            ),
            {"w": wf_id},
        )
    ).fetchall()
    m = {r[0]: r[1] for r in rows}
    return m.get("pending_review", 0), m.get("confirmed", 0), m.get("dismissed", 0)


async def main(commit: bool):
    from sqlalchemy import select
    from app.core.database import get_session_factory
    from app.models.windfarm import Windfarm
    from app.services.power_curve_service import PowerCurveService
    from app.services.structural_constraint_detection_service import (
        StructuralConstraintDetectionService,
    )

    Session = get_session_factory()
    tot_deleted = tot_inserted = tot_preserved = 0

    async with Session() as db:
        print(f"{'wf':>5}  {'name':24} {'nplate':>7}  before(P/C/D)   detected ins prsv   after(P/C/D)")
        print("-" * 96)
        for wf_id in AFFECTED:
            row = (
                await db.execute(
                    select(Windfarm.name, Windfarm.nameplate_capacity_mw).where(
                        Windfarm.id == wf_id
                    )
                )
            ).first()
            if not row or not row[1] or row[1] <= 0:
                print(f"{wf_id:>5}  (no windfarm / nameplate) — skip")
                continue
            name, nameplate = row[0], float(row[1])

            pb, cb, dbf = await _status_counts(db, wf_id)

            pcs = PowerCurveService(db)
            df = await pcs._load_hourly_data(
                wf_id, None, None, nameplate, include_capacity_norm=True
            )
            if df.empty:
                print(f"{wf_id:>5}  {name[:24]:24} {nameplate:>7.0f}  no hourly data — skip")
                continue
            df["wind_bin"] = np.floor(df["wind_speed"]).astype(float)
            if "p_pu_cap" in df.columns:
                df["p_pu"] = df["p_pu_cap"]  # detect on capacity-aware output

            detector = StructuralConstraintDetectionService(db)
            out = await detector.detect_constraints(wf_id, df, replace_existing=True)

            pa, ca, da = await _status_counts(db, wf_id)
            deleted = pb - (pa - out["runs_inserted"])  # old pending removed
            tot_deleted += max(0, deleted)
            tot_inserted += out["runs_inserted"]
            tot_preserved += out["runs_preserved"]

            print(
                f"{wf_id:>5}  {name[:24]:24} {nameplate:>7.0f}  "
                f"{pb:>3}/{cb}/{dbf:<3}      "
                f"{out['runs_detected']:>5} {out['runs_inserted']:>3} {out['runs_preserved']:>4}   "
                f"{pa:>3}/{ca}/{da}"
            )

        print("-" * 96)
        print(
            f"TOTAL old-pending-removed≈{tot_deleted}  new-pending-inserted={tot_inserted}  "
            f"reviewed-preserved={tot_preserved}"
        )
        if commit:
            await db.commit()
            print("\n*** COMMITTED ***")
        else:
            await db.rollback()
            print("\nDRY-RUN (rolled back). Re-run with --commit to apply.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true", help="commit changes (default: dry-run)")
    args = ap.parse_args()
    asyncio.run(main(args.commit))
