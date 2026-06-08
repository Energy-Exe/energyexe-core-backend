"""Read-only: compare OLD (nameplate p_pu) vs NEW (capacity-aware p_pu_cap)
structural-constraint detection for affected windfarms. No DB writes."""

import asyncio
import sys

import numpy as np

sys.path.insert(0, ".")

FARMS = [7200, 7272, 7299, 7259, 7201, 7254]


def show(tag, runs):
    if runs is None or runs.empty:
        print(f"  {tag}: (none)")
        return
    print(f"  {tag}: {len(runs)} run(s)")
    for _, r in runs.sort_values("period_start").iterrows():
        ps = str(r["period_start"])[:10]
        pe = str(r["period_end"])[:10]
        print(
            f"    {ps} -> {pe}  dur={int(r['duration_hours']):>6}h  "
            f"q90={r['mean_q90_ratio']:.2f} q50={r['mean_q50_ratio']:.2f}  "
            f"bins={int(r['wind_bins_affected'])}  trig={r['flag_trigger']}"
        )


async def main():
    from sqlalchemy import select
    from app.core.database import get_session_factory
    from app.models.windfarm import Windfarm
    from app.services.power_curve_service import PowerCurveService
    from app.services.structural_constraint_detection_service import detect_constraints_df

    Session = get_session_factory()
    async with Session() as db:
        for wf_id in FARMS:
            row = (
                await db.execute(
                    select(Windfarm.name, Windfarm.nameplate_capacity_mw).where(
                        Windfarm.id == wf_id
                    )
                )
            ).first()
            name, nameplate = row[0], float(row[1])
            pcs = PowerCurveService(db)
            df = await pcs._load_hourly_data(
                wf_id, None, None, nameplate, include_capacity_norm=True
            )
            df["wind_bin"] = np.floor(df["wind_speed"]).astype(float)

            old_runs = detect_constraints_df(df.copy())  # uses nameplate p_pu

            df_cap = df.copy()
            df_cap["p_pu"] = df_cap["p_pu_cap"]
            new_runs = detect_constraints_df(df_cap)  # uses capacity-aware p_pu

            # capacity context
            cap_min = float(np.nanmin(df["online_capacity_mw"])) if "online_capacity_mw" in df else 0
            cap_max = float(np.nanmax(df["online_capacity_mw"])) if "online_capacity_mw" in df else 0
            print(
                f"\n=== {wf_id} {name}  nameplate={nameplate:.0f}  "
                f"online_cap range≈[{cap_min:.0f}..{cap_max:.0f}] ==="
            )
            show("OLD (nameplate)", old_runs)
            show("NEW (capacity) ", new_runs)


if __name__ == "__main__":
    asyncio.run(main())
