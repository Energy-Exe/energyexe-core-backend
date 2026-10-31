#!/usr/bin/env python3
"""Audit the installed-capacity basis used by the performance pipeline (read-only).

Lists, for every windfarm that has performance data, how the capacity in force
(derived from `generation_units` dates, see `app/utils/capacity.py`) compares
with the windfarm's nameplate, and which registry problems block or distort it.

Outputs (CSV, one row per item) into --out:
  capacity_audit_farms.csv   one row per farm: basis status, nameplate vs today's
                             unit sum, capacity swing inside the data span for the
                             pipeline view (after ramp-up) and the CF view (all
                             hours), hours-before-any-unit flag.
  capacity_audit_units.csv   unit hygiene: NULL capacity, undated, start in the
                             future or before 1980, inactive without end date.
  capacity_audit_phases.csv  phases that start after the farm's ramp-up end and
                             have no unit-level ramp window (first_power /
                             ramp_up_end / COD all NULL) — their commissioning
                             months will read as underperformance.

Usage:
    DATABASE_URL=... poetry run python scripts/audit_capacity_basis.py \
        --out docs/reports/2026-10/capacity_audit
    poetry run python scripts/audit_capacity_basis.py --windfarm-ids 7213 7201 --out /tmp/cap
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import asyncpg
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.utils.capacity import (  # noqa: E402
    ANCHOR_TOLERANCE,
    UnitWindow,
    anchor_date,
    capacity_basis_status,
    capacity_series,
    installed_at,
    utc_midnight,
)

DB_URL = os.environ.get("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
if not DB_URL:
    raise SystemExit("set DATABASE_URL")


FARMS_SQL = """
SELECT w.id, w.name, w.nameplate_capacity_mw::float AS nameplate,
       COALESCE(w.ramp_up_end_date,
                w.commercial_operational_date + interval '2 months',
                w.first_power_date)::date AS ramp_end,
       s.y0, s.y1
FROM windfarms w
JOIN (SELECT windfarm_id, MIN(year) y0, MAX(year) y1
      FROM performance_summaries WHERE period_type = 'year' GROUP BY 1) s ON s.windfarm_id = w.id
WHERE ($1::int[] IS NULL OR w.id = ANY($1::int[]))
ORDER BY w.id
"""

UNITS_SQL = """
SELECT gu.id, gu.windfarm_id, gu.name, gu.capacity_mw::float AS capacity_mw, gu.is_active,
       gu.start_date, gu.end_date, gu.first_power_date, gu.commercial_operational_date,
       gu.ramp_up_end_date
FROM generation_units gu
WHERE gu.windfarm_id = ANY($1::int[])
ORDER BY gu.windfarm_id, COALESCE(gu.first_power_date, gu.start_date) NULLS FIRST, gu.id
"""


def _windows(units: List[asyncpg.Record]) -> List[UnitWindow]:
    out = []
    for u in units:
        if u["capacity_mw"] is None or u["capacity_mw"] <= 0:
            continue
        if not u["is_active"] and u["end_date"] is None:
            continue
        start = u["first_power_date"] or u["start_date"]
        out.append(
            UnitWindow(float(u["capacity_mw"]), utc_midnight(start), utc_midnight(u["end_date"]))
        )
    return out


def _breakpoints(
    windows: List[UnitWindow], span_start: pd.Timestamp, span_end: pd.Timestamp
) -> pd.Series:
    """Instants where capacity can change inside the span, plus both ends."""
    pts = {span_start, span_end}
    for w in windows:
        for b in (w.start, w.end):
            if b is not None and span_start <= b <= span_end:
                pts.add(b)
                pts.add(b - pd.Timedelta(hours=1))
    return pd.Series(sorted(p for p in pts if span_start <= p <= span_end))


def _swing(windows, span_start, span_end, nameplate, today, anchor) -> Dict[str, Optional[float]]:
    if span_end <= span_start:
        return {"min": None, "max": None, "n_levels": 0, "swing_pct": None, "hours_no_unit": False}
    pts = _breakpoints(windows, span_start, span_end)
    cap = capacity_series(windows, pts, nameplate, anchor=anchor, today=today)
    levels = sorted(set(round(float(v), 3) for v in cap))
    mx = max(levels)
    return {
        "min": min(levels),
        "max": mx,
        "n_levels": len(levels),
        "swing_pct": round(100.0 * (mx - min(levels)) / mx, 1) if mx else None,
        # raw == nameplate at a point where no unit is in force → S(h)=0 fallback
        "hours_no_unit": bool(any(installed_at(windows, p) <= 0 for p in pts)),
    }


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--windfarm-ids", nargs="*", type=int, default=None)
    ap.add_argument("--out", required=True)
    ap.add_argument("--today", default=None, help="anchor date override (YYYY-MM-DD)")
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    today = args.today or date.today().isoformat()

    conn = await asyncpg.connect(DB_URL)
    try:
        farms = await conn.fetch(FARMS_SQL, args.windfarm_ids)
        units = await conn.fetch(UNITS_SQL, [f["id"] for f in farms])
    finally:
        await conn.close()

    by_farm: Dict[int, List[asyncpg.Record]] = {}
    for u in units:
        by_farm.setdefault(u["windfarm_id"], []).append(u)

    farm_rows, unit_rows, phase_rows = [], [], []
    today_ts = utc_midnight(today)
    for f in farms:
        wf = f["id"]
        nameplate = float(f["nameplate"] or 0)
        us = by_farm.get(wf, [])
        windows = _windows(us)
        status = capacity_basis_status(windows, nameplate, today=today, windfarm_id=wf)
        s_anchor = installed_at(windows, anchor_date(windows, today=today)) if windows else 0.0
        diff_pct = (
            round(100.0 * (s_anchor / nameplate - 1.0), 2) if nameplate and s_anchor else None
        )

        span_start = pd.Timestamp(f"{f['y0']}-01-01", tz="UTC")
        span_end = min(pd.Timestamp(f"{f['y1']}-12-31 23:00", tz="UTC"), today_ts)
        ramp_end = utc_midnight(f["ramp_end"]) if f["ramp_end"] else None
        pipe_start = max(span_start, ramp_end) if ramp_end is not None else span_start

        cf = _swing(windows, span_start, span_end, nameplate, today, anchor=True)
        pipe = _swing(windows, pipe_start, span_end, nameplate, today, anchor=True)
        farm_rows.append(
            {
                "windfarm_id": wf,
                "name": f["name"],
                "nameplate_mw": nameplate,
                "n_units": len(us),
                "n_windows": len(windows),
                "basis_status": status,
                "units_in_force_today_mw": round(s_anchor, 3),
                "today_vs_nameplate_pct": diff_pct,
                "blocked_reason": (
                    "no sized/dated units"
                    if status == "no_units"
                    else f"today's unit sum differs from nameplate by > {ANCHOR_TOLERANCE:.0%}"
                    if status == "blocked"
                    else ""
                ),
                "data_years": f"{f['y0']}-{f['y1']}",
                "ramp_end": f["ramp_end"].isoformat() if f["ramp_end"] else "",
                "pipeline_cap_min_mw": pipe["min"],
                "pipeline_cap_max_mw": pipe["max"],
                "pipeline_levels": pipe["n_levels"],
                "pipeline_swing_pct": pipe["swing_pct"],
                "cf_view_cap_min_mw": cf["min"],
                "cf_view_cap_max_mw": cf["max"],
                "cf_view_levels": cf["n_levels"],
                "cf_view_swing_pct": cf["swing_pct"],
                # S(h)=0 inside the pipeline span means post-ramp-up hours fall back
                # to nameplate (registry gap); before first power it is expected.
                "pipeline_hours_before_any_unit": pipe["hours_no_unit"],
                "moves_in_pipeline": status == "in_force" and pipe["n_levels"] > 1,
                "moves_in_cf_views": status == "in_force" and cf["n_levels"] > 1,
            }
        )

        for u in us:
            start = u["first_power_date"] or u["start_date"]
            problems = []
            if u["capacity_mw"] is None or u["capacity_mw"] <= 0:
                problems.append("null_or_zero_capacity")
            if start is None:
                problems.append("undated")
            elif start > date.fromisoformat(today):
                problems.append("start_in_future")
            elif start.year < 1980:
                problems.append("start_before_1980")
            if not u["is_active"] and u["end_date"] is None:
                problems.append("inactive_without_end_date")
            if u["end_date"] and start and u["end_date"] <= start:
                problems.append("end_not_after_start")
            if problems:
                unit_rows.append(
                    {
                        "windfarm_id": wf,
                        "windfarm": f["name"],
                        "unit_id": u["id"],
                        "unit": u["name"],
                        "capacity_mw": u["capacity_mw"],
                        "is_active": u["is_active"],
                        "start_date": u["start_date"],
                        "first_power_date": u["first_power_date"],
                        "end_date": u["end_date"],
                        "problems": ";".join(problems),
                    }
                )
            # (e) phases after farm ramp-up with no ramp window of their own
            if (
                start is not None
                and ramp_end is not None
                and utc_midnight(start) > ramp_end
                and u["first_power_date"] is None
                and u["ramp_up_end_date"] is None
                and u["commercial_operational_date"] is None
                and u["capacity_mw"]
            ):
                phase_rows.append(
                    {
                        "windfarm_id": wf,
                        "windfarm": f["name"],
                        "unit_id": u["id"],
                        "unit": u["name"],
                        "capacity_mw": u["capacity_mw"],
                        "phase_start": start,
                        "farm_ramp_end": f["ramp_end"],
                        "note": (
                            "no unit-level first_power/ramp_up_end/COD → commissioning "
                            "months read as underperformance"
                        ),
                    }
                )

    def _write(name: str, rows: List[dict]) -> None:
        path = out_dir / name
        if not rows:
            path.write_text("")
            return
        with path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    _write("capacity_audit_farms.csv", farm_rows)
    _write("capacity_audit_units.csv", unit_rows)
    _write("capacity_audit_phases.csv", phase_rows)

    df = pd.DataFrame(farm_rows)
    print(f"farms with performance data: {len(df)}")
    print(f"basis status counts: {df.basis_status.value_counts().to_dict()}")
    print(f"moves in pipeline: {int(df.moves_in_pipeline.sum())}")
    print(f"moves in CF views: {int(df.moves_in_cf_views.sum())}")
    print(f"unit hygiene rows: {len(unit_rows)}")
    print(f"phases without ramp window: {len(phase_rows)}")
    print("\nBLOCKED:")
    print(
        df[df.basis_status == "blocked"][
            [
                "windfarm_id",
                "name",
                "nameplate_mw",
                "units_in_force_today_mw",
                "today_vs_nameplate_pct",
            ]
        ].to_string(index=False)
    )
    print("\nMOVES IN PIPELINE (anchored):")
    cols = [
        "windfarm_id",
        "name",
        "nameplate_mw",
        "pipeline_cap_min_mw",
        "pipeline_cap_max_mw",
        "pipeline_swing_pct",
    ]
    print(
        df[df.moves_in_pipeline]
        .sort_values("pipeline_swing_pct", ascending=False)[cols]
        .to_string(index=False)
    )
    print(f"\nwritten to {out_dir}")


if __name__ == "__main__":
    asyncio.run(main())
