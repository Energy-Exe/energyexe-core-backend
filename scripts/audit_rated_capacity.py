"""Audit `windfarms.nameplate_capacity_mw` for the 229 windfarms with power
curves built (spec item 2.1).

Per Prioritisation 2026-03-30 Module 1: `rated_mw` is THE basis for
normalising power output to p.u. (`p_pu = power_mw / rated_mw`). If it's
wrong, every downstream metric — ODI, capture ratio, degradation slope,
wind-norm index — is wrong too.

This script flags:
  1. Windfarms with power curves but `nameplate_capacity_mw` IS NULL
  2. Windfarms where `nameplate_capacity_mw` does not match the SUM of
     turbine_units' rated capacities (when both are populated)
  3. Windfarms whose observed peak generation hour exceeded the recorded
     `nameplate_capacity_mw` by >5% (suggests rated capacity is too low)
  4. Windfarms whose observed peak generation < 50% of recorded
     `nameplate_capacity_mw` over a year (suggests rated capacity is too
     high, or persistent under-performance — flagged for human review).

Output: CSV at `/tmp/rated_capacity_audit.csv` with one row per windfarm.

Usage:
    poetry run python scripts/audit_rated_capacity.py
    poetry run python scripts/audit_rated_capacity.py --windfarm-id 7361
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
from typing import List, Optional

import asyncpg


DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:Energyexe1*@energyexedb.cn8a6ka2u5c3.eu-north-1.rds.amazonaws.com:5432/energyexe_db",
).replace("postgresql+asyncpg://", "postgresql://")

OUT_CSV = "/tmp/rated_capacity_audit.csv"


async def get_processed_windfarm_ids(conn: asyncpg.Connection) -> List[int]:
    rows = await conn.fetch(
        "SELECT DISTINCT windfarm_id FROM power_curve_bins ORDER BY windfarm_id"
    )
    return [r["windfarm_id"] for r in rows]


async def audit_windfarm(
    conn: asyncpg.Connection, windfarm_id: int
) -> dict:
    """Run all 4 checks for one windfarm. Returns one row of audit data."""
    wf = await conn.fetchrow(
        """
        SELECT id, name, code, nameplate_capacity_mw, status
        FROM windfarms WHERE id = $1
        """,
        windfarm_id,
    )
    if wf is None:
        return {
            "windfarm_id": windfarm_id,
            "issue": "WINDFARM_NOT_FOUND",
        }

    wf_cap = float(wf["nameplate_capacity_mw"]) if wf["nameplate_capacity_mw"] else None

    # Sum of turbine rated capacities (rated_power_kw → MW)
    turbine_sum = await conn.fetchval(
        """
        SELECT SUM(tm.rated_power_kw / 1000.0)::float
        FROM turbine_units tu
        JOIN turbine_models tm ON tm.id = tu.turbine_model_id
        WHERE tu.windfarm_id = $1
          AND tm.rated_power_kw IS NOT NULL
        """,
        windfarm_id,
    )

    # Peak generation hour observed
    peak = await conn.fetchval(
        """
        SELECT MAX(hourly_gen)::float FROM (
            SELECT hour, SUM(generation_mwh) AS hourly_gen
            FROM generation_data
            WHERE windfarm_id = $1
              AND generation_mwh IS NOT NULL
            GROUP BY hour
        ) sub
        """,
        windfarm_id,
    )

    issues = []

    # Check 1: nameplate is NULL
    if wf_cap is None:
        issues.append("MISSING_NAMEPLATE")

    # Check 2: nameplate vs turbine sum mismatch (>10% drift)
    nameplate_vs_turbine_pct = None
    if wf_cap is not None and turbine_sum is not None and turbine_sum > 0:
        diff_pct = abs(wf_cap - turbine_sum) / turbine_sum * 100
        nameplate_vs_turbine_pct = round(diff_pct, 1)
        if diff_pct > 10:
            issues.append(f"MISMATCH_VS_TURBINES_{diff_pct:.0f}%")

    # Check 3: observed peak > nameplate × 1.05
    peak_vs_nameplate_pct = None
    if wf_cap is not None and peak is not None and wf_cap > 0:
        ratio = peak / wf_cap * 100
        peak_vs_nameplate_pct = round(ratio, 1)
        if ratio > 105:
            issues.append(f"OBSERVED_PEAK_EXCEEDS_NAMEPLATE_{ratio:.0f}%")
        elif ratio < 50:
            issues.append(f"PEAK_FAR_BELOW_NAMEPLATE_{ratio:.0f}%")

    return {
        "windfarm_id": wf["id"],
        "name": wf["name"],
        "code": wf["code"],
        "status": wf["status"],
        "nameplate_capacity_mw": wf_cap,
        "turbine_sum_mw": round(turbine_sum, 2) if turbine_sum else None,
        "observed_peak_mwh": round(peak, 3) if peak else None,
        "nameplate_vs_turbine_pct_diff": nameplate_vs_turbine_pct,
        "peak_vs_nameplate_pct": peak_vs_nameplate_pct,
        "issue": " | ".join(issues) if issues else "OK",
    }


async def main(windfarm_id: Optional[int] = None) -> None:
    conn = await asyncpg.connect(DB_URL)
    try:
        if windfarm_id:
            wf_ids = [windfarm_id]
        else:
            wf_ids = await get_processed_windfarm_ids(conn)
        print(f"Auditing {len(wf_ids)} windfarm(s)...")

        rows = []
        for i, wf_id in enumerate(wf_ids):
            try:
                r = await audit_windfarm(conn, wf_id)
                rows.append(r)
                if i % 25 == 0 and i > 0:
                    print(f"  ...processed {i}/{len(wf_ids)}")
            except Exception as exc:
                print(f"  ERROR for {wf_id}: {exc}")
                rows.append({"windfarm_id": wf_id, "issue": f"AUDIT_ERROR: {exc}"})

        # Write CSV
        if rows:
            fieldnames = [
                "windfarm_id", "name", "code", "status",
                "nameplate_capacity_mw", "turbine_sum_mw", "observed_peak_mwh",
                "nameplate_vs_turbine_pct_diff", "peak_vs_nameplate_pct",
                "issue",
            ]
            with open(OUT_CSV, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            print(f"\nWrote {len(rows)} rows to {OUT_CSV}")

        # Summary
        ok = sum(1 for r in rows if r.get("issue") == "OK")
        flagged = len(rows) - ok
        print(f"\nSummary: {ok} OK, {flagged} flagged for review")
        if flagged:
            print("\nFlagged windfarms:")
            for r in rows:
                if r.get("issue") and r["issue"] != "OK":
                    print(
                        f"  [{r['windfarm_id']}] {r.get('name', '?'):40s} "
                        f"-> {r['issue']}"
                    )
    finally:
        await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--windfarm-id", type=int, default=None,
        help="Audit a single windfarm (default: all with power curves)",
    )
    args = parser.parse_args()
    asyncio.run(main(windfarm_id=args.windfarm_id))
