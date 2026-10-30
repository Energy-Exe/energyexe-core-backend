#!/usr/bin/env python3
"""Snapshot and compare performance-pipeline outputs for a set of windfarms.

Used for before/after verification of pipeline changes (capacity basis,
EPR-184 follow-up). Read-only against the database; writes CSVs.

    # before the change
    poetry run python scripts/jobs/snapshot_pipeline_outputs.py snapshot \
        --windfarm-ids 7213 7201 7182 --out /tmp/snap/pre [--fleet]
    # after the re-run
    poetry run python scripts/jobs/snapshot_pipeline_outputs.py snapshot \
        --windfarm-ids 7213 7201 7182 --out /tmp/snap/post [--fleet]
    poetry run python scripts/jobs/snapshot_pipeline_outputs.py compare \
        --before /tmp/snap/pre --after /tmp/snap/post --closed-years-only \
        --controls 7182

Tables: performance_summaries (ODI + normalisation + commercial columns),
power_curve_bins, performance_anomalies (aggregated per year/type),
degradation_results, constraint_loss_summaries, opportunities. ``--fleet``
also snapshots the yearly performance_summaries rows for every windfarm.

`compare` writes one compare_<table>.csv per table into --after and prints,
per windfarm and table, how many rows changed and the largest relative
change. Control farms that moved are listed explicitly — on an anchored
capacity basis a constant-capacity farm must be byte-identical.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

import asyncpg
import pandas as pd

DB_URL = os.environ.get("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")

# table -> (sql, key columns, value columns)
TABLES: Dict[str, Tuple[str, List[str], List[str]]] = {
    "performance_summaries": (
        """
        SELECT windfarm_id, period_type, year, month,
               total_hours, underperf_hours, overperf_hours, odi_pct_underperf,
               lost_mwh, expected_mwh, odi_pct_loss_mwh, lost_eur,
               norm_ratio_p50, norm_index_p50, norm_ratio_p10, norm_index_p10,
               constraint_proxy_mwh, lost_value_eur, contract_revenue_eur,
               contract_revenue_vs_p50_target_eur
        FROM performance_summaries WHERE windfarm_id = ANY($1::int[])
        ORDER BY windfarm_id, period_type, year, month
        """,
        ["windfarm_id", "period_type", "year", "month"],
        [
            "total_hours",
            "underperf_hours",
            "overperf_hours",
            "odi_pct_underperf",
            "lost_mwh",
            "expected_mwh",
            "odi_pct_loss_mwh",
            "lost_eur",
            "norm_ratio_p50",
            "norm_index_p50",
            "norm_ratio_p10",
            "norm_index_p10",
            "constraint_proxy_mwh",
            "lost_value_eur",
            "contract_revenue_eur",
            "contract_revenue_vs_p50_target_eur",
        ],
    ),
    "power_curve_bins": (
        """
        SELECT windfarm_id, curve_type, year, wind_bin,
               q50_pu, q90_pu, mean_pu, mad_pu, sample_count
        FROM power_curve_bins WHERE windfarm_id = ANY($1::int[])
        ORDER BY windfarm_id, curve_type, year, wind_bin
        """,
        ["windfarm_id", "curve_type", "year", "wind_bin"],
        ["q50_pu", "q90_pu", "mean_pu", "mad_pu", "sample_count"],
    ),
    "performance_anomalies": (
        """
        SELECT windfarm_id, EXTRACT(YEAR FROM hour)::int AS year, anomaly_type,
               COUNT(*) AS n_hours, SUM(lost_mwh) AS lost_mwh, SUM(lost_eur) AS lost_eur,
               AVG(actual_p_pu) AS avg_actual_p_pu
        FROM performance_anomalies WHERE windfarm_id = ANY($1::int[])
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
        """,
        ["windfarm_id", "year", "anomaly_type"],
        ["n_hours", "lost_mwh", "lost_eur", "avg_actual_p_pu"],
    ),
    "degradation_results": (
        """
        SELECT windfarm_id, reference_curve, analysis_start, analysis_end, data_points,
               slope_pct_per_year, r_squared, baseline_cap_pu, n_constraint_hours_excluded
        FROM degradation_results WHERE windfarm_id = ANY($1::int[])
        ORDER BY windfarm_id, reference_curve
        """,
        ["windfarm_id", "reference_curve"],
        [
            "data_points",
            "slope_pct_per_year",
            "r_squared",
            "baseline_cap_pu",
            "n_constraint_hours_excluded",
        ],
    ),
    "constraint_loss_summaries": (
        """
        SELECT windfarm_id, reference_curve, period_start, period_end, duration_hours,
               actual_mwh, expected_mwh, lost_mwh, lost_eur
        FROM constraint_loss_summaries WHERE windfarm_id = ANY($1::int[])
        ORDER BY windfarm_id, reference_curve, period_start
        """,
        ["windfarm_id", "reference_curve", "period_start"],
        ["duration_hours", "actual_mwh", "expected_mwh", "lost_mwh", "lost_eur"],
    ),
    "opportunities": (
        """
        SELECT windfarm_id, schema_code, COALESCE(branch, '') AS branch, detection_period_start,
               severity, status
        FROM opportunities WHERE windfarm_id = ANY($1::int[])
        ORDER BY windfarm_id, schema_code, branch, detection_period_start
        """,
        ["windfarm_id", "schema_code", "branch", "detection_period_start"],
        ["severity", "status"],
    ),
}

FLEET_SQL = """
SELECT windfarm_id, year, norm_index_p50, norm_ratio_p50, lost_mwh, expected_mwh,
       odi_pct_loss_mwh, constraint_proxy_mwh
FROM performance_summaries WHERE period_type = 'year' ORDER BY windfarm_id, year
"""


async def snapshot(args: argparse.Namespace) -> None:
    if not DB_URL:
        raise SystemExit("set DATABASE_URL")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    conn = await asyncpg.connect(DB_URL)
    try:
        for name, (sql, _k, _v) in TABLES.items():
            rows = await conn.fetch(sql, args.windfarm_ids)
            df = pd.DataFrame([dict(r) for r in rows])
            df.to_csv(out / f"{name}.csv", index=False)
            print(f"{name}: {len(df)} rows")
        if args.fleet:
            rows = await conn.fetch(FLEET_SQL)
            pd.DataFrame([dict(r) for r in rows]).to_csv(out / "fleet_yearly.csv", index=False)
            print(f"fleet_yearly: {len(rows)} rows")
    finally:
        await conn.close()
    (out / "META").write_text(
        f"snapshot {date.today().isoformat()}\nwindfarm_ids {args.windfarm_ids}\n"
    )
    print(f"written to {out}")


def _load(dir_: Path, name: str) -> pd.DataFrame:
    p = dir_ / f"{name}.csv"
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(p)


def _compare_table(
    name: str,
    before: pd.DataFrame,
    after: pd.DataFrame,
    keys: List[str],
    values: List[str],
    closed_years_only: bool,
) -> pd.DataFrame:
    if before.empty and after.empty:
        return pd.DataFrame()
    cols = keys + [v for v in values if v in before.columns or v in after.columns]
    before = before.reindex(columns=cols)
    after = after.reindex(columns=cols)
    if closed_years_only and "year" in keys:
        this_year = date.today().year
        before = before[before["year"].isna() | (before["year"] < this_year)]
        after = after[after["year"].isna() | (after["year"] < this_year)]
    # NaN-safe key join
    for df in (before, after):
        for k in keys:
            df[k] = df[k].astype(str)
    m = before.merge(after, on=keys, how="outer", suffixes=("_before", "_after"), indicator=True)
    out_rows = []
    for v in values:
        b, a = f"{v}_before", f"{v}_after"
        if b not in m.columns:
            continue
        if pd.api.types.is_numeric_dtype(m[b]) or pd.api.types.is_numeric_dtype(m[a]):
            bb = pd.to_numeric(m[b], errors="coerce")
            aa = pd.to_numeric(m[a], errors="coerce")
            both = bb.notna() & aa.notna()
            changed = (both & ~((bb - aa).abs() <= 1e-9 + 1e-6 * bb.abs())) | (
                bb.notna() ^ aa.notna()
            )
            rel = pd.Series(float("nan"), index=m.index)
            nz = both & (bb.abs() > 1e-12)
            rel[nz] = ((aa - bb).abs() / bb.abs())[nz]
        else:
            changed = (m[b].astype(str) != m[a].astype(str)) & ~(m[b].isna() & m[a].isna())
            rel = pd.Series(float("nan"), index=m.index)
        sub = m.loc[changed, keys + [b, a]].copy()
        sub["metric"] = v
        sub["rel_change"] = rel[changed].values
        sub["presence"] = m.loc[changed, "_merge"].astype(str).values
        sub = sub.rename(columns={b: "before", a: "after"})
        out_rows.append(sub)
    if not out_rows:
        return pd.DataFrame(columns=keys + ["metric", "before", "after", "rel_change", "presence"])
    res = pd.concat(out_rows, ignore_index=True)
    res.insert(0, "table", name)
    return res


def compare(args: argparse.Namespace) -> None:
    before_dir, after_dir = Path(args.before), Path(args.after)
    controls = set(str(c) for c in (args.controls or []))
    summary_rows = []
    moved_controls = set()
    for name, (_sql, keys, values) in TABLES.items():
        diff = _compare_table(
            name,
            _load(before_dir, name),
            _load(after_dir, name),
            keys,
            values,
            args.closed_years_only,
        )
        diff.to_csv(after_dir / f"compare_{name}.csv", index=False)
        if diff.empty:
            summary_rows.append(
                {"table": name, "windfarm_id": "-", "changed_values": 0, "max_rel_change": None}
            )
            continue
        for wf, g in diff.groupby("windfarm_id"):
            summary_rows.append(
                {
                    "table": name,
                    "windfarm_id": wf,
                    "changed_values": int(len(g)),
                    "max_rel_change": round(float(g["rel_change"].max()), 4)
                    if g["rel_change"].notna().any()
                    else None,
                }
            )
            if wf in controls:
                moved_controls.add(wf)
    fleet_b, fleet_a = _load(before_dir, "fleet_yearly"), _load(after_dir, "fleet_yearly")
    if not fleet_b.empty and not fleet_a.empty:
        fd = _compare_table(
            "fleet_yearly",
            fleet_b,
            fleet_a,
            ["windfarm_id", "year"],
            ["norm_index_p50", "lost_mwh", "expected_mwh", "constraint_proxy_mwh"],
            args.closed_years_only,
        )
        fd.to_csv(after_dir / "compare_fleet_yearly.csv", index=False)
        moved = sorted(fd["windfarm_id"].unique(), key=int) if not fd.empty else []
        print(f"fleet yearly rows: {len(moved)} windfarms changed: {moved}")
    s = pd.DataFrame(summary_rows)
    print(s.to_string(index=False))
    if controls:
        print(
            "\nCONTROLS MOVED:" if moved_controls else "\ncontrols unchanged:",
            sorted(moved_controls) or sorted(controls),
        )
    if moved_controls:
        sys.exit(3)


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot")
    s.add_argument("--windfarm-ids", nargs="+", type=int, required=True)
    s.add_argument("--out", required=True)
    s.add_argument(
        "--fleet", action="store_true", help="also snapshot yearly rows for every windfarm"
    )
    c = sub.add_parser("compare")
    c.add_argument("--before", required=True)
    c.add_argument("--after", required=True)
    c.add_argument("--closed-years-only", action="store_true")
    c.add_argument("--controls", nargs="*", type=int, default=[])
    args = ap.parse_args()
    if args.cmd == "snapshot":
        asyncio.run(snapshot(args))
    else:
        compare(args)


if __name__ == "__main__":
    main()
