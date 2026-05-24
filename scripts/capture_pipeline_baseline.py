#!/usr/bin/env python
"""Capture pre-change baselines for the pipeline correctness work.

Reads `tests/fixtures/reference_windfarms.yaml`, queries the current production
state for each listed windfarm, and writes one JSON file per windfarm to
`tests/fixtures/baselines/{wf_code}_pre.json`.

Each baseline contains the current values of:
  - power_curve_bins (all rows: raw / capability / overall_clean)
  - degradation_results (q50 + q90 rows)
  - performance_summaries (yearly + monthly rows)

The output is intentionally verbose JSON (no minification) so post-change
snapshots diff cleanly with `git diff` or `tests/utils/snapshot_diff.py`.

Read-only. Safe to run against staging or prod.

Usage:
    poetry run python scripts/capture_pipeline_baseline.py \\
        [--out tests/fixtures/baselines] \\
        [--config tests/fixtures/reference_windfarms.yaml] \\
        [--suffix _pre]    # writes wf_LUTELANDET_pre.json etc.

Re-run with --suffix _post after Milestone A merges to capture the new state.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import structlog
import yaml
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Make the project package importable when running as a script
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.database import get_session_factory  # noqa: E402

logger = structlog.get_logger(__name__)


def _json_default(value: Any) -> Any:
    """JSON-encode types that aren't natively supported (Decimal, date, datetime)."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


async def _query_dicts(session: AsyncSession, sql: str, params: dict) -> list[dict]:
    """Run a parameterised SELECT and return [{col: val, ...}, ...]."""
    rows = (await session.execute(text(sql), params)).mappings().all()
    return [dict(r) for r in rows]


async def capture_one_windfarm(
    session: AsyncSession, wf_id: int, wf_code: str
) -> dict[str, Any]:
    """Capture all baseline-relevant rows for a single windfarm."""
    meta = (await _query_dicts(
        session,
        """
        SELECT id, code, name, nameplate_capacity_mw,
               (SELECT MIN(hour)::date FROM generation_data gd
                  JOIN generation_units gu ON gd.generation_unit_id = gu.id
                  WHERE gu.windfarm_id = w.id) AS gen_start,
               (SELECT MAX(hour)::date FROM generation_data gd
                  JOIN generation_units gu ON gd.generation_unit_id = gu.id
                  WHERE gu.windfarm_id = w.id) AS gen_end
        FROM windfarms w WHERE w.id = :wf_id
        """,
        {"wf_id": wf_id},
    ))
    if not meta:
        raise RuntimeError(f"Windfarm id={wf_id} ({wf_code}) not found in DB")

    power_curves = await _query_dicts(
        session,
        """
        SELECT curve_type, year, wind_bin, q50_pu, q90_pu, mean_pu, mad_pu, sample_count
        FROM   power_curve_bins
        WHERE  windfarm_id = :wf_id
        ORDER BY curve_type, year NULLS FIRST, wind_bin
        """,
        {"wf_id": wf_id},
    )

    degradation = await _query_dicts(
        session,
        """
        SELECT reference_curve, analysis_start, analysis_end, data_points,
               slope_pu_per_year, slope_pct_per_year, intercept,
               r_squared, p_value, ci_lower_95, ci_upper_95,
               baseline_cap_pu, pipeline_run_id
        FROM   degradation_results
        WHERE  windfarm_id = :wf_id
        ORDER BY reference_curve
        """,
        {"wf_id": wf_id},
    )

    perf_summaries = await _query_dicts(
        session,
        """
        SELECT period_type, year, month, total_hours,
               underperf_hours, overperf_hours,
               odi_pct_underperf, lost_mwh, expected_mwh,
               odi_pct_loss_mwh, lost_eur, expected_revenue_eur, odi_pct_loss_eur,
               long_run_count, max_run_hours,
               norm_ratio_p50, norm_index_p50,
               norm_ratio_p10, norm_index_p10,
               constraint_proxy_mwh, lost_value_eur
        FROM   performance_summaries
        WHERE  windfarm_id = :wf_id
        ORDER BY period_type, year, month NULLS FIRST
        """,
        {"wf_id": wf_id},
    )

    return {
        "windfarm": meta[0],
        "captured_at": datetime.utcnow().isoformat() + "Z",
        "power_curve_bins": power_curves,
        "degradation_results": degradation,
        "performance_summaries": perf_summaries,
        "row_counts": {
            "power_curve_bins": len(power_curves),
            "degradation_results": len(degradation),
            "performance_summaries": len(perf_summaries),
        },
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent.parent
        / "tests"
        / "fixtures"
        / "reference_windfarms.yaml",
        help="YAML file listing reference windfarms.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).parent.parent / "tests" / "fixtures" / "baselines",
        help="Output directory for per-windfarm JSON files.",
    )
    parser.add_argument(
        "--suffix",
        default="_pre",
        help="Filename suffix (default: '_pre'). Use '_post' after the fix lands.",
    )
    parser.add_argument(
        "--wf-id",
        type=int,
        default=None,
        help="Capture only this windfarm (must be present in the YAML).",
    )
    args = parser.parse_args()

    cfg = yaml.safe_load(args.config.read_text())
    farms = cfg.get("reference_windfarms", [])
    if args.wf_id is not None:
        farms = [f for f in farms if f["id"] == args.wf_id]
        if not farms:
            print(f"[ERROR] wf_id={args.wf_id} not in {args.config}", file=sys.stderr)
            return 2

    args.out.mkdir(parents=True, exist_ok=True)

    factory = get_session_factory()
    for entry in farms:
        wf_id = entry["id"]
        wf_code = entry["code"]
        try:
            async with factory() as session:
                snapshot = await capture_one_windfarm(session, wf_id, wf_code)
        except Exception as exc:
            logger.error(
                "baseline_capture_failed", wf_id=wf_id, code=wf_code, error=str(exc)
            )
            continue

        out_file = args.out / f"{wf_code}{args.suffix}.json"
        out_file.write_text(
            json.dumps(snapshot, indent=2, default=_json_default, sort_keys=False)
        )
        logger.info(
            "baseline_captured",
            wf_id=wf_id,
            code=wf_code,
            path=str(out_file),
            **snapshot["row_counts"],
        )

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
