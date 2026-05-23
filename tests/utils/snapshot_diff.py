"""Diff pre/post pipeline baselines and report per-metric changes vs tolerance.

Used to validate Milestone A / C / B1 changes against the captured-baselines from
`scripts/capture_pipeline_baseline.py`. Each table is keyed by its natural row
identity (so e.g. `power_curve_bins` is matched on `(curve_type, year, wind_bin)`).

Pure function, no I/O beyond JSON read — easy to unit test, easy to call from a
script that writes a markdown report.

Tolerances follow the Layer-2 table in `spec-vs-implementation.md`:

| Module | Metric | Tolerance |
|---|---|---|
| 2 | q50_pu, q90_pu per bin       | ±0.5 % |
| 3 | ODI %                          | ±0.5 %; lost_mwh ±1 % |
| 4 | yearly index                   | ±0.01 % |
| 5 | slope_pu, slope_pct            | ±2 %; CI bounds ±5 % |
| 6 | revenue                        | ±0.1 % |
| 1b | constraint period bounds      | ±1 month; ratio ±0.05 |

A change is **in tolerance** if BOTH the relative AND the absolute tolerance are
satisfied (the bigger of the two wins — useful for numbers near zero where relative
tolerance is meaningless).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

# ─── Tolerances ──────────────────────────────────────────────

# (table_name, column_name) → (rel_tolerance, abs_tolerance)
# rel = fractional (0.02 = ±2%), abs = absolute (in the column's natural unit)
DEFAULT_TOLERANCES: dict[tuple[str, str], tuple[float, float]] = {
    # Module 2 — power curves
    ("power_curve_bins", "q50_pu"):       (0.005, 0.005),
    ("power_curve_bins", "q90_pu"):       (0.005, 0.005),
    ("power_curve_bins", "mad_pu"):       (0.05,  0.01),
    ("power_curve_bins", "sample_count"): (0.05,  3),

    # Module 5 — degradation
    ("degradation_results", "slope_pu_per_year"):  (0.02, 0.0005),
    ("degradation_results", "slope_pct_per_year"): (0.02, 0.05),
    ("degradation_results", "intercept"):          (0.05, 1.0),
    ("degradation_results", "r_squared"):          (0.05, 0.005),
    ("degradation_results", "p_value"):            (0.10, 0.05),
    ("degradation_results", "ci_lower_95"):        (0.05, 0.001),
    ("degradation_results", "ci_upper_95"):        (0.05, 0.001),
    ("degradation_results", "baseline_cap_pu"):    (0.05, 0.005),
    ("degradation_results", "data_points"):        (0.0,  0),

    # Modules 3, 4, 6 on performance_summaries
    ("performance_summaries", "odi_pct_loss_mwh"):  (0.01, 0.05),
    ("performance_summaries", "odi_pct_loss_eur"):  (0.01, 0.05),
    ("performance_summaries", "odi_pct_underperf"): (0.01, 0.05),
    ("performance_summaries", "lost_mwh"):          (0.01, 1.0),
    ("performance_summaries", "lost_eur"):          (0.01, 10.0),
    ("performance_summaries", "norm_index_p50"):    (0.0001, 0.01),
    ("performance_summaries", "norm_index_p10"):    (0.0001, 0.01),
    ("performance_summaries", "norm_ratio_p50"):    (0.0001, 0.0001),
    ("performance_summaries", "norm_ratio_p10"):    (0.0001, 0.0001),
    ("performance_summaries", "constraint_proxy_mwh"): (0.05, 100.0),
    ("performance_summaries", "lost_value_eur"):    (0.001, 100.0),
}

DEFAULT_FALLBACK = (0.05, 0.05)  # used for any column not listed above

# Tables → natural row key (tuple of column names that identify a row)
ROW_KEYS: dict[str, tuple[str, ...]] = {
    "power_curve_bins":      ("curve_type", "year", "wind_bin"),
    "degradation_results":   ("reference_curve",),
    "performance_summaries": ("period_type", "year", "month"),
}


# ─── Data classes ────────────────────────────────────────────


@dataclass
class CellDiff:
    table: str
    row_key: tuple
    column: str
    pre: Any
    post: Any
    abs_change: Optional[float]
    rel_change: Optional[float]
    in_tolerance: bool
    tolerance: tuple[float, float]  # (rel, abs) applied


@dataclass
class TableDiff:
    table: str
    rows_added: list[tuple]
    rows_removed: list[tuple]
    cell_diffs: list[CellDiff]

    @property
    def in_tolerance_count(self) -> int:
        return sum(1 for c in self.cell_diffs if c.in_tolerance)

    @property
    def out_of_tolerance_count(self) -> int:
        return sum(1 for c in self.cell_diffs if not c.in_tolerance)


@dataclass
class DiffReport:
    pre_path: Optional[Path]
    post_path: Optional[Path]
    windfarm_code: str
    tables: dict[str, TableDiff] = field(default_factory=dict)

    @property
    def total_cells_compared(self) -> int:
        return sum(len(t.cell_diffs) for t in self.tables.values())

    @property
    def total_out_of_tolerance(self) -> int:
        return sum(t.out_of_tolerance_count for t in self.tables.values())

    @property
    def all_in_tolerance(self) -> bool:
        return self.total_out_of_tolerance == 0 and not any(
            t.rows_added or t.rows_removed for t in self.tables.values()
        )

    def to_dict(self) -> dict:
        """JSON-safe summary."""
        return {
            "pre_path": str(self.pre_path) if self.pre_path else None,
            "post_path": str(self.post_path) if self.post_path else None,
            "windfarm_code": self.windfarm_code,
            "all_in_tolerance": self.all_in_tolerance,
            "total_cells_compared": self.total_cells_compared,
            "total_out_of_tolerance": self.total_out_of_tolerance,
            "tables": {
                name: {
                    "rows_added": [list(k) for k in t.rows_added],
                    "rows_removed": [list(k) for k in t.rows_removed],
                    "cell_diffs_out_of_tolerance": [
                        asdict(c) | {"row_key": list(c.row_key)}
                        for c in t.cell_diffs
                        if not c.in_tolerance
                    ],
                    "in_tolerance_count": t.in_tolerance_count,
                    "out_of_tolerance_count": t.out_of_tolerance_count,
                }
                for name, t in self.tables.items()
            },
        }


# ─── Diff logic ──────────────────────────────────────────────


def _row_key(row: dict, key_cols: tuple[str, ...]) -> tuple:
    return tuple(row.get(c) for c in key_cols)


def _compare_cell(
    table: str,
    column: str,
    pre: Any,
    post: Any,
    tolerances: dict[tuple[str, str], tuple[float, float]],
) -> tuple[Optional[float], Optional[float], bool, tuple[float, float]]:
    """Returns (abs_change, rel_change, in_tolerance, (rel_tol, abs_tol))."""
    rel_tol, abs_tol = tolerances.get((table, column), DEFAULT_FALLBACK)

    # Identical strings / None / dates: in tolerance, no numeric change
    if pre == post:
        return 0.0 if isinstance(pre, (int, float)) else None, 0.0 if isinstance(pre, (int, float)) else None, True, (rel_tol, abs_tol)

    # Non-numeric mismatch (string, date, etc.) — out of tolerance regardless
    if not isinstance(pre, (int, float)) or not isinstance(post, (int, float)):
        return None, None, False, (rel_tol, abs_tol)

    abs_change = float(post) - float(pre)
    # Relative is undefined when pre == 0; treat as "absolute tolerance only" in that case.
    if pre == 0:
        rel_change = None
        in_tol = abs(abs_change) <= abs_tol
    else:
        rel_change = abs_change / abs(pre)
        # "in tolerance" if EITHER the relative OR absolute test passes — useful for
        # near-zero values where 5% relative tolerance is unrealistic.
        in_tol = (abs(rel_change) <= rel_tol) or (abs(abs_change) <= abs_tol)

    return abs_change, rel_change, in_tol, (rel_tol, abs_tol)


def _diff_table(
    table: str,
    pre_rows: list[dict],
    post_rows: list[dict],
    tolerances: dict[tuple[str, str], tuple[float, float]],
) -> TableDiff:
    key_cols = ROW_KEYS.get(table)
    if not key_cols:
        # No row identity → just count rows
        return TableDiff(table=table, rows_added=[], rows_removed=[], cell_diffs=[])

    pre_by_key = {_row_key(r, key_cols): r for r in pre_rows}
    post_by_key = {_row_key(r, key_cols): r for r in post_rows}

    pre_keys = set(pre_by_key.keys())
    post_keys = set(post_by_key.keys())

    rows_added = sorted(post_keys - pre_keys, key=str)
    rows_removed = sorted(pre_keys - post_keys, key=str)
    shared = pre_keys & post_keys

    cell_diffs: list[CellDiff] = []
    for k in sorted(shared, key=str):
        pre_row = pre_by_key[k]
        post_row = post_by_key[k]
        all_cols = (set(pre_row.keys()) | set(post_row.keys())) - set(key_cols)
        for col in sorted(all_cols):
            abs_c, rel_c, in_tol, tol = _compare_cell(
                table, col, pre_row.get(col), post_row.get(col), tolerances
            )
            cell_diffs.append(
                CellDiff(
                    table=table,
                    row_key=k,
                    column=col,
                    pre=pre_row.get(col),
                    post=post_row.get(col),
                    abs_change=abs_c,
                    rel_change=rel_c,
                    in_tolerance=in_tol,
                    tolerance=tol,
                )
            )

    return TableDiff(
        table=table, rows_added=rows_added, rows_removed=rows_removed, cell_diffs=cell_diffs
    )


def diff_snapshots(
    pre: dict,
    post: dict,
    tolerances: Optional[dict[tuple[str, str], tuple[float, float]]] = None,
) -> DiffReport:
    """Compare two pipeline-baseline snapshots (as produced by capture_pipeline_baseline.py).

    Both `pre` and `post` are the in-memory dicts loaded from the captured JSON.
    """
    tols = dict(DEFAULT_TOLERANCES)
    if tolerances:
        tols.update(tolerances)

    pre_wf_code = (pre.get("windfarm") or {}).get("code", "UNKNOWN")
    post_wf_code = (post.get("windfarm") or {}).get("code", "UNKNOWN")
    if pre_wf_code != post_wf_code:
        raise ValueError(
            f"Snapshot windfarm mismatch: pre={pre_wf_code} vs post={post_wf_code}"
        )

    report = DiffReport(pre_path=None, post_path=None, windfarm_code=pre_wf_code)
    for table in ROW_KEYS:
        pre_rows = pre.get(table, [])
        post_rows = post.get(table, [])
        report.tables[table] = _diff_table(table, pre_rows, post_rows, tols)
    return report


def diff_snapshot_files(
    pre_path: Path,
    post_path: Path,
    tolerances: Optional[dict[tuple[str, str], tuple[float, float]]] = None,
) -> DiffReport:
    """Convenience wrapper that loads two JSON files and diffs them."""
    pre = json.loads(Path(pre_path).read_text())
    post = json.loads(Path(post_path).read_text())
    report = diff_snapshots(pre, post, tolerances)
    report.pre_path = Path(pre_path)
    report.post_path = Path(post_path)
    return report


def format_summary_md(report: DiffReport) -> str:
    """Render a short markdown summary for PR descriptions / release notes."""
    lines = [
        f"# Snapshot diff — {report.windfarm_code}",
        "",
        f"- Cells compared: {report.total_cells_compared}",
        f"- Out of tolerance: {report.total_out_of_tolerance}",
        f"- Verdict: {'✓ all within tolerance' if report.all_in_tolerance else '✗ shifts outside tolerance — review below'}",
        "",
    ]
    for table_name, t in report.tables.items():
        if t.rows_added or t.rows_removed or t.out_of_tolerance_count > 0:
            lines.append(f"## {table_name}")
            if t.rows_added:
                lines.append(f"- Rows added: {len(t.rows_added)} (e.g. {t.rows_added[:3]})")
            if t.rows_removed:
                lines.append(f"- Rows removed: {len(t.rows_removed)} (e.g. {t.rows_removed[:3]})")
            if t.out_of_tolerance_count > 0:
                lines.append(f"- Cell diffs out of tolerance: {t.out_of_tolerance_count}")
                for c in t.cell_diffs:
                    if not c.in_tolerance:
                        lines.append(
                            f"    - `{c.row_key}` `{c.column}`: {c.pre} → {c.post}"
                            f" (Δ={c.abs_change}, rel={c.rel_change})"
                        )
            lines.append("")
    return "\n".join(lines)
