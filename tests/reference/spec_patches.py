"""Pandas 2.3 compatibility patches + config overrides for the vendored reference pipeline.

The vendored `energyexe_pipeline_full.py` is a Jupyter-style script with module-level
execution at the bottom (it runs `load_and_clean(CFG)`, `run_constraint_detection`, etc.
on import). Auto-execution makes Python-level monkey-patching painful — the spec module
fails to import unless `cfg.csv_path` points at a real file. The cleanest workaround is
text-substitution: read the vendored source, apply the pandas 2.3 patches plus the
caller's `csv_path`/`rated_mw` overrides, write the result to a temp path, then run that
file as a normal subprocess.

This module preserves the **vendored source byte-identical** (provenance — see VERSION.md).
All patches happen on a copy.

Patches applied:

  1. **Module 1b — categorical-vs-float comparison.** The spec computes
     `obs_q90["v_center"]` via `.apply(...)` on a Categorical `wind_bin` column,
     which preserves the Categorical dtype. Pandas 2.x raises
     `TypeError: Invalid comparison between dtype=category and float` on the
     subsequent `.between(...)`. Fix: cast to object before apply, float after.
     Mirrors the spec's own pattern at line 994.

  2. **Module 2 — merge collision when Module 1b detects zero constraints.**
     `run_constraint_detection` sets `df_curve_clean["flag_structural_constraint"]
     = False` when zero runs detected. This propagates into `df_no_over` via
     `df = df_curve_clean.copy()`. The subsequent merge then produces
     `_x/_y` suffixed columns instead of the bare name → KeyError. Fix: skip
     the merge when `df_no_over` already has the column; fillna in place instead.

  3. **Module 1 — `dayfirst=True` silently corrupts ISO timestamps.** The spec's
     `load_and_clean` parses dates with `pd.to_datetime(..., dayfirst=True)`.
     On ISO-format input (`YYYY-MM-DD HH:MM:SS`, what `tests/utils/spec_csv_exporter.py`
     emits), pandas tries DD/MM/YYYY first, which (a) silently swaps month/day on
     rows where day ≤ 12 (e.g. `2024-11-01` becomes `2024-01-11`) and (b) drops
     rows where day > 12 as NaT (parsing `01` as month → invalid month 11+).
     Net effect: spec drops ~60 % of real-data CSVs and mis-labels ~half of what
     remains — invalidating every downstream metric. **This bug was the source of
     the spurious "Hornsea 1 -0.605 %/yr degrading" reference number in
     `p-1-validation-notes.md` (P-1.3).** With the patch the spec gives the same
     Hornsea slope (+0.77 %/yr) we compute. Fix: drop the `dayfirst=True` kwarg.

  4. **Module 4 — categorical-vs-int comparison in `compute_hourly_norm_ratios`.**
     `out["expected_mw"] = out["wind_bin"].map(reference_lookup)` returns a
     Categorical Series in pandas 2.x because `wind_bin` is Categorical (from
     `pd.cut`). The subsequent `.gt(0)` then raises `TypeError: Invalid
     comparison between dtype=category and int`. Fix: cast to object before map,
     coerce to numeric after.

All four bugs surface only on real data (the spec's example Lutelandet dataset 2022-2025
happens to dodge most of them). Reported / TBD to spec author (Aje).
"""

from __future__ import annotations

import re
from pathlib import Path

VENDORED_SPEC = Path(__file__).parent / "energyexe_pipeline_full.py"


def write_patched_spec(
    out_path: Path,
    *,
    csv_path: str,
    rated_mw: float,
) -> Path:
    """Read the vendored reference, apply pandas-compat patches + config overrides,
    write to `out_path`. Returns `out_path`.

    The result is a standalone Python file. Run it via subprocess; its outputs land
    in `os.path.dirname(csv_path)` (matching spec behaviour at line 369).

    Args:
        out_path: where to write the patched copy.
        csv_path: replaces the spec's hard-coded `Config.csv_path`. Must be an
            absolute path that the script can `pd.read_csv`. The script writes ~25
            output CSVs alongside this file.
        rated_mw: replaces the spec's hard-coded `Config.rated_mw` (51.3 default).
            Must match the windfarm whose data is in `csv_path` — used to compute
            p_pu = power_mw / rated_mw.
    """
    src = VENDORED_SPEC.read_text()

    src = _apply_v_center_patch(src)
    src = _apply_merge_guard_patch(src)
    src = _apply_dayfirst_patch(src)
    src = _apply_module4_cat_patch(src)
    src = _apply_config_overrides(src, csv_path=csv_path, rated_mw=rated_mw)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(src)
    return out_path


def _apply_v_center_patch(src: str) -> str:
    """Patch 1: categorical-vs-float in _detect_constraint_periods (Module 1b)."""
    before = (
        '    obs_q90["v_center"] = obs_q90["wind_bin"].apply(\n'
        '        lambda b: (b.left + b.right) / 2 if hasattr(b, "left") else np.nan\n'
        '    )'
    )
    after = (
        '    obs_q90["v_center"] = (\n'
        '        obs_q90["wind_bin"].astype(object).apply(\n'
        '            lambda b: (b.left + b.right) / 2 if hasattr(b, "left") else np.nan\n'
        '        ).astype(float)\n'
        '    )'
    )
    if before not in src:
        raise RuntimeError(
            "spec_patches: v_center patch target not found — vendored source may have "
            "been revised. Check tests/reference/VERSION.md and the SHA-256."
        )
    return src.replace(before, after, 1)


def _apply_merge_guard_patch(src: str) -> str:
    """Patch 2: merge-collision guard in run_power_curves (Module 2)."""
    before = (
        '    # Propagate constraint flag into df_no_over if present\n'
        '    if "flag_structural_constraint" in df_curve_clean.columns:\n'
        '        df_no_over = df_no_over.merge(\n'
        '            df_curve_clean[[cfg.time_col, "flag_structural_constraint"]].drop_duplicates(cfg.time_col),\n'
        '            on=cfg.time_col, how="left",\n'
        '        )\n'
        '        df_no_over["flag_structural_constraint"] = df_no_over["flag_structural_constraint"].fillna(False)'
    )
    after = (
        '    # Propagate constraint flag into df_no_over if present\n'
        '    # spec_patches: original merge produces _x/_y suffix collision when the\n'
        '    # column is already on df_no_over (which happens whenever Module 1b\n'
        '    # detected zero constraints — df_curve_clean carries the False column\n'
        '    # through .copy()).\n'
        '    if (\n'
        '        "flag_structural_constraint" in df_curve_clean.columns\n'
        '        and "flag_structural_constraint" not in df_no_over.columns\n'
        '    ):\n'
        '        df_no_over = df_no_over.merge(\n'
        '            df_curve_clean[[cfg.time_col, "flag_structural_constraint"]].drop_duplicates(cfg.time_col),\n'
        '            on=cfg.time_col, how="left",\n'
        '        )\n'
        '        df_no_over["flag_structural_constraint"] = df_no_over["flag_structural_constraint"].fillna(False)\n'
        '    elif "flag_structural_constraint" in df_no_over.columns:\n'
        '        df_no_over["flag_structural_constraint"] = df_no_over["flag_structural_constraint"].fillna(False)'
    )
    if before not in src:
        raise RuntimeError(
            "spec_patches: merge-guard patch target not found — vendored source may "
            "have been revised. Check tests/reference/VERSION.md and the SHA-256."
        )
    return src.replace(before, after, 1)


def _apply_dayfirst_patch(src: str) -> str:
    """Patch 3: `dayfirst=True` silently corrupts ISO timestamps.

    On `YYYY-MM-DD HH:MM:SS` input, pandas with dayfirst=True:
      - rows with day ≤ 12: month/day swapped (e.g. 2024-11-01 → 2024-01-11)
      - rows with day > 12: dropped as NaT (parser tries day=YYYY etc, fails)

    The exporter at tests/utils/spec_csv_exporter.py emits ISO timestamps.
    Without this patch the spec drops ~60 % of rows and mis-labels another
    ~half of what remains. See module docstring + p-1-validation-notes.md
    P-1.4 for the discovery.
    """
    before = (
        'df0[cfg.time_col]  = pd.to_datetime(df0[cfg.time_col], dayfirst=True, errors="coerce")'
    )
    after = (
        'df0[cfg.time_col]  = pd.to_datetime(df0[cfg.time_col], errors="coerce")  '
        '# spec_patches: dropped dayfirst=True (corrupts ISO timestamps)'
    )
    if before not in src:
        raise RuntimeError(
            "spec_patches: dayfirst patch target not found — vendored source may "
            "have been revised. Check tests/reference/VERSION.md and the SHA-256."
        )
    return src.replace(before, after, 1)


def _apply_module4_cat_patch(src: str) -> str:
    """Patch 4: categorical-vs-int comparison in `compute_hourly_norm_ratios`.

    `out["wind_bin"].map(reference_lookup)` returns a Categorical Series in
    pandas 2.x (because `wind_bin` came from `pd.cut`). The downstream
    `.gt(0)` then raises `TypeError: Invalid comparison between
    dtype=category and int`. Cast to object before map, coerce to numeric
    after.
    """
    before = 'out["expected_mw"] = out["wind_bin"].map(reference_lookup)'
    after = (
        'out["expected_mw"] = pd.to_numeric(\n'
        '        out["wind_bin"].astype(object).map(reference_lookup), errors="coerce"\n'
        '    )  # spec_patches: cat-to-float for downstream .gt(0)'
    )
    if before not in src:
        raise RuntimeError(
            "spec_patches: module4 cat patch target not found — vendored source may "
            "have been revised. Check tests/reference/VERSION.md and the SHA-256."
        )
    return src.replace(before, after, 1)


def _apply_config_overrides(src: str, *, csv_path: str, rated_mw: float) -> str:
    """Replace the hard-coded `csv_path` and `rated_mw` in Config dataclass."""
    csv_pattern = re.compile(r'(csv_path:\s*str\s*=\s*)r?"[^"]+"')
    rated_pattern = re.compile(r'(rated_mw:\s*float\s*=\s*)[\d.]+')

    new_src, csv_n = csv_pattern.subn(rf'\1"{csv_path}"', src, count=1)
    if csv_n != 1:
        raise RuntimeError("spec_patches: csv_path override target not found.")
    new_src, rated_n = rated_pattern.subn(rf'\g<1>{rated_mw}', new_src, count=1)
    if rated_n != 1:
        raise RuntimeError("spec_patches: rated_mw override target not found.")
    return new_src
