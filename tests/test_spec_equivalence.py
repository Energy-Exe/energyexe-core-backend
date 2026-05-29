"""Layer-3 spec-equivalence test (the one referenced in reference_windfarms.yaml).

Proves the backend's Module 2 + Module 5 math reproduces the vendored
reference pipeline (``tests/reference/energyexe_pipeline_full.py``) when the
backend's one enhancement beyond the spec — structural-constraint masking — is
removed. Runs hermetically on synthetic data: no DB, no prod, no network.

Flow:
  1. Generate a deterministic multi-year hourly CSV in the spec's input format.
  2. Run the (patched) vendored spec as a subprocess; read its Module 5
     degradation estimate CSV.
  3. Run the backend's own static Module 2 + Module 5 code on the same CSV,
     unmasked.
  4. Assert the q50 (P50) degradation slope + baseline agree within tolerance.

Marked ``integration``: it shells out to the spec (needs matplotlib /
statsmodels / scikit-learn) and is slower than the pure-unit tests, so it is
skipped by default (``-m 'not integration'``). Run with ``-m integration``.

Known, intentional divergences this test documents rather than fails on:
  * The q90 (P10) reference is NOT asserted equivalent. The spec applies the
    ``min_median_pu_for_operational`` floor against ``q50_bin`` for BOTH
    references (spec line 998), whereas the backend applies it against the
    active reference (``q90_pu``) — so the P10 fit admits extra low-wind hours
    and diverges. See test_q90_operational_floor_divergence_is_documented.
  * n can differ by a handful of rows: the backend keeps ``wind == 14.0`` via
    ``wind_speed.between(4, 14)`` + ``np.floor``; the spec drops it via
    ``v_center.between(4, 14)`` on a ``[14, 15)`` bin (centre 14.5).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from app.services.degradation_service import DegradationService
from app.services.power_curve_service import PowerCurveService

pytestmark = pytest.mark.integration

RATED_MW = 100.0
SLOPE_PU_PER_YEAR = -0.01  # mild, known degradation injected into the synthetic data


def _make_spec_csv(
    path: Path,
    *,
    start="2021-01-01",
    end="2024-01-01",
    seed=7,
    noise=0.02,
) -> None:
    """Write a deterministic hourly CSV in the spec's input column format.

    ``noise`` controls per-hour scatter. Low noise keeps every operational bin's
    q50 and q90 on the same side of the 0.10 floor (clean case). Higher noise
    lifts q90 above 0.10 in low-wind bins while q50 stays below — creating the
    bins where the q50-vs-q90 floor source actually matters (issue #80).
    """
    rng = np.random.RandomState(seed)
    hours = pd.date_range(start=start, end=end, freq="h", inclusive="left")
    wind = rng.uniform(4.0, 14.0, size=len(hours))
    base = 1.0 / (1.0 + np.exp(-(wind - 8.0)))
    yf = hours.year + (hours.dayofyear - 1) / 365.25
    trend = SLOPE_PU_PER_YEAR * (yf - yf[0])
    p_pu = np.clip(base + trend + rng.normal(0, noise, len(hours)), 0.0, 1.0)
    pd.DataFrame(
        {
            "time": hours.strftime("%Y-%m-%d %H:%M:%S"),
            "wind_speed_mps": np.round(wind, 4),
            "power_mw": np.round(p_pu * RATED_MW, 4),
            "Price[Currency/MWh]": 30.0,
        }
    ).to_csv(path, index=False)


def _run_spec(csv_path: Path, tmp_path: Path) -> dict:
    """Run the patched vendored spec and return its q50/q90 Module 5 estimates."""
    from tests.reference.spec_patches import write_patched_spec

    spec_py = tmp_path / "spec_run.py"
    write_patched_spec(spec_py, csv_path=str(csv_path), rated_mw=RATED_MW)
    proc = subprocess.run(
        [sys.executable, str(spec_py)],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
    )
    assert proc.returncode == 0, f"spec failed:\n{proc.stderr[-3000:]}"
    out = {}
    for ref in ("q50", "q90"):
        f = tmp_path / f"hourly_operational_degradation_estimate_vs_{ref}.csv"
        assert f.exists(), f"spec did not emit {f.name}"
        row = pd.read_csv(f).iloc[0]
        out[ref] = {
            "slope_pct": float(row["slope_pct_per_year_relative"]),
            "baseline": float(row["baseline_cap_pu_operational_first_year"]),
            "n": int(row["n"]),
        }
    return out


def _run_backend_unmasked(csv_path: Path, ref_col: str) -> dict:
    """Run the backend's static Module 2 + Module 5 on the CSV (no DB, no mask)."""
    raw = pd.read_csv(csv_path)
    raw["hour"] = pd.to_datetime(raw["time"])
    df = pd.DataFrame(
        {
            "hour": raw["hour"],
            "year": raw["hour"].dt.year.astype(int),
            "generation_mwh": pd.to_numeric(raw["power_mw"], errors="coerce"),
            "wind_speed": pd.to_numeric(raw["wind_speed_mps"], errors="coerce"),
            "market_price": pd.to_numeric(raw["Price[Currency/MWh]"], errors="coerce"),
        }
    )
    df["p_pu"] = df["generation_mwh"] / RATED_MW

    _, df_curve = PowerCurveService.apply_hard_filters(df)
    years = sorted(df_curve["year"].unique())

    yearly_raw = pd.concat(
        [
            PowerCurveService.compute_bin_stats(df_curve[df_curve["year"] == y]).assign(year=y)
            for y in years
            if not PowerCurveService.compute_bin_stats(df_curve[df_curve["year"] == y]).empty
        ],
        ignore_index=True,
    )
    ovp = PowerCurveService.flag_overperformance(df_curve, yearly_raw)
    df_no_over = df_curve[~ovp].copy()

    # Build per-year capability curves for both the active reference and q50
    # (the operational floor is always q50 — see issue #80 / spec line 998).
    curves: dict = {}
    q50_curves: dict = {}
    for y in years:
        stats = PowerCurveService.compute_bin_stats(df_no_over[df_no_over["year"] == y])
        for _, r in stats.iterrows():
            left = r.get("wind_bin_left")
            if pd.isna(left):
                continue
            if pd.notna(r.get(ref_col)):
                curves.setdefault(int(y), {})[float(left)] = float(r[ref_col])
            if pd.notna(r.get("q50_pu")):
                q50_curves.setdefault(int(y), {})[float(left)] = float(r["q50_pu"])

    floor = None if ref_col == "q50_pu" else q50_curves
    resid = DegradationService.compute_residuals(df_no_over, curves, floor_curves=floor)
    trend = DegradationService.fit_degradation_trend(resid)
    return {"slope_pct": trend["slope_pct"], "baseline": trend["baseline_cap_pu"], "n": trend["n"]}


@pytest.fixture(scope="module")
def csv_and_spec(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("spec_equiv")
    csv_path = tmp / "synthetic.csv"
    _make_spec_csv(csv_path)
    spec = _run_spec(csv_path, tmp)
    return csv_path, spec


@pytest.fixture(scope="module")
def csv_and_spec_divergent(tmp_path_factory):
    """Higher-noise data so low-wind bins have q50 < 0.10 ≤ q90 — the regime
    where the q50-vs-q90 operational floor source changes the q90 fit."""
    tmp = tmp_path_factory.mktemp("spec_equiv_div")
    csv_path = tmp / "synthetic_div.csv"
    _make_spec_csv(csv_path, noise=0.06)
    spec = _run_spec(csv_path, tmp)
    return csv_path, spec


def test_q50_degradation_matches_spec(csv_and_spec):
    """The backend's P50 degradation reproduces the corrected spec."""
    csv_path, spec = csv_and_spec
    backend = _run_backend_unmasked(csv_path, "q50_pu")

    # Baseline is the hours-weighted median of the first-year reference — must
    # match closely (the Bug-C fix made this per-windfarm, not hardcoded 0.35).
    assert backend["baseline"] == pytest.approx(spec["q50"]["baseline"], abs=0.005)
    # Slope agreement: the headline degradation metric.
    assert backend["slope_pct"] == pytest.approx(spec["q50"]["slope_pct"], abs=0.05)
    # Row counts may differ by a few due to the documented boundary handling.
    assert abs(backend["n"] - spec["q50"]["n"]) <= 5


def test_q90_degradation_matches_spec_on_divergent_data(csv_and_spec_divergent):
    """The backend's P10/q90 fit reproduces the spec (issue #80 fix).

    Uses higher-noise data so some operational bins have q50 < 0.10 ≤ q90.
    Before the fix the backend floored on q90 and admitted those low-wind hours
    the spec excludes (floor on q50), inflating both slope and n. With the fix
    (floor always q50) the two agree.
    """
    csv_path, spec = csv_and_spec_divergent

    # Sanity: the data must actually contain a divergence bin, else this test
    # would pass trivially (as it would on the clean fixture).
    raw = pd.read_csv(csv_path)
    raw["wind_bin"] = np.floor(raw["wind_speed_mps"]).astype(float)
    raw["p_pu"] = raw["power_mw"] / RATED_MW
    op = raw[(raw["wind_speed_mps"] >= 4.0) & (raw["wind_speed_mps"] <= 14.0)]
    by_bin = op.groupby("wind_bin")["p_pu"].agg(q50="median", q90=lambda s: s.quantile(0.90))
    divergent_bins = by_bin[(by_bin["q50"] < 0.10) & (by_bin["q90"] >= 0.10)]
    assert not divergent_bins.empty, "fixture has no q50<0.10≤q90 bin to exercise the fix"

    backend = _run_backend_unmasked(csv_path, "q90_pu")
    assert backend["baseline"] == pytest.approx(spec["q90"]["baseline"], abs=0.01)
    assert backend["slope_pct"] == pytest.approx(spec["q90"]["slope_pct"], abs=0.05)
    assert abs(backend["n"] - spec["q90"]["n"]) <= 5
