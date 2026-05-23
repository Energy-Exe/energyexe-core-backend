# ==============================================================================
# ENERGYEXE — WIND FARM PERFORMANCE PIPELINE
# Full pipeline — all cells in sequential order
# Version: Structural-constraint-aware (post-Niord update)
#
# Cell order:
#   Cell 1  — Imports, config, brand theme
#   Cell 2  — Utility functions (shared across all modules)
#   Cell 3  — Module 1: Data loading & cleaning
#   Cell 3b — Module 1b: Structural constraint detection (NEW)
#   Cell 4  — Module 2: Power curve analysis
#   Cell 4b — Module 2 charts (optional, run separately)
#   Cell 5  — Module 3: Anomaly detection & loss quantification
#   Cell 6  — Module 4: Wind normalisation
#   Cell 6b — Module 4 charts (optional, run separately)
#   Cell 7  — Module 5: Degradation analysis
#   Cell 7b — Module 5 charts (optional, run separately)
#   Cell 8  — Module 6: Commercial reporting
# ==============================================================================


# ==============================================================================
# CELL 1 — IMPORTS, CONFIG, BRAND THEME
# ==============================================================================

from __future__ import annotations
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Brand colours ─────────────────────────────────────────────────────────────
THEME_BG          = "#142840"
THEME_ACCENT      = "#F2AA84"
CURVE_RAW_COLOR   = "#5DADE2"
CURVE_CLEAN_COLOR = "#F5B041"
RAW_POINTS_COLOR  = "#AAAAAA"

def apply_theme(ax):
    ax.set_facecolor(THEME_BG)
    ax.figure.set_facecolor(THEME_BG)
    ax.title.set_color(THEME_ACCENT)
    ax.xaxis.label.set_color(THEME_ACCENT)
    ax.yaxis.label.set_color(THEME_ACCENT)
    ax.tick_params(colors=THEME_ACCENT)
    for spine in ax.spines.values():
        spine.set_color(THEME_ACCENT)
        spine.set_alpha(0.7)
    ax.grid(True, alpha=0.18, color=THEME_ACCENT)

def theme_legend(leg):
    if leg is None:
        return
    frame = leg.get_frame()
    frame.set_facecolor(THEME_BG)
    frame.set_edgecolor(THEME_ACCENT)
    frame.set_alpha(0.35)
    for txt in leg.get_texts():
        txt.set_color(THEME_ACCENT)

@dataclass
class Config:
    # --- Data source ---
    csv_path:  str = r"D:\Power\Power curves\Lutelandet\Wind_power_price_rawdata_Lutelandet_2022-2025.csv"
    time_col:  str = "time"
    wind_col:  str = "wind_speed_mps"
    power_col: str = "power_mw"
    price_col: str = "Price[Currency/MWh]"

    # --- Turbine / site ---
    rated_mw: float = 51.3

    # --- Commercial ---
    base_ppa_price_eur_per_mwh: Optional[float] = None
    p50_target_mwh_per_year:    float           = 150_000
    ppa_price_scenarios: list = field(
        default_factory=lambda: [23.2, 26.0, 30.0, 35.0, 40.0]
    )

    # --- Binning ---
    bin_width:            float = 1.0
    min_samples_per_bin:  int   = 30
    wind_min_for_curve:   float = 2.0
    wind_max_for_curve:   float = 25.0

    # --- Hard plausibility limits ---
    wind_min_allowed: float = 0.0
    wind_max_allowed: float = 40.0
    p_pu_min_allowed: float = -0.05
    p_pu_max_allowed: float =  1.20

    # --- Percentile logic ---
    base_q:       int          = 50
    cap_q:        int          = 90
    plot_main_q:  int          = 50
    plot_ref_q:   Optional[int] = 90

    # --- Anomaly thresholds ---
    underperf_mad_k: float = 2.5
    overperf_mad_k:  float = 1.5
    ceiling_pu:      float = 1.02
    long_run_hours:  int   = 24

    # --- IsolationForest ---
    use_isolation_forest:    bool  = True
    isolation_contamination: float = 0.03

    # --- Operational degradation band ---
    op_wind_min:                   float = 4.0
    op_wind_max:                   float = 14.0
    min_median_pu_for_operational: float = 0.10

    # --- Wind normalisation ---
    norm_wind_min_mps: float = 4.0

    # --- Structural constraint detection (Module 1b) ---
    constraint_detection_bands: list = field(
        default_factory=lambda: [
            {"wind_min": 7.0,  "wind_max": 10.0, "q90_ratio_threshold": 0.70},
            {"wind_min": 10.0, "wind_max": 25.0, "q90_ratio_threshold": 0.80},
        ]
    )
    constraint_min_hours: int = 336  # 2 weeks

CFG = Config()


# ==============================================================================
# CELL 2 — UTILITY FUNCTIONS
# ==============================================================================

import warnings

try:
    from sklearn.ensemble import IsolationForest
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    log.warning("scikit-learn not available — IsolationForest disabled.")

try:
    from statsmodels.tsa.seasonal import seasonal_decompose
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False
    log.warning("statsmodels not available — seasonal decomposition disabled.")

_REF_LABELS = {"q50": "P50", "q90": "P10"}


def percentile_col(q) -> str:
    return f"q{int(round(float(q)))}"


def safe_percentile(x: pd.Series, q: float) -> float:
    if len(x) == 0:
        return np.nan
    return float(np.nanpercentile(x.to_numpy(dtype=float), q))


def make_percentile_agg(q: float):
    def _fn(x):
        return safe_percentile(x, q)
    _fn.__name__ = percentile_col(q)
    return _fn


def make_mad_agg(x: pd.Series) -> float:
    arr = x.to_numpy(dtype=float)
    med = np.nanmedian(arr)
    return float(np.nanmedian(np.abs(arr - med)))
make_mad_agg.__name__ = "mad"


def make_bins(vmin: float, vmax: float, width: float) -> np.ndarray:
    bins = np.arange(vmin, vmax + width, width)
    if bins[-1] < vmax:
        bins = np.append(bins, vmax)
    return bins


def add_interval_centers(agg: pd.DataFrame, bin_col: str = "wind_bin") -> pd.DataFrame:
    out = agg.copy()
    out["v_left"]   = out[bin_col].apply(lambda x: x.left  if hasattr(x, "left")  else np.nan).astype(float)
    out["v_right"]  = out[bin_col].apply(lambda x: x.right if hasattr(x, "right") else np.nan).astype(float)
    out["v_center"] = 0.5 * (out["v_left"] + out["v_right"])
    return out


def required_percentiles(cfg: Config) -> list:
    qs = {50, cfg.cap_q}
    if cfg.plot_ref_q:
        qs.add(cfg.plot_ref_q)
    return sorted(qs)


def _build_bin_agg(df: pd.DataFrame, bins: np.ndarray, qs: list, cfg: Config) -> pd.DataFrame:
    d = df.copy()
    d["wind_bin"] = pd.cut(d["v"], bins=bins, right=False, include_lowest=True)
    agg_dict = {
        "n":   ("p_pu", "count"),
        "q50": ("p_pu", "median"),
        "mad": ("p_pu", make_mad_agg),
    }
    for q in qs:
        if q == 50:
            continue
        col = percentile_col(q)
        agg_dict[col] = ("p_pu", make_percentile_agg(q))
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Mean of empty slice")
        warnings.filterwarnings("ignore", message="Degrees of freedom <= 0")
        agg = d.groupby("wind_bin", observed=False).agg(**agg_dict).reset_index()
    agg = add_interval_centers(agg, "wind_bin")
    agg = agg[agg["n"] >= cfg.min_samples_per_bin].copy()
    return agg


def build_yearly_curves(df: pd.DataFrame, bins: np.ndarray, cfg: Config) -> pd.DataFrame:
    qs    = required_percentiles(cfg)
    parts = []
    for y in sorted(df["year"].unique()):
        agg = _build_bin_agg(df[df["year"] == y], bins, qs, cfg)
        agg["year"] = y
        parts.append(agg)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def build_overall_curve(df: pd.DataFrame, bins: np.ndarray, cfg: Config) -> pd.DataFrame:
    return _build_bin_agg(df, bins, required_percentiles(cfg), cfg)


def map_bin_stats_to_rows(df: pd.DataFrame, curves_stats: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    cap_col = percentile_col(cfg.cap_q)
    out = df.copy()
    for col in ["q50_bin", f"{cap_col}_bin", "mad_bin"]:
        out[col] = np.nan

    for (wb, yr), grp in out.groupby(["wind_bin", "year"], observed=True):
        mask = (
            (curves_stats["wind_bin"] == wb) &
            (curves_stats["year"]     == yr)
        )
        row = curves_stats[mask]
        if row.empty:
            continue
        idx = grp.index
        out.loc[idx, "q50_bin"]         = float(row["q50"].iloc[0])
        out.loc[idx, f"{cap_col}_bin"]  = float(row[cap_col].iloc[0]) if cap_col in row.columns else np.nan
        out.loc[idx, "mad_bin"]         = float(row["mad"].iloc[0])
    return out


def compute_loss_value_eur(lost_mwh, market_price, ppa_price):
    if ppa_price is not None:
        return lost_mwh * float(ppa_price)
    if market_price is None or market_price.isna().all():
        raise ValueError("No market price data and base_ppa_price_eur_per_mwh is None.")
    n_nan = int(market_price.isna().sum())
    if n_nan > 0:
        mean_price = float(market_price.mean())
        log.warning("Market price has %d NaN rows — filling with mean %.2f.", n_nan, mean_price)
        market_price = market_price.fillna(mean_price)
    return lost_mwh * market_price


def save_figure(fig, path: str, bg: str = THEME_BG, dpi: int = 150):
    path = str(path)
    fd, tmp = tempfile.mkstemp(suffix=".png", dir=os.path.dirname(path) or ".")
    os.close(fd)
    try:
        fig.savefig(tmp, dpi=dpi, facecolor=bg, bbox_inches="tight")
        shutil.move(tmp, path)
        log.info("Saved: %s", os.path.basename(path))
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def auto_detect_columns(df: pd.DataFrame, cfg: Config) -> Config:
    import copy
    cfg = copy.copy(cfg)
    cols_lower = {c.lower(): c for c in df.columns}

    def _find(candidates, current):
        if current in df.columns:
            return current
        for cand in candidates:
            if cand in cols_lower:
                found = cols_lower[cand]
                log.warning("Column '%s' not found — using '%s' instead.", current, found)
                return found
        return current

    cfg.time_col  = _find(["time", "datetime", "timestamp", "date"],           cfg.time_col)
    cfg.wind_col  = _find(["wind_speed_mps", "wind_speed", "windspeed", "v"],  cfg.wind_col)
    cfg.power_col = _find(["power_mw", "power", "p_mw", "active_power"],       cfg.power_col)
    if cfg.price_col:
        cfg.price_col = _find(
            ["price[currency/mwh]", "price", "market_price", "spot_price", "da_price"],
            cfg.price_col,
        )
    return cfg


def remove_seasonal_component(series: pd.Series, period: int = 8760) -> pd.Series:
    if not HAS_STATSMODELS or len(series) < 2 * period:
        return series
    try:
        result = seasonal_decompose(
            series.interpolate().ffill().bfill(),
            model="additive", period=period, extrapolate_trend="freq",
        )
        return series - result.seasonal
    except Exception as exc:
        log.warning("Seasonal decomposition failed (%s) — using raw series.", exc)
        return series


# ==============================================================================
# CELL 3 — MODULE 1: DATA LOADING & CLEANING
# ==============================================================================

def data_quality_report(df0, df_clean, cfg):
    rows = []
    for col in [cfg.time_col, cfg.wind_col, cfg.power_col]:
        if col not in df0.columns:
            continue
        n_total = len(df0)
        n_null  = int(df0[col].isna().sum())
        row = {
            "Column": col, "Total_rows": n_total,
            "Null_rows": n_null,
            "Null_%": round(100.0 * n_null / max(n_total, 1), 2),
        }
        if pd.api.types.is_numeric_dtype(df0[col]):
            row.update({
                "Min":  round(float(df0[col].min()),  4),
                "Max":  round(float(df0[col].max()),  4),
                "Mean": round(float(df0[col].mean()), 4),
            })
        rows.append(row)
    rows.append({
        "Column": "TOTAL (all cols)", "Total_rows": len(df0),
        "Null_rows": len(df0) - len(df_clean),
        "Null_%": round(100.0 * (len(df0) - len(df_clean)) / max(len(df0), 1), 2),
    })
    return pd.DataFrame(rows)


def load_and_clean(cfg=CFG):
    output_dir = str(Path(cfg.csv_path).parent)
    log.info("Loading: %s", cfg.csv_path)
    try:
        df0 = pd.read_csv(cfg.csv_path)
    except Exception as exc:
        raise RuntimeError(f"Failed to load CSV: {exc}") from exc
    log.info("Loaded %d rows, %d columns.", len(df0), len(df0.columns))
    cfg = auto_detect_columns(df0, cfg)
    cols = [cfg.time_col, cfg.wind_col, cfg.power_col]
    has_price = bool(cfg.price_col) and cfg.price_col in df0.columns
    if has_price:
        cols.append(cfg.price_col)
    df0 = df0[cols].copy()
    df0[cfg.time_col]  = pd.to_datetime(df0[cfg.time_col], dayfirst=True, errors="coerce")
    df0[cfg.wind_col]  = pd.to_numeric(df0[cfg.wind_col],  errors="coerce")
    df0[cfg.power_col] = pd.to_numeric(df0[cfg.power_col], errors="coerce")
    df0["v"]    = df0[cfg.wind_col]
    df0["p_pu"] = df0[cfg.power_col] / cfg.rated_mw
    df0["market_price"] = (
        pd.to_numeric(df0[cfg.price_col], errors="coerce") if has_price else np.nan
    )
    N0 = len(df0)
    df = df0.dropna(subset=[cfg.time_col, "v", "p_pu"]).copy()
    df = df[df["v"].between(cfg.wind_min_allowed, cfg.wind_max_allowed, inclusive="both")]
    df = df[df["p_pu"].between(cfg.p_pu_min_allowed, cfg.p_pu_max_allowed, inclusive="both")]
    df["year"]         = df[cfg.time_col].dt.year
    df["month_period"] = df[cfg.time_col].dt.to_period("M")
    df["flag_outside_curve_wind"] = ~df["v"].between(
        cfg.wind_min_for_curve, cfg.wind_max_for_curve, inclusive="both"
    )
    bins = make_bins(cfg.wind_min_for_curve, cfg.wind_max_for_curve, cfg.bin_width)
    df["wind_bin"] = pd.cut(df["v"], bins=bins, right=False, include_lowest=True)
    df_curve = df.loc[~df["flag_outside_curve_wind"]].copy()
    dq = data_quality_report(df0, df, cfg)
    dq.to_csv(os.path.join(output_dir, "data_quality_report.csv"), index=False)
    log.info("Saved: data_quality_report.csv")
    cleaning_summary = pd.DataFrame([
        ("Original rows",             N0),
        ("After hard cleaning",       len(df)),
        ("Used for curve wind-range", len(df_curve)),
    ], columns=["Category", "Count"])
    cleaning_summary["% of original"] = 100.0 * cleaning_summary["Count"] / N0
    cleaning_summary.to_csv(os.path.join(output_dir, "cleaning_exclusion_summary.csv"), index=False)
    log.info("Saved: cleaning_exclusion_summary.csv")
    log.info("\n%s", cleaning_summary.to_string(index=False))
    return df, df_curve, cfg, output_dir

df_clean, df_curve, cfg, output_dir = load_and_clean(CFG)


# ==============================================================================
# CELL 3b — MODULE 1b: STRUCTURAL CONSTRAINT DETECTION
# Run after Module 1. Detects sustained export constraint periods that would
# contaminate the capability curve. Outputs df_curve_clean (used by Module 2+).
# ==============================================================================

def _detect_constraint_periods(df_curve: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if not cfg.constraint_detection_bands:
        log.info("Module 1b: constraint_detection_bands is empty — skipping detection.")
        return pd.DataFrame()

    df = df_curve.copy()
    df["_year"]  = df[cfg.time_col].dt.year
    df["_month"] = df[cfg.time_col].dt.to_period("M")

    years = df["_year"].unique()
    ref_rows = []
    for yr in years:
        ref_df  = df[df["_year"] != yr]
        ref_q90 = (
            ref_df.groupby("wind_bin", observed=True)["p_pu"]
            .quantile(0.90).reset_index().rename(columns={"p_pu": "ref_q90"})
        )
        ref_q90["_year"] = yr
        ref_rows.append(ref_q90)

    if not ref_rows:
        log.warning("Module 1b: insufficient years to build leave-one-out reference.")
        return pd.DataFrame()

    ref_table = pd.concat(ref_rows, ignore_index=True)

    obs_q90 = (
        df.groupby(["wind_bin", "_month", "_year"], observed=True)["p_pu"]
        .quantile(0.90).reset_index().rename(columns={"p_pu": "obs_q90"})
    )
    obs_q90 = obs_q90.merge(ref_table, on=["wind_bin", "_year"], how="left")
    obs_q90 = obs_q90.dropna(subset=["ref_q90"])
    obs_q90 = obs_q90[obs_q90["ref_q90"] > 0].copy()
    obs_q90["q90_ratio"] = obs_q90["obs_q90"] / obs_q90["ref_q90"]

    obs_q90["v_center"] = obs_q90["wind_bin"].apply(
        lambda b: (b.left + b.right) / 2 if hasattr(b, "left") else np.nan
    )
    obs_q90["flag_constrained_bin_month"] = False
    for band in cfg.constraint_detection_bands:
        in_band = obs_q90["v_center"].between(band["wind_min"], band["wind_max"], inclusive="left")
        below   = obs_q90["q90_ratio"] < band["q90_ratio_threshold"]
        obs_q90.loc[in_band & below, "flag_constrained_bin_month"] = True

    flagged_bin_months = obs_q90.loc[
        obs_q90["flag_constrained_bin_month"], ["wind_bin", "_month"]
    ].drop_duplicates()

    df = df.merge(flagged_bin_months.assign(_constrained=True),
                  on=["wind_bin", "_month"], how="left")
    df["_constrained"] = df["_constrained"].fillna(False)

    df_sorted = df.sort_values(cfg.time_col).copy()
    df_sorted["_run_id"] = (
        (df_sorted["_constrained"] != df_sorted["_constrained"].shift()).cumsum()
    )

    constrained_runs = df_sorted[df_sorted["_constrained"]].groupby("_run_id").agg(
        period_start=(cfg.time_col, "min"),
        period_end=(cfg.time_col,   "max"),
        duration_hours=(cfg.time_col, "count"),
        wind_bins_affected=("wind_bin", lambda x: len(x.unique())),
        mean_q90_ratio=("p_pu", lambda x: x.quantile(0.90)),
    ).reset_index(drop=True)

    constrained_runs = constrained_runs[
        constrained_runs["duration_hours"] >= cfg.constraint_min_hours
    ].copy()
    constrained_runs["flag_source"] = "auto_constraint_detector"
    return constrained_runs


def run_constraint_detection(df_clean, df_curve, cfg, output_dir):
    log.info("Module 1b: running structural constraint detection...")
    constraint_runs = _detect_constraint_periods(df_curve, cfg)

    if constraint_runs.empty:
        log.info("Module 1b: no structural constraint periods detected.")
        df_clean = df_clean.copy()
        df_clean["flag_structural_constraint"] = False
        df_curve_clean = df_curve.copy()
        df_curve_clean["flag_structural_constraint"] = False
        empty_flags = pd.DataFrame(columns=[
            "period_start", "period_end", "duration_hours",
            "wind_bins_affected", "mean_q90_ratio", "flag_source", "review_status",
        ])
        empty_flags.to_csv(os.path.join(output_dir, "structural_constraint_flags.csv"), index=False)
        log.info("Saved: structural_constraint_flags.csv (empty)")
        return df_clean, df_curve_clean, constraint_runs

    constrained_timestamps = set()
    for _, run in constraint_runs.iterrows():
        mask = (
            (df_curve[cfg.time_col] >= run["period_start"]) &
            (df_curve[cfg.time_col] <= run["period_end"])
        )
        constrained_timestamps.update(df_curve.loc[mask, cfg.time_col].tolist())

    df_clean = df_clean.copy()
    df_clean["flag_structural_constraint"] = df_clean[cfg.time_col].isin(constrained_timestamps)

    df_curve_clean = df_curve.copy()
    df_curve_clean["flag_structural_constraint"] = df_curve_clean[cfg.time_col].isin(constrained_timestamps)
    n_flagged = int(df_curve_clean["flag_structural_constraint"].sum())
    df_curve_clean = df_curve_clean[~df_curve_clean["flag_structural_constraint"]].copy()

    log.info(
        "Module 1b: %d constraint run(s), %d hours excluded from curve (%.1f%%).",
        len(constraint_runs), n_flagged, 100.0 * n_flagged / max(len(df_curve), 1),
    )

    constraint_runs["review_status"] = "pending_review"
    constraint_runs.to_csv(os.path.join(output_dir, "structural_constraint_flags.csv"), index=False)
    log.info("Saved: structural_constraint_flags.csv (%d rows)", len(constraint_runs))

    for _, run in constraint_runs.iterrows():
        log.warning(
            "STRUCTURAL CONSTRAINT CANDIDATE: %s to %s (%d hours, mean Q90 ratio %.2f)",
            run["period_start"].date(), run["period_end"].date(),
            run["duration_hours"], run["mean_q90_ratio"],
        )

    return df_clean, df_curve_clean, constraint_runs


df_clean, df_curve_clean, constraint_runs = run_constraint_detection(
    df_clean, df_curve, cfg, output_dir
)


# ==============================================================================
# CELL 4 — MODULE 2: POWER CURVE ANALYSIS
# ==============================================================================

def _flag_overperf(df_mapped: pd.DataFrame, cfg: Config) -> pd.Series:
    cap_col = percentile_col(cfg.cap_q)
    cap_bin = f"{cap_col}_bin"
    flag = (
        (df_mapped["p_pu"] > df_mapped[cap_bin] + cfg.overperf_mad_k * df_mapped["mad_bin"])
        | (df_mapped["p_pu"] > cfg.ceiling_pu)
    )
    return flag.fillna(False)


def run_power_curves(df_curve_clean, cfg, output_dir):
    cap_col = percentile_col(cfg.cap_q)
    bins    = make_bins(cfg.wind_min_for_curve, cfg.wind_max_for_curve, cfg.bin_width)
    df      = df_curve_clean.copy()
    df["wind_bin"] = pd.cut(df["v"], bins=bins, right=False, include_lowest=True)

    yearly_raw_stats = build_yearly_curves(df, bins, cfg)
    if yearly_raw_stats.empty:
        raise RuntimeError("No yearly curves produced. Lower cfg.min_samples_per_bin or check data coverage.")
    yearly_raw_stats.to_csv(os.path.join(output_dir, "yearly_power_curves_raw.csv"), index=False)
    log.info("Saved: yearly_power_curves_raw.csv")

    df_mapped  = map_bin_stats_to_rows(df, yearly_raw_stats, cfg)
    ovp_flag   = _flag_overperf(df_mapped, cfg)
    df_no_over = df_mapped.loc[~ovp_flag].copy()
    log.info("Overperformance removed: %d rows (%.1f%%)", int(ovp_flag.sum()), 100.0 * ovp_flag.mean())

    # Propagate constraint flag into df_no_over if present
    if "flag_structural_constraint" in df_curve_clean.columns:
        df_no_over = df_no_over.merge(
            df_curve_clean[[cfg.time_col, "flag_structural_constraint"]].drop_duplicates(cfg.time_col),
            on=cfg.time_col, how="left",
        )
        df_no_over["flag_structural_constraint"] = df_no_over["flag_structural_constraint"].fillna(False)

    yearly_capability_stats = build_yearly_curves(df_no_over, bins, cfg)
    yearly_capability_stats.to_csv(
        os.path.join(output_dir, f"yearly_power_curves_capability_{cap_col}.csv"), index=False
    )
    log.info("Saved: yearly_power_curves_capability_%s.csv", cap_col)

    keep_cols = ["wind_bin", "n", "q50", cap_col, "v_left", "v_right", "v_center", "year"]
    yearly_capability_stats[keep_cols].to_csv(
        os.path.join(output_dir, "yearly_power_curves.csv"), index=False
    )
    log.info("Saved: yearly_power_curves.csv")

    # overall_clean built from all non-constrained hours across all years
    overall_clean = build_overall_curve(df_no_over, bins, cfg)
    log.info("Module 2 complete.")
    return yearly_raw_stats, yearly_capability_stats, overall_clean, df_no_over


yearly_raw_stats, yearly_capability_stats, overall_clean, df_no_over = \
    run_power_curves(df_curve_clean, cfg, output_dir)


# ==============================================================================
# CELL 4b — MODULE 2 CHARTS (optional, run separately)
# ==============================================================================

def plot_power_curves(df_curve_clean, df_no_over, yearly_capability_stats, overall_clean, cfg, output_dir):
    """Optional — call separately after run_power_curves() if charts are needed."""
    cap_col = percentile_col(cfg.cap_q)
    bins    = make_bins(cfg.wind_min_for_curve, cfg.wind_max_for_curve, cfg.bin_width)
    overall_uncleaned = build_overall_curve(df_curve_clean, bins, cfg)

    # Scatter subsampled to 5000 points for performance
    sample = df_no_over.sample(min(5000, len(df_no_over)), random_state=42)

    fig, ax = plt.subplots(figsize=(10, 6))
    apply_theme(ax)
    ax.scatter(sample["v"], sample["p_pu"], s=4, alpha=0.3, color=RAW_POINTS_COLOR,
               rasterized=True, label="Hourly data")
    if "v_center" in overall_uncleaned.columns:
        ax.plot(overall_uncleaned["v_center"], overall_uncleaned["q50"],
                color=CURVE_RAW_COLOR, lw=2, label="Raw P50")
    if "v_center" in overall_clean.columns:
        ax.plot(overall_clean["v_center"], overall_clean["q50"],
                color=CURVE_CLEAN_COLOR, lw=2, label="Clean P50")
    ax.set_xlabel("Wind speed (m/s)")
    ax.set_ylabel("Power (p.u.)")
    ax.set_title("Power Curve — Raw vs Clean")
    theme_legend(ax.legend())
    save_figure(fig, os.path.join(output_dir, "power_curve_unclean_vs_clean_q50.png"))
    plt.close(fig)

# To generate charts: run this line
# plot_power_curves(df_curve_clean, df_no_over, yearly_capability_stats, overall_clean, cfg, output_dir)


# ==============================================================================
# CELL 5 — MODULE 3: ANOMALY DETECTION & LOSS QUANTIFICATION
# ==============================================================================

def classify_anomalies_statistical(df_mapped: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    cap_col = percentile_col(cfg.cap_q)
    cap_bin = f"{cap_col}_bin"
    out = df_mapped.copy()
    out["flag_underperf_statistical"] = (
        (out["p_pu"] < out["q50_bin"] - cfg.underperf_mad_k * out["mad_bin"])
    ).fillna(False)
    out["flag_overperf_statistical"] = (
        (out["p_pu"] > out[cap_bin] + cfg.overperf_mad_k * out["mad_bin"])
        | (out["p_pu"] > cfg.ceiling_pu)
    ).fillna(False)
    return out


def classify_anomalies_isolation_forest(out: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    out["flag_isolation_forest"] = False
    if not cfg.use_isolation_forest or not HAS_SKLEARN:
        return out
    features = out[["v", "p_pu"]].dropna()
    if len(features) < 100:
        log.warning("IsolationForest: fewer than 100 rows — skipping.")
        return out
    clf   = IsolationForest(contamination=cfg.isolation_contamination, random_state=42, n_jobs=-1)
    preds = clf.fit_predict(features.to_numpy(dtype=float))
    out.loc[features.index, "flag_isolation_forest"] = preds == -1
    log.info("IsolationForest flagged %d anomalies (%.1f%%).",
             int(out["flag_isolation_forest"].sum()), 100.0 * out["flag_isolation_forest"].mean())
    return out


def classify_anomalies(df_mapped, cfg):
    out = classify_anomalies_statistical(df_mapped, cfg)
    out = classify_anomalies_isolation_forest(out, cfg)
    out["flag_underperf_anomaly"] = out["flag_underperf_statistical"]
    out["flag_overperf_anomaly"]  = out["flag_overperf_statistical"]
    has_iso = "flag_isolation_forest" in out.columns
    out["flag_any_anomaly"] = (
        out["flag_underperf_anomaly"] | out["flag_overperf_anomaly"]
        | (out["flag_isolation_forest"] if has_iso else False)
    )
    return out


def _build_overall_clean_lookup(overall_clean: pd.DataFrame, cfg: Config) -> pd.Series:
    return overall_clean.set_index("wind_bin")["q50"] * cfg.rated_mw


def attach_losses(df_class, yearly_capability_stats, overall_clean, cfg):
    bins    = make_bins(cfg.wind_min_for_curve, cfg.wind_max_for_curve, cfg.bin_width)
    cap_col = percentile_col(cfg.cap_q)
    df = df_class.copy()
    if "wind_bin" not in df.columns:
        df["wind_bin"] = pd.cut(df["v"], bins=bins, right=False, include_lowest=True)
    df["actual_mwh"] = df["p_pu"] * cfg.rated_mw
    df_mapped = map_bin_stats_to_rows(df, yearly_capability_stats, cfg)
    df_mapped = df_mapped.rename(columns={
        "q50_bin":          "q50_clean_bin",
        f"{cap_col}_bin":   f"{cap_col}_clean_bin",
        "mad_bin":          "mad_clean_bin",
    })
    df_mapped["expected_mwh_clean_q50"] = df_mapped["q50_clean_bin"] * cfg.rated_mw

    # Override reference for structurally constrained hours
    has_constraint = "flag_structural_constraint" in df_mapped.columns
    if has_constraint and df_mapped["flag_structural_constraint"].any():
        overall_lookup   = _build_overall_clean_lookup(overall_clean, cfg)
        constrained_mask = df_mapped["flag_structural_constraint"].fillna(False)
        df_mapped.loc[constrained_mask, "expected_mwh_clean_q50"] = (
            df_mapped.loc[constrained_mask, "wind_bin"].map(overall_lookup)
        )
        log.info("Module 3: %d constrained hours using overall_clean Q50 as loss reference.",
                 int(constrained_mask.sum()))

    df_mapped["lost_mwh_underperf"]  = (
        df_mapped["expected_mwh_clean_q50"] - df_mapped["actual_mwh"]
    ).clip(lower=0.0)
    df_mapped["lost_value_eur"] = compute_loss_value_eur(
        df_mapped["lost_mwh_underperf"], df_mapped["market_price"], cfg.base_ppa_price_eur_per_mwh
    )
    df_mapped["lost_mwh_underperf_only"]   = np.where(
        df_mapped.get("flag_underperf_anomaly", pd.Series(False, index=df_mapped.index)),
        df_mapped["lost_mwh_underperf"], 0.0
    )
    df_mapped["lost_value_underperf_only"] = np.where(
        df_mapped.get("flag_underperf_anomaly", pd.Series(False, index=df_mapped.index)),
        df_mapped["lost_value_eur"], 0.0
    )
    return df_mapped


def make_underperf_runs(df_underperf, cfg):
    d = df_underperf.sort_values(cfg.time_col).reset_index(drop=True)
    d["_gap"] = d[cfg.time_col].diff().dt.total_seconds().div(3600).gt(1)
    d["_run"] = d["_gap"].cumsum()
    runs = d.groupby("_run").agg(
        start_time=(cfg.time_col,         "min"),
        end_time=(cfg.time_col,           "max"),
        duration_hours=(cfg.time_col,     "count"),
        lost_mwh=("lost_mwh_underperf",   "sum"),
        lost_eur=("lost_value_eur",        "sum"),
    ).reset_index(drop=True)
    return runs


def _agg_anomaly_table(df_flagged, df_class, group_cols, cfg):
    if isinstance(group_cols, str):
        group_cols = [group_cols]
    agg = df_flagged.groupby(group_cols, as_index=False).agg(
        Hours_Underperforming=("flag_underperf_anomaly", "sum"),
        Underperf_Lost_MWh=("lost_mwh_underperf_only",   "sum"),
        Underperf_LostValue_EUR=("lost_value_underperf_only", "sum"),
        Expected_MWh=("expected_mwh_clean_q50",           "sum"),
        Actual_MWh=("actual_mwh",                         "sum"),
    )
    agg["Expected_Revenue_EUR"] = compute_loss_value_eur(
        agg["Expected_MWh"],
        df_flagged.groupby(group_cols)["market_price"].mean().reset_index(drop=True)
        if cfg.base_ppa_price_eur_per_mwh is None else pd.Series([cfg.base_ppa_price_eur_per_mwh] * len(agg)),
        cfg.base_ppa_price_eur_per_mwh,
    )
    total_hrs = df_class.groupby(group_cols, as_index=False).size().rename(columns={"size": "Hours_Total"})
    agg = agg.merge(total_hrs, on=group_cols, how="left")
    agg["ODI_(%underperf)"] = 100.0 * agg["Hours_Underperforming"] / agg["Hours_Total"]
    agg["ODI_(%_Loss_MWh)"] = 100.0 * agg["Underperf_Lost_MWh"]    / agg["Expected_MWh"]
    agg["ODI_(%_Loss_EUR)"] = 100.0 * agg["Underperf_LostValue_EUR"] / agg["Expected_Revenue_EUR"]
    return agg


def _build_constraint_loss_summary(df_flagged, constraint_runs, cfg):
    if constraint_runs.empty or "flag_structural_constraint" not in df_flagged.columns:
        return pd.DataFrame()
    constrained = df_flagged[df_flagged["flag_structural_constraint"].fillna(False)].copy()
    if constrained.empty:
        return pd.DataFrame()
    rows = []
    for _, run in constraint_runs.iterrows():
        period_mask = (
            (constrained[cfg.time_col] >= run["period_start"]) &
            (constrained[cfg.time_col] <= run["period_end"])
        )
        period_df = constrained[period_mask]
        if period_df.empty:
            continue
        rows.append({
            "period_start":     run["period_start"],
            "period_end":       run["period_end"],
            "duration_hours":   run["duration_hours"],
            "actual_mwh":       round(float(period_df["actual_mwh"].sum()), 1),
            "expected_mwh_q50": round(float(period_df["expected_mwh_clean_q50"].sum()), 1),
            "lost_mwh":         round(float(period_df["lost_mwh_underperf"].sum()), 1),
            "lost_eur":         round(float(period_df["lost_value_eur"].sum()), 0),
            "mean_q90_ratio":   round(float(run.get("mean_q90_ratio", np.nan)), 3),
            "reference_curve":  "overall_clean_q50",
        })
    if not rows:
        return pd.DataFrame()
    summary = pd.DataFrame(rows)
    totals = {
        "period_start":     "TOTAL", "period_end": "",
        "duration_hours":   int(summary["duration_hours"].sum()),
        "actual_mwh":       round(float(summary["actual_mwh"].sum()), 1),
        "expected_mwh_q50": round(float(summary["expected_mwh_q50"].sum()), 1),
        "lost_mwh":         round(float(summary["lost_mwh"].sum()), 1),
        "lost_eur":         round(float(summary["lost_eur"].sum()), 0),
        "mean_q90_ratio":   np.nan,
        "reference_curve":  "",
    }
    return pd.concat([summary, pd.DataFrame([totals])], ignore_index=True)


def run_anomaly_and_losses(df_no_over, yearly_capability_stats, overall_clean, constraint_runs, cfg, output_dir):
    bins = make_bins(cfg.wind_min_for_curve, cfg.wind_max_for_curve, cfg.bin_width)
    df   = df_no_over.copy()
    df["wind_bin"] = pd.cut(df["v"], bins=bins, right=False, include_lowest=True)

    if "flag_structural_constraint" not in df.columns:
        log.warning("Module 3: flag_structural_constraint not found — constrained hours will use yearly reference.")
        df["flag_structural_constraint"] = False

    df_mapped = map_bin_stats_to_rows(df, yearly_capability_stats, cfg)
    df_class  = classify_anomalies(df_mapped, cfg)

    flag_cols = [cfg.time_col, "year", "month_period", "v", "p_pu", "wind_bin",
                 "flag_underperf_anomaly", "flag_overperf_anomaly", "flag_any_anomaly",
                 "flag_structural_constraint"]
    if "flag_isolation_forest" in df_class.columns:
        flag_cols.append("flag_isolation_forest")
    flag_cols = [c for c in flag_cols if c in df_class.columns]
    df_class[flag_cols].to_csv(os.path.join(output_dir, "anomaly_point_flags.csv"), index=False)
    log.info("Saved: anomaly_point_flags.csv")

    df_flagged = attach_losses(df_class, yearly_capability_stats, overall_clean, cfg)

    anomaly_yearly  = _agg_anomaly_table(df_flagged, df_class, "year", cfg).rename(columns={"year": "Year"})
    anomaly_monthly = _agg_anomaly_table(df_flagged, df_class, ["year", "month_period"], cfg).rename(
        columns={"year": "Year", "month_period": "Month"})

    anomaly_yearly.to_csv(os.path.join(output_dir, "anomaly_yearly_summary.csv"),  index=False)
    anomaly_monthly.to_csv(os.path.join(output_dir, "anomaly_monthly_summary.csv"), index=False)
    log.info("Saved: anomaly_yearly_summary.csv, anomaly_monthly_summary.csv")

    df_under  = df_flagged.loc[df_flagged["flag_underperf_anomaly"]].copy()
    runs      = make_underperf_runs(df_under, cfg)
    runs.to_csv(os.path.join(output_dir, "anomaly_runs.csv"), index=False)
    long_runs = runs.loc[runs["duration_hours"] >= cfg.long_run_hours]
    long_runs.to_csv(os.path.join(output_dir, "anomaly_long_runs.csv"), index=False)
    log.info("Saved: anomaly_runs.csv, anomaly_long_runs.csv")

    constraint_loss = _build_constraint_loss_summary(df_flagged, constraint_runs, cfg)
    if not constraint_loss.empty:
        constraint_loss.to_csv(os.path.join(output_dir, "constraint_loss_summary.csv"), index=False)
        total_row = constraint_loss[constraint_loss["period_start"] == "TOTAL"].iloc[0]
        log.warning("CONSTRAINT LOSS TOTAL: %.0f lost MWh / %.0f lost EUR",
                    total_row["lost_mwh"], total_row["lost_eur"])

    return df_flagged


df_flagged = run_anomaly_and_losses(
    df_no_over, yearly_capability_stats, overall_clean, constraint_runs, cfg, output_dir
)


# ==============================================================================
# CELL 6 — MODULE 4: WIND NORMALISATION
# ==============================================================================

def build_reference_curve_lookup(overall_curve, curve_col, rated_mw):
    return overall_curve.set_index("wind_bin")[curve_col] * rated_mw


def compute_hourly_norm_ratios(df, reference_lookup, cfg):
    out = df.copy()
    if "wind_bin" not in out.columns:
        bins = make_bins(cfg.wind_min_for_curve, cfg.wind_max_for_curve, cfg.bin_width)
        out["wind_bin"] = pd.cut(out["v"], bins=bins, right=False, include_lowest=True)
    out["expected_mw"] = out["wind_bin"].map(reference_lookup)
    out["actual_mw"]   = out["p_pu"] * cfg.rated_mw
    valid = (
        out["v"].ge(cfg.norm_wind_min_mps)
        & out["expected_mw"].gt(0)
        & out["expected_mw"].notna()
        & out["actual_mw"].notna()
    )
    out["norm_ratio"] = np.where(valid, out["actual_mw"] / out["expected_mw"], np.nan)
    return out.loc[valid].copy()


def compute_monthly_norm_index(hourly_ratios, cfg):
    monthly = hourly_ratios.groupby("month_period", as_index=False)["norm_ratio"].mean()
    monthly.rename(columns={"norm_ratio": "avg_norm_ratio"}, inplace=True)
    historical_mean = float(monthly["avg_norm_ratio"].mean())
    monthly["Index_vs_Base"] = monthly["avg_norm_ratio"] / historical_mean * 100
    return monthly


def compute_yearly_norm_index(monthly_norm):
    monthly_norm = monthly_norm.copy()
    monthly_norm["year"] = monthly_norm["month_period"].dt.year
    yearly = monthly_norm.groupby("year", as_index=False)["avg_norm_ratio"].mean()
    yearly.rename(columns={"avg_norm_ratio": "avg_norm_ratio_yearly"}, inplace=True)
    historical_mean = float(yearly["avg_norm_ratio_yearly"].mean())
    yearly["Index_vs_HistoricalMean_%"] = yearly["avg_norm_ratio_yearly"] / historical_mean * 100
    return yearly


def run_wind_normalisation(df_no_over, overall_clean, cfg, output_dir, ref_curve_cols=("q50", "q90")):
    results = {}
    for ref_curve_col in ref_curve_cols:
        ref_label = _REF_LABELS.get(ref_curve_col, ref_curve_col.upper())
        log.info("Wind normalisation: %s (%s)", ref_curve_col, ref_label)
        reference_lookup = build_reference_curve_lookup(overall_clean, ref_curve_col, cfg.rated_mw)
        hourly_ratios    = compute_hourly_norm_ratios(df_no_over, reference_lookup, cfg)
        hourly_ratios.to_csv(os.path.join(output_dir, f"wind_norm_hourly_ratios_{ref_curve_col}.csv"), index=False)
        monthly_norm = compute_monthly_norm_index(hourly_ratios, cfg)
        monthly_norm.to_csv(os.path.join(output_dir, f"wind_norm_monthly_index_{ref_curve_col}.csv"), index=False)
        yearly_norm  = compute_yearly_norm_index(monthly_norm)
        yearly_norm.to_csv(os.path.join(output_dir, f"wind_norm_yearly_index_{ref_curve_col}.csv"), index=False)
        log.info("Saved: wind norm outputs for %s", ref_curve_col)
        results[ref_curve_col] = (hourly_ratios, monthly_norm, yearly_norm)
    return results


def plot_wind_normalisation(wind_norm_results, cfg, output_dir):
    """Optional — call separately after run_wind_normalisation() if charts are needed."""
    for ref_curve_col, (_, monthly_norm, yearly_norm) in wind_norm_results.items():
        ref_label = _REF_LABELS.get(ref_curve_col, ref_curve_col.upper())

        fig, ax = plt.subplots(figsize=(12, 5))
        apply_theme(ax)
        x = range(len(monthly_norm))
        colors = [CURVE_CLEAN_COLOR if v >= 100 else CURVE_RAW_COLOR
                  for v in monthly_norm["Index_vs_Base"]]
        ax.bar(x, monthly_norm["Index_vs_Base"], color=colors)
        ax.axhline(100, color=THEME_ACCENT, lw=1.5, linestyle="--", alpha=0.7)
        ax.set_title(f"Wind-Normalised Monthly Index vs Historical Mean ({ref_label})")
        ax.set_ylabel("Index (100 = historical mean)")
        save_figure(fig, os.path.join(output_dir, f"wind_norm_monthly_index_{ref_curve_col}.png"))
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(8, 5))
        apply_theme(ax)
        colors = [CURVE_CLEAN_COLOR if v >= 100 else CURVE_RAW_COLOR
                  for v in yearly_norm["Index_vs_HistoricalMean_%"]]
        ax.bar(yearly_norm["year"].astype(str), yearly_norm["Index_vs_HistoricalMean_%"], color=colors)
        ax.axhline(100, color=THEME_ACCENT, lw=1.5, linestyle="--", alpha=0.7)
        ax.set_title(f"Wind-Normalised Yearly Index vs Historical Mean ({ref_label})")
        ax.set_ylabel("Index (100 = historical mean)")
        save_figure(fig, os.path.join(output_dir, f"wind_norm_yearly_index_{ref_curve_col}.png"))
        plt.close(fig)


wind_norm_results = run_wind_normalisation(df_no_over, overall_clean, cfg, output_dir)

# To generate charts: run this line
# plot_wind_normalisation(wind_norm_results, cfg, output_dir)


# ==============================================================================
# CELL 7 — MODULE 5: DEGRADATION ANALYSIS
# ==============================================================================

def compute_hourly_operational_degradation(df_no_over, yearly_clean_stats, bins, cap_col, cfg):
    cap_bin = f"{cap_col}_bin"
    empty_summary = {
        "cap_col": cap_col,
        "baseline_cap_pu_operational_first_year": np.nan,
        "slope_pu_per_year": np.nan, "intercept_pu": np.nan,
        "slope_pct_per_year_relative": np.nan,
        "ci95_pct": None, "r2": np.nan, "n": 0,
        "n_constraint_hours_excluded": 0,
    }

    if yearly_clean_stats.empty or cap_col not in yearly_clean_stats.columns:
        return pd.DataFrame(), empty_summary

    df = df_no_over.copy()
    df["wind_bin"] = pd.cut(df["v"], bins=bins, right=False, include_lowest=True)
    df = map_bin_stats_to_rows(df, yearly_clean_stats, cfg)
    df["v_center"] = df["wind_bin"].astype(object).apply(
        lambda iv: 0.5 * (float(iv.left) + float(iv.right)) if pd.notna(iv) else np.nan
    ).astype(float)
    df = df[
        df["v_center"].between(cfg.op_wind_min, cfg.op_wind_max, inclusive="both")
        & (df["q50_bin"] >= cfg.min_median_pu_for_operational)
    ].copy()
    df = df.dropna(subset=[cap_bin, "p_pu", cfg.time_col]).copy()
    df["year_fraction"] = df[cfg.time_col].dt.year + (df[cfg.time_col].dt.dayofyear - 1) / 365.25
    df["residual_pu"]   = df["p_pu"] - df[cap_bin]

    # Exclude structurally constrained hours from OLS
    has_constraint   = "flag_structural_constraint" in df.columns
    constraint_mask  = df["flag_structural_constraint"].fillna(False) if has_constraint else pd.Series(False, index=df.index)
    n_excluded       = int(constraint_mask.sum())
    df_fit           = df[~constraint_mask].copy()

    if n_excluded > 0:
        log.info("Module 5 (%s): excluding %d constrained hours from OLS (%.1f%%).",
                 cap_col, n_excluded, 100.0 * n_excluded / max(len(df), 1))

    if len(df_fit) < 100:
        log.warning("Module 5 (%s): fewer than 100 qualifying hours after constraint exclusion.", cap_col)
        empty_summary["n_constraint_hours_excluded"] = n_excluded
        return df, empty_summary

    series        = df_fit.set_index(cfg.time_col)["residual_pu"].sort_index()
    deseasonalised = remove_seasonal_component(series, period=8760)
    df_fit["residual_deseasonalised"] = deseasonalised.values

    X = df_fit["year_fraction"].values
    y = df_fit["residual_deseasonalised"].values
    valid = ~np.isnan(y)
    X, y = X[valid], y[valid]
    if len(X) < 2:
        empty_summary["n_constraint_hours_excluded"] = n_excluded
        return df, empty_summary

    slope, intercept = np.polyfit(X, y, 1)
    y_pred = slope * X + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2     = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else np.nan

    n = len(X); se_slope = np.nan; ci95 = None
    try:
        from scipy import stats as scipy_stats
        residuals = y - y_pred
        mse = np.sum(residuals ** 2) / (n - 2)
        ssx = np.sum((X - X.mean()) ** 2)
        se_slope = float(np.sqrt(mse / ssx)) if ssx > 0 else np.nan
        t_crit   = float(scipy_stats.t.ppf(0.975, df=n - 2))
        ci95     = (float(slope - t_crit * se_slope), float(slope + t_crit * se_slope))
    except ImportError:
        if not np.isnan(se_slope):
            ci95 = (float(slope - 1.96 * se_slope), float(slope + 1.96 * se_slope))

    first_year  = int(df_fit["year_fraction"].min())
    baseline_df = df_fit[df_fit["year_fraction"].between(first_year, first_year + 1)]
    baseline_cap = float(baseline_df[cap_bin].median()) if len(baseline_df) else np.nan
    slope_pct    = float(slope / baseline_cap * 100) if baseline_cap and not np.isnan(baseline_cap) else np.nan

    summary = {
        "cap_col": cap_col,
        "baseline_cap_pu_operational_first_year": round(baseline_cap, 4),
        "slope_pu_per_year":           round(float(slope), 6),
        "intercept_pu":                round(float(intercept), 6),
        "slope_pct_per_year_relative": round(slope_pct, 4) if not np.isnan(slope_pct) else np.nan,
        "ci95_pct": (round(ci95[0] / baseline_cap * 100, 4), round(ci95[1] / baseline_cap * 100, 4))
                    if ci95 and baseline_cap else None,
        "r2": round(r2, 4),
        "n":  n,
        "n_constraint_hours_excluded": n_excluded,
    }
    log.info("Module 5 (%s): slope=%.4f p.u./yr (%.2f%%/yr), R²=%.3f, n=%d, excluded=%d",
             cap_col, slope, slope_pct if not np.isnan(slope_pct) else 0.0, r2, n, n_excluded)
    return df, summary


def run_degradation(df_no_over, yearly_capability_stats, cfg, output_dir):
    bins    = make_bins(cfg.wind_min_for_curve, cfg.wind_max_for_curve, cfg.bin_width)
    cap_col = percentile_col(cfg.cap_q)
    results = {}
    for ref_col in ["q50", cap_col]:
        log.info("Module 5: degradation vs %s", ref_col)
        hourly_df, deg = compute_hourly_operational_degradation(
            df_no_over, yearly_capability_stats, bins, ref_col, cfg
        )
        if hourly_df.empty:
            results[ref_col] = {"hourly": pd.DataFrame(), "summary": deg}
            continue
        by_year = hourly_df.groupby("year").agg(
            mean_residual_pu=("residual_pu", "mean"),
            median_residual_pu=("residual_pu", "median"),
            n_hours=("residual_pu", "count"),
        ).reset_index()
        hourly_df.to_csv(os.path.join(output_dir, f"hourly_operational_residuals_vs_{ref_col}.csv"), index=False)
        by_year.to_csv(os.path.join(output_dir, f"hourly_operational_residuals_by_year_vs_{ref_col}.csv"), index=False)
        pd.DataFrame([deg]).to_csv(
            os.path.join(output_dir, f"hourly_operational_degradation_estimate_vs_{ref_col}.csv"), index=False
        )
        log.info("Saved: degradation outputs for %s", ref_col)
        results[ref_col] = {"hourly": hourly_df, "summary": deg}
    return results


def plot_degradation_results(degradation_results, cfg, output_dir):
    """Optional — call separately after run_degradation() if charts are needed."""
    for ref_col, result in degradation_results.items():
        hourly_df = result.get("hourly")
        deg       = result.get("summary", {})
        if hourly_df is None or hourly_df.empty:
            continue
        ref_label = _REF_LABELS.get(ref_col, ref_col.upper())
        fig, ax = plt.subplots(figsize=(12, 5))
        apply_theme(ax)
        sample = hourly_df.dropna(subset=["year_fraction", "residual_pu"])
        sample = sample.sample(min(5000, len(sample)), random_state=42)
        ax.scatter(sample["year_fraction"], sample["residual_pu"],
                   s=3, alpha=0.2, color=RAW_POINTS_COLOR, rasterized=True, label="Hourly residual")
        slope = deg.get("slope_pu_per_year")
        intercept = deg.get("intercept_pu")
        if slope is not None and intercept is not None and not np.isnan(slope):
            x_range = np.linspace(hourly_df["year_fraction"].min(), hourly_df["year_fraction"].max(), 100)
            ax.plot(x_range, slope * x_range + intercept, color=CURVE_CLEAN_COLOR, lw=2,
                    label=f"OLS trend: {slope*100:.3f}%/yr")
        ax.axhline(0, color=THEME_ACCENT, lw=1, linestyle="--", alpha=0.5)
        ax.set_xlabel("Year")
        ax.set_ylabel("Residual p.u.")
        ax.set_title(f"Operational Residual Trend vs {ref_label}")
        theme_legend(ax.legend())
        save_figure(fig, os.path.join(output_dir, f"hourly_operational_residual_trend_vs_{ref_col}.png"))
        plt.close(fig)


degradation_results = run_degradation(df_no_over, yearly_capability_stats, cfg, output_dir)

# To generate charts: run this line
# plot_degradation_results(degradation_results, cfg, output_dir)


# ==============================================================================
# CELL 8 — MODULE 6: COMMERCIAL REPORTING
# ==============================================================================

def compute_constraint_proxy_timeseries(df_no_over, yearly_capability_stats, cfg):
    cap_col = percentile_col(cfg.cap_q)
    bins    = make_bins(cfg.wind_min_for_curve, cfg.wind_max_for_curve, cfg.bin_width)
    ts = df_no_over.copy()
    ts["wind_bin"] = pd.cut(ts["v"], bins=bins, right=False, include_lowest=True)
    ts = map_bin_stats_to_rows(ts, yearly_capability_stats, cfg)
    ts["actual_mwh"]      = ts["p_pu"] * cfg.rated_mw
    cap_bin = f"{cap_col}_bin"
    ts["lost_mwh_proxy"]  = ((ts[cap_bin] - ts["q50_bin"]) * cfg.rated_mw).clip(lower=0.0)
    return ts


def compute_commercial_baseline(ts, cfg):
    cap_col = percentile_col(cfg.cap_q)
    rows = []
    for y in sorted(ts["year"].unique()):
        t             = ts[ts["year"] == y]
        actual_mwh    = float(t["actual_mwh"].sum())
        contract_rev  = float(compute_loss_value_eur(t["actual_mwh"], t["market_price"], cfg.base_ppa_price_eur_per_mwh).sum())
        avg_price     = float(t["market_price"].mean()) if cfg.base_ppa_price_eur_per_mwh is None else cfg.base_ppa_price_eur_per_mwh
        target_rev    = float(cfg.p50_target_mwh_per_year * avg_price)
        lost_mwh      = float(t["lost_mwh_proxy"].sum())
        lost_value    = float(compute_loss_value_eur(t["lost_mwh_proxy"], t["market_price"], cfg.base_ppa_price_eur_per_mwh).sum())
        rows.append((y, actual_mwh, contract_rev, contract_rev - target_rev, lost_mwh, lost_value))
    return pd.DataFrame(rows, columns=[
        "Year", "Actual_MWh", "Contract_Revenue_EUR",
        "Contract_Revenue_vs_P50Target_EUR",
        f"LostEnergyProxy_MWh_({cap_col}-q50)", "LostValue_EUR",
    ])


def compute_ppa_scenarios(ts, cfg):
    cap_col = percentile_col(cfg.cap_q)
    years = sorted(ts["year"].unique()); ppa_rows = []
    for ppa in cfg.ppa_price_scenarios:
        for y in years:
            t          = ts[ts["year"] == y]
            actual_mwh = float(t["actual_mwh"].sum())
            rev        = actual_mwh * ppa
            gap        = rev - cfg.p50_target_mwh_per_year * ppa
            val_1pct   = 0.01 * actual_mwh * ppa
            ppa_rows.append((y, ppa, rev, gap, val_1pct))
    scenario_ppa = pd.DataFrame(ppa_rows, columns=[
        "Year", "PPA_EUR_per_MWh", "ContractRevenue_EUR",
        "Revenue_vs_P50Target_EUR", "Value_of_1pct_EUR_per_year",
    ])
    if cfg.base_ppa_price_eur_per_mwh in cfg.ppa_price_scenarios:
        base_rev = (
            scenario_ppa[scenario_ppa["PPA_EUR_per_MWh"] == cfg.base_ppa_price_eur_per_mwh]
            [["Year", "ContractRevenue_EUR"]].rename(columns={"ContractRevenue_EUR": "BaseRevenue_EUR"})
        )
        scenario_ppa = scenario_ppa.merge(base_rev, on="Year", how="left")
        scenario_ppa["Revenue_Uplift_vs_Base_EUR"] = (
            scenario_ppa["ContractRevenue_EUR"] - scenario_ppa["BaseRevenue_EUR"])
    else:
        scenario_ppa["BaseRevenue_EUR"]            = np.nan
        scenario_ppa["Revenue_Uplift_vs_Base_EUR"] = np.nan
    return scenario_ppa


def run_commercial(df_no_over, yearly_capability_stats, cfg, output_dir):
    cap_col = percentile_col(cfg.cap_q)
    ts      = compute_constraint_proxy_timeseries(df_no_over, yearly_capability_stats, cfg)
    commercial_baseline = compute_commercial_baseline(ts, cfg)
    commercial_baseline.to_csv(
        os.path.join(output_dir, f"commercial_summary_board_level_{cap_col}_vs_q50.csv"), index=False)
    log.info("Saved: commercial_summary_board_level_%s_vs_q50.csv", cap_col)
    scenario_ppa = compute_ppa_scenarios(ts, cfg)
    scenario_ppa.to_csv(os.path.join(output_dir, f"scenario_ppa_{cap_col}_vs_q50.csv"), index=False)
    log.info("Saved: scenario_ppa_%s_vs_q50.csv", cap_col)
    return commercial_baseline, scenario_ppa


commercial_baseline, scenario_ppa = run_commercial(
    df_no_over, yearly_capability_stats, cfg, output_dir
)

log.info("Pipeline complete. All outputs saved to: %s", output_dir)
