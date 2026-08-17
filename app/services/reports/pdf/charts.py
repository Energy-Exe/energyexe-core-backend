"""Matplotlib chart helpers for report PDFs.

Brand palette follows the brain-agent ``eexe_style`` theme (light variant for
print). Always Agg backend, always ``plt.close(fig)`` — renders run inside the
API process and must not leak figures.
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

_SEVERITY_COLORS = {
    "confirmed": "#DC2626",
    "indicative": "#D97706",
    "watch": "#2563EB",
    "pass": "#16A34A",
    "suppressed": "#94A3B8",
}
_INK = "#0F1B2D"
_SLATE = "#475569"
_LINE = "#E2E8F0"


def generation_comparison_chart(series: dict, out_path: Path) -> Path:
    """Digest generation chart: current-period bars, previous period overlaid
    as a dashed step line when the two windows have the same bucket count."""
    current = series.get("current") or {}
    previous = series.get("previous") or {}
    points = current.get("points") or []
    prev_points = previous.get("points") or []
    labels = [p["label"] for p in points]
    values = [p["gwh"] for p in points]

    fig, ax = plt.subplots(figsize=(6.4, 2.6), dpi=200)
    try:
        x = range(len(values))
        ax.bar(x, values, color="#2563EB", width=0.7, label=current.get("label"))
        if prev_points and len(prev_points) == len(points):
            ax.step(
                x,
                [p["gwh"] for p in prev_points],
                where="mid",
                color=_SLATE,
                linestyle="--",
                linewidth=1.2,
                label=previous.get("label"),
            )
            # Anchored above the axes so it never overlaps the tallest bar.
            ax.legend(
                fontsize=8,
                frameon=False,
                loc="lower right",
                bbox_to_anchor=(1.0, 1.0),
                ncol=2,
                labelcolor=_SLATE,
            )
        ax.margins(y=0.08)
        ax.set_xticks(list(x))
        step = max(1, len(labels) // 10)
        ax.set_xticklabels(
            [lbl if i % step == 0 else "" for i, lbl in enumerate(labels)],
            fontsize=8,
            color=_SLATE,
        )
        ax.tick_params(colors=_SLATE, labelsize=8, length=0)
        ax.set_ylabel(series.get("unit", "GWh"), fontsize=9, color=_SLATE)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        for spine in ("left", "bottom"):
            ax.spines[spine].set_color(_LINE)
        ax.set_title("Generation", loc="left", fontsize=11, color=_INK, fontweight="bold")
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    finally:
        plt.close(fig)
    return out_path


def wind_norm_chart(series: dict, out_path: Path) -> Path:
    """Monthly wind-normalised performance index: bars above/below the P50
    baseline (green over, orange under), dashed reference at the baseline."""
    points = series.get("points") or []
    baseline = float(series.get("baseline", 100))
    labels = [p["label"] for p in points]
    values = [float(p["index"]) for p in points]
    colors = ["#22C55E" if v >= baseline else "#F97316" for v in values]

    fig, ax = plt.subplots(figsize=(6.4, 2.6), dpi=200)
    try:
        x = range(len(values))
        ax.bar(x, values, color=colors, width=0.7)
        ax.axhline(baseline, color=_SLATE, linestyle="--", linewidth=1.0)
        ax.text(
            len(values) - 0.4 if values else 0,
            baseline,
            f"P50 = {baseline:.0f}",
            fontsize=7,
            color=_SLATE,
            va="bottom",
            ha="right",
        )
        # Zoom to the interesting band around the baseline rather than 0.
        if values:
            lo, hi = min(values + [baseline]), max(values + [baseline])
            pad = max((hi - lo) * 0.25, 2.0)
            ax.set_ylim(lo - pad, hi + pad)
        ax.set_xticks(list(x))
        step = max(1, len(labels) // 10)
        ax.set_xticklabels(
            [lbl if i % step == 0 else "" for i, lbl in enumerate(labels)],
            fontsize=8,
            color=_SLATE,
        )
        ax.tick_params(colors=_SLATE, labelsize=8, length=0)
        ax.set_ylabel("Index (P50 = 100)", fontsize=9, color=_SLATE)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        for spine in ("left", "bottom"):
            ax.spines[spine].set_color(_LINE)
        ax.set_title(
            "Wind-normalised performance",
            loc="left",
            fontsize=11,
            color=_INK,
            fontweight="bold",
        )
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    finally:
        plt.close(fig)
    return out_path


def capture_rate_line_chart(series: dict, out_path: Path) -> Path:
    """Monthly capture rate (%) with a dashed reference at 100% (= market
    average price achieved)."""
    points = series.get("points") or []
    labels = [p["label"] for p in points]
    values = [float(p["capture_rate_pct"]) for p in points]

    fig, ax = plt.subplots(figsize=(6.4, 2.6), dpi=200)
    try:
        x = list(range(len(values)))
        ax.plot(x, values, color="#2563EB", linewidth=1.6, marker="o", markersize=3)
        ax.axhline(100, color=_SLATE, linestyle="--", linewidth=1.0)
        ax.text(
            x[-1] if x else 0,
            100,
            "market avg",
            fontsize=7,
            color=_SLATE,
            va="bottom",
            ha="right",
        )
        ax.margins(y=0.15)
        ax.set_xticks(x)
        step = max(1, len(labels) // 10)
        ax.set_xticklabels(
            [lbl if i % step == 0 else "" for i, lbl in enumerate(labels)],
            fontsize=8,
            color=_SLATE,
        )
        ax.tick_params(colors=_SLATE, labelsize=8, length=0)
        ax.set_ylabel("Capture rate (%)", fontsize=9, color=_SLATE)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        for spine in ("left", "bottom"):
            ax.spines[spine].set_color(_LINE)
        ax.set_title("Capture rate", loc="left", fontsize=11, color=_INK, fontweight="bold")
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    finally:
        plt.close(fig)
    return out_path


def severity_bar_chart(severity_counts: dict, out_path: Path) -> Path:
    """Horizontal bar chart of findings by severity for the PDF header area."""
    order = ["confirmed", "indicative", "watch", "pass", "suppressed"]
    labels = [s.capitalize() for s in order]
    values = [int(severity_counts.get(s, 0) or 0) for s in order]
    bar_colors = [_SEVERITY_COLORS[s] for s in order]

    fig, ax = plt.subplots(figsize=(6.4, 2.2), dpi=200)
    try:
        bars = ax.barh(labels[::-1], values[::-1], color=bar_colors[::-1], height=0.62)
        ax.bar_label(bars, padding=4, fontsize=9, color=_SLATE)
        ax.set_xlabel("")
        ax.tick_params(colors=_SLATE, labelsize=9, length=0)
        ax.set_xticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.set_title("Findings by severity", loc="left", fontsize=11, color=_INK, fontweight="bold")
        ax.margins(x=0.08)
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    finally:
        plt.close(fig)
    return out_path
