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
