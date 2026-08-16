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
