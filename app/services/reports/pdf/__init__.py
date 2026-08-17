"""PDF rendering for generated reports.

``render_report_pdf`` is sync/CPU-bound — call it via ``asyncio.to_thread``.
It consumes the same stored report + section rows the frontend renders, so
the two outputs can never drift.
"""

from pathlib import Path

from app.models.report import Report


def render_report_pdf(report: Report, tmp_dir: Path) -> Path:
    """Render ``report`` into ``tmp_dir`` and return the PDF path.

    Dispatches to the per-type renderer. The report must be loaded with its
    ``sections`` and scope relationships (windfarm/portfolio).
    """
    if report.report_type == "opportunity":
        from app.services.reports.pdf.renderers.opportunity import render

        return render(report, tmp_dir)
    if report.report_type == "digest":
        from app.services.reports.pdf.renderers.digest import render

        return render(report, tmp_dir)
    raise ValueError(f"No PDF renderer for report type {report.report_type}")
