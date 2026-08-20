"""Opportunity Report PDF renderer (EPR-88)."""

from datetime import datetime, timezone
from pathlib import Path

from app.models.report import Report, SectionStatus
from app.services.reports.pdf.builder import MUTED, SEVERITY_COLORS, PdfBuilder
from app.services.reports.pdf.charts import (
    capture_rate_line_chart,
    generation_comparison_chart,
    severity_bar_chart,
    wind_norm_chart,
)

_FLAGGED = ("confirmed", "indicative", "watch")


def _section(report: Report, key: str):
    for s in report.sections:
        if s.section_key == key:
            return s
    return None


def _generated(section) -> bool:
    return section is not None and section.status == SectionStatus.GENERATED and section.data


def render(report: Report, tmp_dir: Path) -> Path:
    scope_name = report.windfarm.name if report.windfarm is not None else report.title
    subtitle = (
        f"Opportunity assessment · {report.period_start:%d %b %Y} – {report.period_end:%d %b %Y}"
        f" · generated {datetime.now(timezone.utc):%d %b %Y} · v{report.version}"
    )
    pdf = PdfBuilder(f"{scope_name} — Opportunity Report", subtitle=subtitle)

    exec_summary = _section(report, "executive_summary")
    if exec_summary is not None and (exec_summary.narrative_json or exec_summary.narrative_text):
        pdf.heading("Executive Summary")
        summary = exec_summary.narrative_json or {}
        if summary.get("bullets"):
            if summary.get("overall_assessment"):
                pdf.paragraph(summary["overall_assessment"])
            pdf.bullets([b.get("text", "") for b in summary["bullets"]])
        elif exec_summary.narrative_text:
            for para in exec_summary.narrative_text.split("\n\n"):
                if para.strip():
                    pdf.paragraph(para.strip())

    metrics = _section(report, "key_metrics")
    if _generated(metrics):
        pdf.heading("Key Metrics")
        cards = metrics.data.get("cards", [])
        # Six cards overflow the fixed-width card row — chunk into rows of <=4.
        for i in range(0, len(cards), 4):
            pdf.metric_cards(cards[i : i + 4])
        if metrics.data.get("previous_label"):
            pdf.small(f"Deltas vs {metrics.data['previous_label']}.")
        if metrics.data.get("note"):
            pdf.small(metrics.data["note"])

    generation = _section(report, "generation_chart")
    if _generated(generation) and (generation.data.get("series") or {}).get("current", {}).get(
        "points"
    ):
        pdf.heading("Generation")
        chart_path = generation_comparison_chart(
            generation.data["series"], tmp_dir / "generation.png"
        )
        pdf.image(chart_path, width_in=6.2)

    findings = _section(report, "findings")
    if _generated(findings):
        pdf.heading("Performance Snapshot")
        counts = findings.data.get("severity_counts", {})
        chart_path = severity_bar_chart(counts, tmp_dir / "severity.png")
        pdf.image(chart_path, width_in=5.6)

        rows = findings.data.get("rows", [])
        body = []
        row_colors = []
        for r in rows:
            severity = (r.get("severity") or "").lower()
            body.append(
                [
                    r.get("domain", ""),
                    r.get("display_name", ""),
                    r.get("schema_code", ""),
                    r.get("key_metric") or "—",
                    severity.capitalize(),
                ]
            )
            row_colors.append(MUTED if severity == "pass" else SEVERITY_COLORS.get(severity))
        pdf.table(
            ["Domain", "Finding", "Schema", "Key metric", "Severity"],
            body,
            row_text_colors=row_colors,
        )

        suppressed = [r for r in rows if (r.get("severity") or "").lower() == "suppressed"]
        for r in suppressed:
            if r.get("suppression_reason"):
                pdf.small(f"{r['schema_code']} suppressed: {r['suppression_reason']}")

        # Per-finding evidence for flagged rows (pass/suppressed skipped to
        # keep the PDF tight). Values are builder-formatted — identical to web.
        flagged = [
            r for r in rows if (r.get("severity") or "").lower() in _FLAGGED and r.get("evidence")
        ]
        if flagged:
            pdf.heading("Finding Evidence", level=2)
            for r in flagged:
                pdf.heading(f"{r.get('schema_code', '')} — {r.get('display_name', '')}", level=3)
                if r.get("one_liner"):
                    pdf.small(r["one_liner"])
                pdf.table(
                    ["Metric", "Value"],
                    [[item.get("label", ""), item.get("value", "")] for item in r["evidence"]],
                )
                for note in r.get("notes") or []:
                    pdf.small(note)
                period = r.get("detection_period") or {}
                if period.get("start") and period.get("end"):
                    pdf.small(f"Detected over {period['start']} – {period['end']}.")

    wind_norm = _section(report, "wind_norm_chart")
    if _generated(wind_norm) and (wind_norm.data.get("series") or {}).get("points"):
        pdf.heading("Wind-Normalised Performance")
        chart_path = wind_norm_chart(wind_norm.data["series"], tmp_dir / "wind_norm.png")
        pdf.image(chart_path, width_in=6.2)

    capture = _section(report, "capture_rate_chart")
    if _generated(capture) and (capture.data.get("series") or {}).get("points"):
        pdf.heading("Capture Rate Trend")
        chart_path = capture_rate_line_chart(capture.data["series"], tmp_dir / "capture_rate.png")
        pdf.image(chart_path, width_in=6.2)

    action_plan = _section(report, "action_plan")
    if action_plan is not None and (action_plan.narrative_json or action_plan.narrative_text):
        pdf.heading("Action Plan")
        tiers = (action_plan.narrative_json or {}).get("tiers")
        if tiers:
            for tier in tiers:
                pdf.heading(f"{tier.get('tier', '')} — {tier.get('label', '')}", level=3)
                items = []
                for action in tier.get("actions", []):
                    tags = " · ".join(
                        t
                        for t in (
                            action.get("horizon"),
                            action.get("external"),
                            ", ".join(action.get("linked_schemas") or []) or None,
                        )
                        if t
                    )
                    items.append(action.get("title", "") + (f"  ({tags})" if tags else ""))
                if items:
                    pdf.bullets(items)
                if tier.get("context"):
                    pdf.small(tier["context"])
        elif action_plan.narrative_text:
            for para in action_plan.narrative_text.split("\n\n"):
                if para.strip():
                    pdf.paragraph(para.strip())

    pdf.small(
        "Generated by the EnergyExe platform. Findings reflect the detection engine's "
        "current assessment; metrics are computed over the stated period."
    )

    out = tmp_dir / "report.pdf"
    pdf.save(out)
    return out
