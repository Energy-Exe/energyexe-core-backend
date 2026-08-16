"""Tests for the reports platform (EPR-81/82) — registry, helpers, PDF renderer.

DB-integration paths (orchestrator, service) are exercised against staging;
these tests cover the pure logic: registry shape, metric formatting, and a
real reportlab+matplotlib render from in-memory model instances.
"""

from datetime import date, datetime

from app.models.report import Report, ReportSection, ReportStatus, SectionStatus
from app.models.windfarm import Windfarm
from app.services.reports.data_builders.opportunity import _fmt_number, _key_metric
from app.services.reports.registry import REPORT_TYPE_REGISTRY, get_report_type


class TestRegistry:
    def test_opportunity_type_registered(self):
        spec = get_report_type("opportunity")
        assert spec is not None
        assert spec.scope == "windfarm"
        assert {s.key for s in spec.sections} == {
            "executive_summary",
            "key_metrics",
            "findings",
            "action_plan",
        }

    def test_exec_summary_is_pass2_and_renders_first(self):
        spec = get_report_type("opportunity")
        exec_summary = spec.section("executive_summary")
        assert exec_summary.pass_number == 2
        assert exec_summary.render_first is True
        assert exec_summary.ai_enabled is True
        assert spec.display_ordered()[0].key == "executive_summary"

    def test_data_sections_have_builders_and_ai_sections_do_not(self):
        spec = get_report_type("opportunity")
        assert spec.section("key_metrics").data_builder is not None
        assert spec.section("findings").data_builder is not None
        assert spec.section("findings").ai_enabled is False
        assert spec.section("action_plan").data_builder is None
        assert spec.section("action_plan").ai_enabled is True

    def test_section_kinds_are_frontend_registry_keys(self):
        valid = {"metric_strip", "findings_table", "action_plan", "narrative", "scorecard", "chart_embed"}
        for spec in REPORT_TYPE_REGISTRY.values():
            for section in spec.sections:
                assert section.kind in valid, f"{spec.code}.{section.key}: {section.kind}"


class TestKeyMetricHelpers:
    def test_fmt_number_scales(self):
        assert _fmt_number(2_450_000) == "2.5M"
        assert _fmt_number(12_345) == "12,345"
        assert _fmt_number(47.3) == "47.3"

    def test_key_metric_prefers_known_slots(self):
        slots = {"note": "x", "lost_eur": 125_000.0, "other_pct": 5.0}
        assert "Lost EUR" in _key_metric(slots)

    def test_key_metric_falls_back_to_first_numeric(self):
        assert _key_metric({"custom_thing": 7}) == "Custom thing: 7"
        assert _key_metric({"only_text": "abc"}) is None
        assert _key_metric({}) is None


class TestOpportunityPdf:
    def _fake_report(self) -> Report:
        report = Report(
            report_type="opportunity",
            scope_type="windfarm",
            windfarm_id=1,
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            version=1,
            status=ReportStatus.COMPLETE,
            title="Opportunity Report — Testfarm",
            requested_by_id=1,
        )
        report.windfarm = Windfarm(name="Testfarm", code="TESTF")
        report.sections = [
            ReportSection(
                section_key="key_metrics",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=1,
                data={
                    "cards": [
                        {"label": "P50 attainment", "value": "92.1", "unit": "%"},
                        {"label": "Lost value (period)", "value": "1,250,000", "unit": "EUR"},
                        {"label": "Capture rate vs zone", "value": "88.4", "unit": "%"},
                        {"label": "Schemas flagged", "value": "5"},
                    ]
                },
                generated_at=datetime(2026, 8, 17),
            ),
            ReportSection(
                section_key="findings",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=2,
                data={
                    "rows": [
                        {
                            "schema_code": "FIN-01",
                            "domain": "Financial",
                            "display_name": "P50 Generation Attainment",
                            "key_metric": "Attainment %: 89",
                            "severity": "confirmed",
                        },
                        {
                            "schema_code": "OPS-02",
                            "domain": "Operational",
                            "display_name": "Performance Seasonality",
                            "key_metric": None,
                            "severity": "pass",
                        },
                        {
                            "schema_code": "MKT-01",
                            "domain": "Market",
                            "display_name": "Low Capture Rate — Contracting",
                            "key_metric": None,
                            "severity": "suppressed",
                            "suppression_reason": "data gap >= 72h detected",
                        },
                    ],
                    "severity_counts": {"confirmed": 1, "pass": 1, "suppressed": 1},
                },
                generated_at=datetime(2026, 8, 17),
            ),
        ]
        return report

    def test_renders_pdf_with_findings_and_metrics(self, tmp_path):
        from app.services.reports.pdf import render_report_pdf

        out = render_report_pdf(self._fake_report(), tmp_path)
        assert out.exists()
        content = out.read_bytes()
        assert content[:5] == b"%PDF-"
        assert len(content) > 10_000  # non-trivial: tables + embedded chart

    def test_unknown_type_raises(self, tmp_path):
        import pytest

        from app.services.reports.pdf import render_report_pdf

        report = self._fake_report()
        report.report_type = "mystery"
        with pytest.raises(ValueError):
            render_report_pdf(report, tmp_path)
