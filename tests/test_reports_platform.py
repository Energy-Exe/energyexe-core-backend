"""Tests for the reports platform (EPR-81/82) — registry, helpers, PDF renderer.

DB-integration paths (orchestrator, service) are exercised against staging;
these tests cover the pure logic: registry shape, metric formatting, and a
real reportlab+matplotlib render from in-memory model instances.
"""

from datetime import date, datetime

import pytest

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
            "generation_chart",
            "findings",
            "wind_norm_chart",
            "capture_rate_chart",
            "action_plan",
        }
        # The trend charts pair up side-by-side; findings + action plan are full.
        assert spec.section("wind_norm_chart").layout == "two_col_left"
        assert spec.section("capture_rate_chart").layout == "two_col_right"
        assert spec.section("findings").layout == "full"
        assert spec.section("action_plan").layout == "full"

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
        valid = {
            "metric_strip",
            "findings_table",
            "action_plan",
            "narrative",
            "scorecard",
            "chart_embed",
        }
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

    def test_renders_narratives_with_markup_hostile_text(self, tmp_path):
        """Structured exec summary + action plan render, and '&'/'<' in LLM
        text must not abort reportlab's markup parser."""
        from app.services.reports.pdf import render_report_pdf

        report = self._fake_report()
        report.sections.append(
            ReportSection(
                section_key="executive_summary",
                status=SectionStatus.GENERATED,
                pass_number=2,
                display_order=0,
                narrative_json={
                    "overall_assessment": "Solid year for O&M <with caveats>.",
                    "bullets": [
                        {
                            "text": "P50 attainment reached 92.1%.",
                            "source_sections": ["key_metrics"],
                        },
                        {
                            "text": "R&D & maintenance flagged FIN-01.",
                            "source_sections": ["findings"],
                        },
                    ],
                },
                narrative_text="fallback",
                generated_at=datetime(2026, 8, 17),
            )
        )
        report.sections.append(
            ReportSection(
                section_key="action_plan",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=3,
                narrative_json={
                    "tiers": [
                        {
                            "tier": "P1",
                            "label": "Strategic",
                            "actions": [
                                {
                                    "title": "Audit P50 shortfall & report",
                                    "horizon": "0-3 months",
                                    "external": "OEM",
                                    "linked_schemas": ["FIN-01"],
                                }
                            ],
                            "context": "Grounded in the confirmed attainment gap.",
                        }
                    ]
                },
                generated_at=datetime(2026, 8, 17),
            )
        )
        out = render_report_pdf(report, tmp_path)
        assert out.read_bytes()[:5] == b"%PDF-"


class TestRetainOnExport:
    def _report(self, **kw) -> Report:
        fields = dict(
            report_type="opportunity",
            scope_type="windfarm",
            windfarm_id=1,
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            version=1,
            status=ReportStatus.COMPLETE,
            title="t",
            requested_by_id=1,
            locked=False,
        )
        fields.update(kw)
        report = Report(**fields)
        report.sections = []
        return report

    def test_frozen_on_export_or_lock(self):
        assert self._report().is_frozen is False
        assert self._report(pdf_downloaded_at=datetime(2026, 8, 17)).is_frozen is True
        assert self._report(locked=True).is_frozen is True

    def test_pdf_staleness_predicate(self):
        from app.services.reports.orchestrator import pdf_is_stale

        report = self._report()
        assert pdf_is_stale(report) is True  # no artifact at all

        report.pdf_s3_key = "reports/1/v1/x.pdf"
        report.pdf_generated_at = datetime(2026, 8, 17, 12, 0)
        report.sections = [
            ReportSection(
                section_key="findings",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=1,
                generated_at=datetime(2026, 8, 17, 11, 0),
            )
        ]
        assert pdf_is_stale(report) is False  # artifact newer than sections

        report.sections[0].generated_at = datetime(2026, 8, 17, 13, 0)
        assert pdf_is_stale(report) is True  # section regenerated after render

        # FAILED sections never make the artifact stale
        report.sections[0].status = SectionStatus.FAILED
        assert pdf_is_stale(report) is False


class TestFactCheck:
    def test_rounded_and_scaled_values_verify(self):
        from app.services.reports import fact_check as fc

        payload = {"p50_pct": 83.28, "lost_eur": 1_437_000, "capture_ratio": 0.8328}
        assert fc.verify_bullet("P50 attainment was 83.3%", [payload]) == []
        assert fc.verify_bullet("EUR 1.4 million of lost value", [payload]) == []
        assert fc.verify_bullet("capture rate of 83.3% vs the zone", [payload]) == []

    def test_fabricated_number_fails(self):
        from app.services.reports import fact_check as fc

        assert fc.verify_bullet("capture rate was 91.2%", [{"capture": 83.3}]) == [91.2]

    def test_identifiers_and_years_are_not_claims(self):
        from app.services.reports import fact_check as fc

        assert fc.verify_bullet("P50 and MKT_05 in 2024 (EPR-88)", [{"x": 1}]) == []

    def test_numbers_inside_formatted_strings_are_sources(self):
        from app.services.reports import fact_check as fc

        assert (
            fc.verify_bullet(
                "lost value of EUR 45,000", [{"card": {"value": "45,120", "unit": "EUR"}}]
            )
            == []
        )


class TestNarrativeService:
    def test_prompts_load_with_version(self):
        from app.services.reports.narrative_service import _load_prompt

        for key in ("action_plan", "executive_summary"):
            version, body = _load_prompt("opportunity", key)
            assert version == "2"  # bumped for the EPR-88 enrichment
            assert "$windfarm_name" in body

    def test_model_resolution_follows_tier(self):
        from app.core.config import get_settings
        from app.services.reports.narrative_service import _resolve_model

        settings = get_settings()
        spec = get_report_type("opportunity")
        assert _resolve_model(spec.section("action_plan")) == settings.REPORTS_LLM_MODEL_SUMMARY
        assert (
            _resolve_model(spec.section("executive_summary")) == settings.REPORTS_LLM_MODEL_SUMMARY
        )

    def test_summary_payload_only_generated_pass1(self):
        """'No new claims' layer 1: the exec summary sees only generated
        Pass-1 outputs — failed sections and raw data stay out."""
        from app.services.reports.narrative_service import _summary_payload

        report = Report(report_type="opportunity", title="t")
        report.sections = [
            ReportSection(
                section_key="key_metrics",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=1,
                data={"cards": []},
            ),
            ReportSection(
                section_key="findings",
                status=SectionStatus.FAILED,
                pass_number=1,
                display_order=2,
                data={"rows": []},
            ),
            ReportSection(
                section_key="executive_summary",
                status=SectionStatus.GENERATED,
                pass_number=2,
                display_order=0,
                narrative_json={"bullets": []},
            ),
        ]
        payload = _summary_payload(report)
        assert set(payload.keys()) == {"key_metrics"}


class TestDigestPeriods:
    def test_previous_window_full_month(self):
        from app.services.reports.data_builders.digest import previous_window

        assert previous_window(date(2026, 7, 1), date(2026, 7, 31)) == (
            date(2026, 6, 1),
            date(2026, 6, 30),
        )

    def test_previous_window_quarter_and_year_boundary(self):
        from app.services.reports.data_builders.digest import previous_window

        assert previous_window(date(2026, 1, 1), date(2026, 3, 31)) == (
            date(2025, 10, 1),
            date(2025, 12, 31),
        )

    def test_previous_window_arbitrary_dates_shift_by_length(self):
        from app.services.reports.data_builders.digest import previous_window

        start, end = previous_window(date(2026, 7, 10), date(2026, 7, 19))
        assert (end - start).days == 9
        assert end == date(2026, 7, 9)

    def test_yoy_window_full_month_handles_leap_length(self):
        from app.services.reports.data_builders.digest import yoy_window

        # Feb 2025 (28d) -> Feb 2024 must be the FULL leap month, not 28 days.
        assert yoy_window(date(2025, 2, 1), date(2025, 2, 28)) == (
            date(2024, 2, 1),
            date(2024, 2, 29),
        )

    def test_yoy_equals_previous_for_annual(self):
        from app.services.reports.data_builders.digest import previous_window, yoy_window

        window = (date(2025, 1, 1), date(2025, 12, 31))
        assert previous_window(*window) == yoy_window(*window)

    def test_period_labels(self):
        from app.services.reports.data_builders.digest import period_label

        assert period_label(date(2026, 7, 1), date(2026, 7, 31)) == "Jul 2026"
        assert period_label(date(2026, 4, 1), date(2026, 6, 30)) == "Q2 2026"
        assert period_label(date(2025, 1, 1), date(2025, 12, 31)) == "2025"
        assert period_label(date(2026, 7, 10), date(2026, 7, 19)) == "10 Jul 2026 – 19 Jul 2026"


class TestDigestScorecard:
    def test_direction_uses_flat_tolerance(self):
        from app.services.reports.data_builders.digest import _direction

        assert _direction(100.0, 100.3) == "flat"
        assert _direction(110.0, 100.0) == "up"
        assert _direction(90.0, 100.0) == "down"
        assert _direction(None, 100.0) is None
        assert _direction(100.0, None) is None

    def test_scorecard_row_shape_and_deltas(self):
        from app.services.reports.data_builders.digest import _scorecard_row

        metrics = {
            "current": {"generation_gwh": 96.5},
            "previous": {"generation_gwh": 88.1},
            "yoy": {"generation_gwh": None},
        }
        row = _scorecard_row("generation", "Generation", "GWh", metrics, "generation_gwh")
        assert row["values"] == {"current": "96.5", "previous": "88.1", "yoy": "n/a"}
        assert row["direction"] == {"previous": "up"}
        assert row["delta_pct"]["previous"] == 9.5
        assert "yoy" not in row["delta_pct"]

    def test_registry_digest_spec(self):
        spec = get_report_type("digest")
        assert spec is not None
        assert spec.scope == "windfarm"
        assert {s.key for s in spec.sections} == {
            "executive_summary",
            "scorecard",
            "finding_changes",
            "wind_resource",
            "generation_chart",
        }
        # Every Pass-1 digest section MUST have a data builder — a builderless
        # section stays UNGENERATED and pins the whole report at PARTIAL.
        for section in spec.sections:
            if section.pass_number == 1:
                assert section.data_builder is not None, section.key
        assert spec.section("executive_summary").narrative.tier == "summary"
        assert spec.display_ordered()[0].key == "executive_summary"


class TestDigestPdf:
    def test_digest_pdf_renders(self, tmp_path):
        from app.services.reports.pdf.renderers.digest import render

        windfarm = Windfarm(id=1, name="Testfarm & Co <AS>", code="TEST")
        report = Report(
            id=1,
            report_type="digest",
            scope_type="windfarm",
            windfarm_id=1,
            period_start=date(2026, 4, 1),
            period_end=date(2026, 6, 30),
            version=1,
            status=ReportStatus.COMPLETE,
            title="Periodic Digest — Testfarm",
            requested_by_id=1,
        )
        report.windfarm = windfarm
        scorecard_data = {
            "columns": [
                {"key": "current", "label": "Q2 2026"},
                {"key": "previous", "label": "Q1 2026"},
            ],
            "rows": [
                {
                    "key": "generation",
                    "label": "Generation",
                    "unit": "GWh",
                    "values": {"current": "96.5", "previous": "88.1"},
                    "raw": {"current": 96.5, "previous": 88.1},
                    "direction": {"previous": "up"},
                    "delta_pct": {"previous": 9.5},
                }
            ],
            "notes": ["A note with markup-hostile chars: <5% & rising"],
        }
        report.sections = [
            ReportSection(
                id=1,
                report_id=1,
                section_key="executive_summary",
                pass_number=2,
                display_order=0,
                layout="full",
                status=SectionStatus.GENERATED,
                narrative_json={
                    "overall_assessment": "Generation rose 9.5% on stronger wind.",
                    "bullets": [
                        {"text": "Generation 88.1 → 96.5 GWh.", "source_sections": ["scorecard"]}
                    ],
                },
            ),
            ReportSection(
                id=2,
                report_id=1,
                section_key="scorecard",
                pass_number=1,
                display_order=1,
                layout="full",
                status=SectionStatus.GENERATED,
                data=scorecard_data,
            ),
            ReportSection(
                id=3,
                report_id=1,
                section_key="finding_changes",
                pass_number=1,
                display_order=2,
                layout="two_col_left",
                status=SectionStatus.GENERATED,
                data={
                    "columns": [{"key": "current", "label": "As of 30 Jun 2026"}],
                    "rows": [
                        {
                            "key": "confirmed",
                            "label": "Confirmed",
                            "unit": None,
                            "values": {"current": "3"},
                            "raw": {"current": 3},
                            "direction": {},
                            "delta_pct": {},
                        }
                    ],
                    "notes": ["No detection history before 01 Apr 2026."],
                },
            ),
            ReportSection(
                id=4,
                report_id=1,
                section_key="wind_resource",
                pass_number=1,
                display_order=3,
                layout="two_col_right",
                status=SectionStatus.GENERATED,
                data={
                    "cards": [
                        {"label": "P50 attainment", "value": "96.2", "unit": "%", "raw": 96.2}
                    ],
                    "note": "Expected generation is the P50 model output.",
                },
            ),
            ReportSection(
                id=5,
                report_id=1,
                section_key="generation_chart",
                pass_number=1,
                display_order=4,
                layout="full",
                status=SectionStatus.GENERATED,
                data={
                    "chart_key": "windfarm_generation",
                    "series": {
                        "unit": "GWh",
                        "current": {
                            "label": "Q2 2026",
                            "points": [
                                {"label": "Apr 2026", "gwh": 30.1},
                                {"label": "May 2026", "gwh": 33.2},
                                {"label": "Jun 2026", "gwh": 33.2},
                            ],
                        },
                        "previous": {
                            "label": "Q1 2026",
                            "points": [
                                {"label": "Jan 2026", "gwh": 29.0},
                                {"label": "Feb 2026", "gwh": 28.4},
                                {"label": "Mar 2026", "gwh": 30.7},
                            ],
                        },
                    },
                },
            ),
        ]
        out = render(report, tmp_path)
        assert out.exists()
        assert out.stat().st_size > 5000


class TestFactCheckSigns:
    def test_signed_delta_backs_unsigned_claim(self):
        from app.services.reports.fact_check import verify_bullet

        payload = {"rows": [{"key": "capture_rate", "delta_pct": {"previous": -2.7}}]}
        assert verify_bullet("Capture rate slipped 2.7% vs Q3.", [payload]) == []
        assert verify_bullet("Capture rate moved -2.7% vs Q3.", [payload]) == []

    def test_fabricated_number_still_fails(self):
        from app.services.reports.fact_check import verify_bullet

        payload = {"rows": [{"key": "capture_rate", "delta_pct": {"previous": -2.7}}]}
        assert verify_bullet("Capture rate slipped 8.9% vs Q3.", [payload]) == [8.9]


class TestEvidenceFormatter:
    def test_ops04_degradation_slots(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        out = format_evidence(
            SchemaCode.OPS_04,
            {
                "slope_pct_per_year": -2.31,
                "p_value": 0.032,
                "r_squared": 0.61,
                "ci_lower_95_pct": -3.4,
                "ci_upper_95_pct": -1.2,
                "years_of_data": 4.2,
                "n_constraint_hours_excluded": 812,
                "period": "2021-01..2025-12",
                "baseline_caveat": True,
            },
        )
        by_label = {i["label"]: i["value"] for i in out["items"]}
        assert by_label["Degradation slope"] == "-2.31%/yr"
        assert by_label["p-value"] == "0.032"
        assert by_label["95% CI"] == "-3.4 to -1.2%/yr"
        assert by_label["Constraint hours excluded"] == "812"
        # period is excluded; the caveat becomes a note, not a grid item.
        assert "period" not in {i["label"].lower() for i in out["items"]}
        assert any("baseline" in n.lower() for n in out["notes"])

    def test_mkt01_ratios_render_as_percent(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        out = format_evidence(
            SchemaCode.MKT_01,
            {"capture_rate": 0.831, "zone_avg_capture": 0.902, "gap_pp": 7.1, "price_zone": "NO3"},
        )
        by_label = {i["label"]: i["value"] for i in out["items"]}
        assert by_label["Capture rate"] == "83.1%"
        assert by_label["Zone average"] == "90.2%"
        assert by_label["Gap vs zone"] == "7.1 pp"
        assert by_label["Price zone"] == "NO3"

    def test_month_list_caps_and_counts_extra(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        months = [f"2025-{m:02d}" for m in range(1, 10)]
        out = format_evidence(SchemaCode.OPS_01, {"disruption_month_list": months})
        (value,) = [i["value"] for i in out["items"] if i["label"] == "Disrupted months"]
        assert value.endswith("+3 more")

    def test_daterange_and_missing_slots_skipped(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        out = format_evidence(
            SchemaCode.DQ_01,
            {"max_gap_hours": 96, "largest_gap_start": "2025-03-01T00:00:00"},
        )
        labels = [i["label"] for i in out["items"]]
        assert "Largest gap" in labels
        assert "Largest gap window" not in labels  # end missing -> pair skipped
        assert "Gaps in period" not in labels  # absent slot skipped

    def test_reclassified_and_downgrade_notes(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        out = format_evidence(
            SchemaCode.MKT_03,
            {"cannibalisation_index": 1.21, "reclassified_from": ["MKT_01"]},
        )
        assert any("MKT-01" in n for n in out["notes"])

    def test_empty_slots_yield_empty_items(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        out = format_evidence(SchemaCode.FIN_01, {})
        assert out == {"items": [], "notes": []}
        assert format_evidence(SchemaCode.FIN_01, None)["items"] == []


class TestKeyMetricsCards:
    def test_card_with_delta_fields(self):
        from app.services.reports.data_builders.opportunity import _card

        card = _card("Generation", 96.5, "GWh", ",.1f", previous=88.0, good_direction="up")
        assert card["value"] == "96.5"
        assert card["unit"] == "GWh"
        assert card["raw"] == 96.5
        assert card["direction"] == "up"
        assert card["good_direction"] == "up"
        assert card["delta_pct"] == 9.7

    def test_card_without_good_direction_stays_legacy_shape(self):
        from app.services.reports.data_builders.opportunity import _card

        card = _card("Anything", 12.0, "%", ".1f")
        assert set(card) == {"label", "value", "unit", "raw"}

    def test_card_none_value_degrades(self):
        from app.services.reports.data_builders.opportunity import _card

        card = _card("Capture", None, "%", ".1f", previous=90.0, good_direction="up")
        assert card["value"] == "n/a"
        assert card["unit"] is None
        assert card["delta_pct"] is None
        assert card["direction"] is None


class TestEnrichedOpportunityPdf:
    def test_render_with_evidence_deltas_and_charts(self, tmp_path):
        from app.services.reports.pdf import render_report_pdf

        report = Report(
            report_type="opportunity",
            scope_type="windfarm",
            windfarm_id=1,
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            version=2,
            status=ReportStatus.COMPLETE,
            title="Opportunity Report — Testfarm",
            requested_by_id=1,
        )
        report.windfarm = Windfarm(name="Testfarm", code="TESTF")
        month_points = [{"label": f"{m:02d} 2025", "gwh": 3.0 + m * 0.1} for m in range(1, 13)]
        report.sections = [
            ReportSection(
                section_key="key_metrics",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=1,
                data={
                    "cards": [
                        {
                            "label": "P50 attainment",
                            "value": "92.1",
                            "unit": "%",
                            "raw": 92.1,
                            "delta_pct": 4.2,
                            "direction": "up",
                            "good_direction": "up",
                        },
                        {"label": "Generation", "value": "40.7", "unit": "GWh", "raw": 40.7},
                        {"label": "Capacity factor", "value": "35.4", "unit": "%", "raw": 35.4},
                        {"label": "Lost value (period)", "value": "47,959", "unit": "EUR"},
                        {"label": "Capture rate vs zone", "value": "83.3", "unit": "%"},
                        {"label": "Schemas flagged", "value": "7"},
                    ],
                    "previous_label": "2024",
                },
                generated_at=datetime(2026, 8, 17),
            ),
            ReportSection(
                section_key="generation_chart",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=2,
                data={
                    "chart_key": "windfarm_generation",
                    "series": {
                        "unit": "GWh",
                        "current": {"label": "2025", "points": month_points},
                        "previous": {"label": "2024", "points": month_points},
                    },
                },
                generated_at=datetime(2026, 8, 17),
            ),
            ReportSection(
                section_key="findings",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=3,
                data={
                    "rows": [
                        {
                            "schema_code": "FIN-01",
                            "domain": "Financial",
                            "display_name": "P50 Generation Attainment",
                            "one_liner": "Actual generation below the P50 target.",
                            "key_metric": "Attainment %: 89",
                            "severity": "confirmed",
                            "evidence": [
                                {"label": "P50 attainment", "value": "89.3%"},
                                {"label": "Actual generation", "value": "40.7 GWh"},
                                {"label": "P50 target", "value": "45.6 GWh"},
                            ],
                            "notes": ["Provisional pending review."],
                            "detection_period": {"start": "2024-06-26", "end": "2026-06-26"},
                        },
                        {
                            "schema_code": "OPS-02",
                            "domain": "Operational",
                            "display_name": "Performance Seasonality",
                            "severity": "pass",
                            "evidence": None,
                            "notes": [],
                        },
                    ],
                    "severity_counts": {"confirmed": 1, "pass": 1},
                    "assessed_schemas": 2,
                },
                generated_at=datetime(2026, 8, 17),
            ),
            ReportSection(
                section_key="wind_norm_chart",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=4,
                data={
                    "chart_key": "wind_norm_monthly",
                    "series": {
                        "unit": "index",
                        "baseline": 100,
                        "points": [
                            {"label": f"{m:02d} 2025", "index": 95 + m} for m in range(1, 13)
                        ],
                    },
                },
                generated_at=datetime(2026, 8, 17),
            ),
            ReportSection(
                section_key="capture_rate_chart",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=5,
                data={
                    "chart_key": "capture_rate_monthly",
                    "series": {
                        "unit": "%",
                        "points": [
                            {"label": f"{m:02d} 2025", "capture_rate_pct": 80.0 + m}
                            for m in range(1, 13)
                        ],
                        "overall_capture_rate_pct": 86.2,
                        "currency": "EUR",
                    },
                },
                generated_at=datetime(2026, 8, 17),
            ),
        ]

        out = render_report_pdf(report, tmp_path)
        assert out.exists()
        content = out.read_bytes()
        assert content[:5] == b"%PDF-"
        assert len(content) > 30_000  # three charts + tables embedded

    def test_empty_chart_series_skipped(self, tmp_path):
        """Chart sections with no points must not break the render."""
        from app.services.reports.pdf import render_report_pdf

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
                section_key="wind_norm_chart",
                status=SectionStatus.GENERATED,
                pass_number=1,
                display_order=1,
                data={"chart_key": "wind_norm_monthly", "series": {"points": []}},
                generated_at=datetime(2026, 8, 17),
            ),
        ]
        out = render_report_pdf(report, tmp_path)
        assert out.read_bytes()[:5] == b"%PDF-"


# ── EPR-110 / EPR-111 / EPR-112 ─────────────────────────────────────────


class _FakeResult:
    """Canned result for a single ``session.execute`` call."""

    def __init__(self, value=None, rows=None):
        self._value = value
        self._rows = rows or []

    def scalar_one_or_none(self):
        return self._value

    def scalar_one(self):
        return self._value

    def all(self):
        return self._rows

    def scalars(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeSession:
    """Records the statements it is handed and replays canned results."""

    def __init__(self, *results):
        self.results = list(results)
        self.statements = []

    async def execute(self, statement, *args, **kwargs):
        self.statements.append(statement)
        return self.results.pop(0) if self.results else _FakeResult()

    def sql(self, index: int = 0) -> str:
        return str(self.statements[index])


class TestReportOwnershipScoping:
    """EPR-112 — reports are private to the user who generated them."""

    def _report(self, requested_by_id: int) -> Report:
        report = Report(
            report_type="opportunity",
            scope_type="windfarm",
            windfarm_id=1,
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            version=1,
            status=ReportStatus.COMPLETE,
            title="t",
            requested_by_id=requested_by_id,
        )
        report.windfarm = None
        report.portfolio = None
        return report

    def _user(self, user_id: int, is_superuser: bool = False):
        from app.models.user import User

        return User(id=user_id, email="u@x.com", username="u", is_superuser=is_superuser)

    async def test_owner_can_read_own_report(self):
        from app.services.reports.service import ReportService

        session = _FakeSession(_FakeResult(value=self._report(requested_by_id=7)))
        report = await ReportService(session).get_report(1, self._user(7))
        assert report.requested_by_id == 7

    async def test_other_user_gets_not_found(self):
        from app.core.exceptions import NotFoundException
        from app.services.reports.service import ReportService

        session = _FakeSession(_FakeResult(value=self._report(requested_by_id=7)))
        with pytest.raises(NotFoundException):
            await ReportService(session).get_report(1, self._user(8))

    async def test_superuser_gets_no_bypass(self):
        """Every internal account is a superuser, so an exemption would leave
        the library shared in practice — which is the bug EPR-112 reports."""
        from app.core.exceptions import NotFoundException
        from app.services.reports.service import ReportService

        session = _FakeSession(_FakeResult(value=self._report(requested_by_id=7)))
        with pytest.raises(NotFoundException):
            await ReportService(session).get_report(1, self._user(8, is_superuser=True))

    async def test_list_filters_by_requester_for_superusers_too(self):
        from app.services.reports.service import ReportService

        session = _FakeSession(_FakeResult(value=0), _FakeResult(rows=[]))
        await ReportService(session).list_reports(self._user(8, is_superuser=True))
        assert "reports.requested_by_id = " in session.sql(0)

    async def test_version_chain_is_owner_scoped(self):
        """The chain drives soft_delete — unscoped, one user's delete stamps
        deleted_at on another user's report for the same farm/type/period."""
        from app.services.reports.service import ReportService

        session = _FakeSession(_FakeResult(rows=[]))
        await ReportService(session)._version_chain(self._report(requested_by_id=7))
        assert "reports.requested_by_id = " in session.sql(0)


class TestBidzoneDisplayNames:
    """EPR-110 — bidzones.code is the raw EIC; bidzones.name is readable."""

    def _report_with_zone(self, code: str, name: str) -> Report:
        from app.models.bidzone import Bidzone
        from app.models.country import Country

        report = Report(
            report_type="opportunity",
            scope_type="windfarm",
            windfarm_id=1,
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            title="t",
            requested_by_id=1,
        )
        report.portfolio = None
        report.windfarm = Windfarm(name="Tellenes", code="TELLENES", nameplate_capacity_mw=168)
        report.windfarm.bidzone = Bidzone(code=code, name=name)
        report.windfarm.country = Country(name="Norway", code="NOR")
        return report

    def test_scope_meta_emits_the_name_not_the_eic(self):
        from app.services.reports.service import ReportService

        meta = ReportService.scope_meta(self._report_with_zone("10YNO-2--------T", "NO2"))
        assert meta.bidzone == "NO2"
        assert meta.capacity_mw == 168
        assert meta.country == "Norway"

    def test_evidence_resolves_zone_code_to_name(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        out = format_evidence(
            SchemaCode.MKT_03,
            {"cannibalisation_index": 1.11, "price_zone": "10YNO-2--------T"},
            zone_names={"10YNO-2--------T": "NO2"},
        )
        by_label = {i["label"]: i["value"] for i in out["items"]}
        assert by_label["Price zone"] == "NO2"

    def test_unmapped_zone_code_passes_through(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        out = format_evidence(
            SchemaCode.MKT_01, {"price_zone": "50Y0JVU59B4JWQCU"}, zone_names={"X": "Y"}
        )
        assert {i["value"] for i in out["items"]} == {"50Y0JVU59B4JWQCU"}

    def test_no_map_keeps_previous_behaviour(self):
        from app.models.opportunity import SchemaCode
        from app.services.opportunity_schemas.evidence import format_evidence

        out = format_evidence(SchemaCode.MKT_02, {"price_zone": "NO3"})
        assert {i["value"] for i in out["items"]} == {"NO3"}


class TestCoverageAwareMetrics:
    """EPR-111 — a part-covered window must not read as a collapse."""

    def test_capacity_factor_uses_covered_hours(self):
        from app.services.reports.data_builders.common import capacity_factor_pct

        # Tellenes: 221.8 GWh over 3,264 covered hours of a 8,784-hour window.
        assert round(capacity_factor_pct(221_800, 168, 3264), 1) == 40.4
        # The pre-fix denominator is what produced the reported 15.0%.
        assert round(capacity_factor_pct(221_800, 168, 8784), 1) == 15.0

    def test_capacity_factor_degrades_on_no_coverage(self):
        from app.services.reports.data_builders.common import capacity_factor_pct

        assert capacity_factor_pct(221_800, 168, 0) is None
        assert capacity_factor_pct(None, 168, 3264) is None
        assert capacity_factor_pct(221_800, None, 3264) is None
        # A genuine zero-generation period is 0%, not "unknown".
        assert capacity_factor_pct(0.0, 168, 3264) == 0.0

    def test_coverage_note_only_for_material_gaps(self):
        from app.services.reports.data_builders.common import coverage_note

        note = coverage_note(
            date(2026, 8, 18), date(2025, 12, 31), date(2025, 8, 18), date(2025, 12, 31)
        )
        assert "31 Dec 2025" in note
        # A window short by ordinary import lag stays quiet.
        assert (
            coverage_note(
                date(2026, 8, 18), date(2026, 8, 16), date(2025, 8, 18), date(2026, 8, 16)
            )
            is None
        )
        assert coverage_note(date(2026, 8, 18), None, date(2025, 8, 18), date(2026, 8, 18)) is None

    async def test_effective_window_clips_to_last_hour_with_data(self):
        from app.services.reports.context import ReportContext
        from app.services.reports.data_builders.common import effective_window

        session = _FakeSession(_FakeResult(value=datetime(2025, 12, 31, 23, 0)))
        ctx = ReportContext(
            db=session,
            report_id=1,
            scope_type="windfarm",
            period_start=date(2025, 8, 18),
            period_end=date(2026, 8, 18),
            windfarm=Windfarm(id=7220, name="Tellenes", code="TELLENES"),
        )
        start, end, data_through = await effective_window(ctx, date(2025, 8, 18), date(2026, 8, 18))
        assert (start, end, data_through) == (
            date(2025, 8, 18),
            date(2025, 12, 31),
            date(2025, 12, 31),
        )

    async def test_effective_window_untouched_when_fully_covered(self):
        from app.services.reports.context import ReportContext
        from app.services.reports.data_builders.common import effective_window

        ctx_kwargs = dict(
            report_id=1,
            scope_type="windfarm",
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            windfarm=Windfarm(id=1, name="F", code="F"),
        )
        covered = ReportContext(
            db=_FakeSession(_FakeResult(value=datetime(2025, 12, 31, 23, 0))), **ctx_kwargs
        )
        assert await effective_window(covered, date(2025, 1, 1), date(2025, 12, 31)) == (
            date(2025, 1, 1),
            date(2025, 12, 31),
            None,
        )
        # No data at all — leave the window alone and let the caller's
        # empty-comparison guard render n/a.
        empty = ReportContext(db=_FakeSession(_FakeResult(value=None)), **ctx_kwargs)
        assert await effective_window(empty, date(2025, 1, 1), date(2025, 12, 31)) == (
            date(2025, 1, 1),
            date(2025, 12, 31),
            None,
        )

    def test_clipped_window_compares_against_the_same_season(self):
        """A clipped window compares year-over-year, not against the span that
        happens to precede it — otherwise the coverage artefact is merely
        traded for a seasonal one (a Norwegian autumn against its spring)."""
        from app.services.reports.data_builders.common import previous_window, yoy_window

        # Unclipped full year: the two are the same window, so nothing moves.
        assert previous_window(date(2025, 8, 18), date(2026, 8, 17)) == yoy_window(
            date(2025, 8, 18), date(2026, 8, 17)
        )
        # Clipped to the covered 136 days, they diverge — the preceding span
        # lands in spring, the year-earlier span keeps the same months.
        clipped = (date(2025, 8, 18), date(2025, 12, 31))
        assert previous_window(*clipped) == (date(2025, 4, 4), date(2025, 8, 17))
        assert yoy_window(*clipped) == (date(2024, 8, 18), date(2024, 12, 31))
        # Same length either way — the comparison stays like-for-like.
        yoy = yoy_window(*clipped)
        assert (yoy[1] - yoy[0]).days == (clipped[1] - clipped[0]).days
