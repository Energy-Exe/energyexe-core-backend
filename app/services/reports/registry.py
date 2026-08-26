"""Report type registry — the declarative heart of the reports platform.

Modeled on ``app/services/opportunity_schemas/registry.py``: frozen specs of
pure(ish) functions, consumed by the orchestrator, the API layer, and the PDF
renderers. Adding report type #4 means adding one ``ReportTypeSpec`` here (plus
prompts and any new data builders) — no orchestrator or endpoint changes.

Section ``kind`` values are the frontend renderer registry keys (client-ui
``report-kit``): metric_strip | findings_table | action_plan | narrative |
scorecard | chart_embed. Keep the two in lock-step.
"""

from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Optional

from app.services.reports.context import ReportContext

# A data builder computes one section's deterministic slice. It must not
# commit the session; the orchestrator owns transactions.
DataBuilder = Callable[[ReportContext], Awaitable[dict]]


@dataclass(frozen=True)
class NarrativeSpec:
    """AI narrative config for a section (consumed by the narrative service)."""

    prompt_key: str  # app/prompts/reports/{report_type}/{prompt_key}.txt
    max_tokens: int = 1500
    # Which settings default supplies the model: 'section' ->
    # REPORTS_LLM_MODEL_SECTION, 'summary' -> REPORTS_LLM_MODEL_SUMMARY.
    tier: str = "section"
    # An explicit model id overrides the tier default.
    model: Optional[str] = None


@dataclass(frozen=True)
class SectionSpec:
    key: str
    title: str
    kind: str
    pass_number: int = 1  # 2 = synthesises across all Pass-1 outputs
    layout: str = "full"  # full | two_col_left | two_col_right
    data_builder: Optional[DataBuilder] = None
    narrative: Optional[NarrativeSpec] = None
    # Executive summary: generated last (pass 2) but rendered first.
    render_first: bool = False
    # Pass-1 data sections that read another data section's persisted payload
    # (EPR-117: key_metrics takes "Schemas flagged" from findings' counts). The
    # orchestrator runs sections with an ``after`` only once the independent
    # sections have landed.
    after: tuple = ()

    @property
    def ai_enabled(self) -> bool:
        return self.narrative is not None


@dataclass(frozen=True)
class ReportTypeSpec:
    code: str
    title: str
    scope: str  # 'windfarm' | 'portfolio' | 'both'
    sections: tuple = field(default_factory=tuple)

    def section(self, key: str) -> Optional[SectionSpec]:
        for s in self.sections:
            if s.key == key:
                return s
        return None

    def display_ordered(self) -> list:
        """Sections in render order: render_first sections lead."""
        return sorted(
            self.sections, key=lambda s: (0 if s.render_first else 1, self.sections.index(s))
        )


def _build_registry() -> Dict[str, ReportTypeSpec]:
    # Imported here to avoid a registry -> builders -> registry cycle.
    from app.services.reports.data_builders import digest as dig
    from app.services.reports.data_builders import opportunity as opp

    opportunity_report = ReportTypeSpec(
        code="opportunity",
        title="Opportunity Report",
        scope="windfarm",
        sections=(
            SectionSpec(
                key="executive_summary",
                title="Executive Summary",
                kind="narrative",
                pass_number=2,
                render_first=True,
                narrative=NarrativeSpec(
                    prompt_key="executive_summary", max_tokens=1200, tier="summary"
                ),
            ),
            SectionSpec(
                key="key_metrics",
                title="Key Metrics",
                kind="metric_strip",
                data_builder=opp.build_key_metrics,
                after=("findings",),
            ),
            SectionSpec(
                key="generation_chart",
                title="Generation",
                kind="chart_embed",
                data_builder=opp.build_generation_chart,
            ),
            SectionSpec(
                key="findings",
                title="Performance Snapshot",
                kind="findings_table",
                data_builder=opp.build_findings,
            ),
            SectionSpec(
                key="wind_norm_chart",
                title="Wind-Normalised Performance",
                kind="chart_embed",
                layout="two_col_left",
                data_builder=opp.build_wind_norm_chart,
            ),
            SectionSpec(
                key="capture_rate_chart",
                title="Capture Rate Trend",
                kind="chart_embed",
                layout="two_col_right",
                data_builder=opp.build_capture_rate_chart,
            ),
            SectionSpec(
                key="action_plan",
                title="Action Plan",
                kind="action_plan",
                narrative=NarrativeSpec(prompt_key="action_plan", max_tokens=2000, tier="summary"),
            ),
        ),
    )

    digest_report = ReportTypeSpec(
        code="digest",
        title="Periodic Digest",
        scope="windfarm",
        sections=(
            SectionSpec(
                key="executive_summary",
                title="Executive Summary",
                kind="narrative",
                pass_number=2,
                render_first=True,
                narrative=NarrativeSpec(
                    prompt_key="executive_summary", max_tokens=1200, tier="summary"
                ),
            ),
            SectionSpec(
                key="scorecard",
                title="Period Scorecard",
                kind="scorecard",
                data_builder=dig.build_scorecard,
            ),
            SectionSpec(
                key="finding_changes",
                title="Finding Changes",
                kind="scorecard",
                layout="two_col_left",
                data_builder=dig.build_finding_changes,
            ),
            SectionSpec(
                key="wind_resource",
                title="Wind Resource",
                kind="metric_strip",
                layout="two_col_right",
                data_builder=dig.build_wind_resource,
            ),
            SectionSpec(
                key="generation_chart",
                title="Generation",
                kind="chart_embed",
                data_builder=dig.build_generation_chart,
            ),
        ),
    )

    return {
        opportunity_report.code: opportunity_report,
        digest_report.code: digest_report,
    }


REPORT_TYPE_REGISTRY: Dict[str, ReportTypeSpec] = _build_registry()


def get_report_type(code: str) -> Optional[ReportTypeSpec]:
    return REPORT_TYPE_REGISTRY.get(code)
