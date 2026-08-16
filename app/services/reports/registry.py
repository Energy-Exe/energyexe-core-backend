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
            ),
            SectionSpec(
                key="findings",
                title="Performance Snapshot",
                kind="findings_table",
                layout="two_col_left",
                data_builder=opp.build_findings,
            ),
            SectionSpec(
                key="action_plan",
                title="Action Plan",
                kind="action_plan",
                layout="two_col_right",
                narrative=NarrativeSpec(prompt_key="action_plan", max_tokens=2000, tier="summary"),
            ),
        ),
    )

    return {opportunity_report.code: opportunity_report}


REPORT_TYPE_REGISTRY: Dict[str, ReportTypeSpec] = _build_registry()


def get_report_type(code: str) -> Optional[ReportTypeSpec]:
    return REPORT_TYPE_REGISTRY.get(code)
