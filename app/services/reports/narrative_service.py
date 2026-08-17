"""AI narrative generation for report sections (EPR-82/88 Phase 2).

Single-shot Anthropic Messages calls over a prepared data slice — NOT the
brain-agent loop. Structured output is enforced by forcing a tool call whose
``input_schema`` is the section's output schema (the pinned ``anthropic==0.72.1``
predates GA structured outputs; tool-forcing gives the same guarantee), then
validating with Pydantic and — for the Pass-2 executive summary — running the
numeric fact-check so the summary can never introduce claims its cited
sections don't contain.
"""

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import List, Literal, Optional, Tuple

import structlog
from pydantic import BaseModel, Field, ValidationError

from app.core.config import get_settings
from app.models.report import Report, ReportSection, SectionStatus
from app.services.reports import fact_check
from app.services.reports.registry import ReportTypeSpec, SectionSpec

logger = structlog.get_logger()

_PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts" / "reports"

# USD per million tokens (input, output), matched by model-id prefix. Update
# alongside model changes; per-section actuals are persisted either way.
_PRICING: dict = {
    "claude-opus-5": (5.00, 25.00),
    "claude-sonnet-5": (3.00, 15.00),
    "claude-haiku-4-5": (1.00, 5.00),
}
_DEFAULT_PRICING: Tuple[float, float] = (5.00, 25.00)

_REQUEST_TIMEOUT_S = 120.0
_MAX_ATTEMPTS = 2  # one retry on schema-invalid output


class NarrativeError(Exception):
    """Generation failed for a reason worth surfacing on the section row."""


# ── output schemas ──────────────────────────────────────────────────────
# The action plan shape is the client contract — keep in lock-step with
# report-kit/sections/action-plan-section.tsx (ActionPlanData).


class ActionItem(BaseModel):
    title: str
    horizon: Optional[str] = None
    external: Optional[str] = None
    linked_schemas: List[str] = Field(default_factory=list)


class ActionTier(BaseModel):
    tier: Literal["P1", "P2", "P3"]
    label: str
    actions: List[ActionItem]
    context: Optional[str] = None


class ActionPlanOutput(BaseModel):
    tiers: List[ActionTier]


class SummaryBullet(BaseModel):
    text: str
    source_sections: List[str]


class ExecutiveSummaryOutput(BaseModel):
    overall_assessment: str
    bullets: List[SummaryBullet]


@dataclass
class NarrativeResult:
    narrative_json: dict
    narrative_text: Optional[str]
    data: Optional[dict]  # structured mirror for kinds the UI renders from `data`
    model: str
    prompt_version: str
    input_tokens: int
    output_tokens: int
    cost_usd: float


# ── prompt + model plumbing ─────────────────────────────────────────────


@lru_cache(maxsize=32)
def _load_prompt(report_type: str, prompt_key: str) -> Tuple[str, str]:
    """Return (version, template_text) from the versioned prompt file."""
    path = _PROMPTS_DIR / report_type / f"{prompt_key}.txt"
    raw = path.read_text(encoding="utf-8")
    version = "1"
    body = raw
    if "---" in raw:
        header, _, body = raw.partition("---")
        for line in header.splitlines():
            if line.strip().lower().startswith("version:"):
                version = line.split(":", 1)[1].strip()
    return version, body.strip()


def _resolve_model(section_spec: SectionSpec) -> str:
    settings = get_settings()
    narrative = section_spec.narrative
    if narrative.model:
        return narrative.model
    if narrative.tier == "summary":
        return settings.REPORTS_LLM_MODEL_SUMMARY
    return settings.REPORTS_LLM_MODEL_SECTION


def _cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    pricing = _DEFAULT_PRICING
    for prefix, rates in _PRICING.items():
        if model.startswith(prefix):
            pricing = rates
            break
    return (input_tokens / 1_000_000) * pricing[0] + (output_tokens / 1_000_000) * pricing[1]


def _client():
    settings = get_settings()
    if not settings.ANTHROPIC_API_KEY:
        raise NarrativeError("AI narratives disabled: ANTHROPIC_API_KEY is not configured")
    import anthropic

    return anthropic.AsyncAnthropic(
        api_key=settings.ANTHROPIC_API_KEY, timeout=_REQUEST_TIMEOUT_S, max_retries=2
    )


async def _call_structured(
    model: str,
    system: str,
    user_content: str,
    tool_name: str,
    tool_description: str,
    input_schema: dict,
    max_tokens: int,
) -> Tuple[dict, int, int]:
    """Force one tool call and return (payload, input_tokens, output_tokens)."""
    client = _client()
    response = await client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_content}],
        tools=[{"name": tool_name, "description": tool_description, "input_schema": input_schema}],
        tool_choice={"type": "tool", "name": tool_name},
    )
    if response.stop_reason == "max_tokens":
        raise NarrativeError(f"Narrative truncated at {max_tokens} tokens")
    block = next((b for b in response.content if b.type == "tool_use"), None)
    if block is None:
        raise NarrativeError("Model returned no structured output")
    return dict(block.input), response.usage.input_tokens, response.usage.output_tokens


# ── section input assembly ──────────────────────────────────────────────


def _generated_pass1(report: Report) -> List[ReportSection]:
    return [
        s
        for s in sorted(report.sections, key=lambda s: s.display_order)
        if s.pass_number == 1 and s.status in (SectionStatus.GENERATED, SectionStatus.STALE)
    ]


def _period_label(report: Report) -> str:
    return f"{report.period_start:%d %b %Y} – {report.period_end:%d %b %Y}"


def _scope_name(report: Report) -> str:
    return report.windfarm.name if report.windfarm is not None else report.title


def _render_system_prompt(report: Report, spec: ReportTypeSpec, section_spec: SectionSpec) -> str:
    _, template = _load_prompt(spec.code, section_spec.narrative.prompt_key)
    return Template(template).safe_substitute(
        windfarm_name=_scope_name(report), period_label=_period_label(report)
    )


def _data_payload(report: Report) -> dict:
    """Pass-1 data-section outputs, keyed by section — the action plan input."""
    return {s.section_key: s.data for s in _generated_pass1(report) if s.data is not None}


def _summary_payload(report: Report) -> dict:
    """All generated Pass-1 outputs (data + narratives) — the ONLY thing the
    executive summary is allowed to see ("no new claims" layer 1)."""
    payload = {}
    for s in _generated_pass1(report):
        entry: dict = {}
        if s.data is not None:
            entry["data"] = s.data
        if s.narrative_json is not None:
            entry["narrative"] = s.narrative_json
        if entry:
            payload[s.section_key] = entry
    return payload


def _known_schema_codes(report: Report) -> set:
    findings = next((s for s in report.sections if s.section_key == "findings"), None)
    if findings is None or not findings.data:
        return set()
    return {r.get("schema_code") for r in findings.data.get("rows", []) if r.get("schema_code")}


# ── generation entry points ─────────────────────────────────────────────


async def generate(
    report: Report, spec: ReportTypeSpec, section_spec: SectionSpec
) -> NarrativeResult:
    """Generate one section's narrative. ``report`` must have sections and
    windfarm loaded. Raises :class:`NarrativeError` on any failure."""
    if section_spec.pass_number == 2:
        return await _generate_summary(report, spec, section_spec)
    if section_spec.kind == "action_plan":
        return await _generate_action_plan(report, spec, section_spec)
    raise NarrativeError(f"No narrative generator for section kind {section_spec.kind}")


async def _generate_action_plan(
    report: Report, spec: ReportTypeSpec, section_spec: SectionSpec
) -> NarrativeResult:
    payload = _data_payload(report)
    if not payload:
        raise NarrativeError("No generated data sections to plan from")

    model = _resolve_model(section_spec)
    version, _ = _load_prompt(spec.code, section_spec.narrative.prompt_key)
    system = _render_system_prompt(report, spec, section_spec)
    user = "Report data:\n```json\n" + json.dumps(payload, default=str) + "\n```"
    schema = ActionPlanOutput.model_json_schema()

    plan, input_tokens, output_tokens = await _call_with_validation(
        ActionPlanOutput,
        model=model,
        system=system,
        user_content=user,
        tool_name="emit_action_plan",
        tool_description="Record the three-tier action plan for this report.",
        input_schema=schema,
        max_tokens=section_spec.narrative.max_tokens,
    )

    # Linked schemas must exist in the findings — silently drop hallucinated codes.
    known = _known_schema_codes(report)
    for tier in plan.tiers:
        for action in tier.actions:
            action.linked_schemas = [c for c in action.linked_schemas if c in known]

    narrative_json = plan.model_dump()
    return NarrativeResult(
        narrative_json=narrative_json,
        # No narrative_text: the client renders this kind from `data`, and the
        # section shell would otherwise render the text a second time below it.
        narrative_text=None,
        data=narrative_json,  # the client renders `action_plan` from section.data
        model=model,
        prompt_version=version,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=_cost_usd(model, input_tokens, output_tokens),
    )


async def _generate_summary(
    report: Report, spec: ReportTypeSpec, section_spec: SectionSpec
) -> NarrativeResult:
    payload = _summary_payload(report)
    if not payload:
        raise NarrativeError("No generated Pass-1 sections to summarise")
    allowed_keys = sorted(payload.keys())

    model = _resolve_model(section_spec)
    version, _ = _load_prompt(spec.code, section_spec.narrative.prompt_key)
    system = _render_system_prompt(report, spec, section_spec)
    user = "Generated report sections:\n```json\n" + json.dumps(payload, default=str) + "\n```"

    # Citation-constrained schema ("no new claims" layer 2): source_sections is
    # an enum over the sections that actually generated — an uncited bullet or
    # an invented section key is schema-invalid.
    schema = {
        "type": "object",
        "properties": {
            "overall_assessment": {"type": "string"},
            "bullets": {
                "type": "array",
                "minItems": 3,
                "maxItems": 5,
                "items": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"},
                        "source_sections": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"type": "string", "enum": allowed_keys},
                        },
                    },
                    "required": ["text", "source_sections"],
                },
            },
        },
        "required": ["overall_assessment", "bullets"],
    }

    summary, input_tokens, output_tokens = await _call_with_validation(
        ExecutiveSummaryOutput,
        model=model,
        system=system,
        user_content=user,
        tool_name="emit_executive_summary",
        tool_description="Record the executive summary synthesised from the sections.",
        input_schema=schema,
        max_tokens=section_spec.narrative.max_tokens,
    )

    # "No new claims" layer 3 — numeric fact-check against the cited sections.
    problems = []
    for i, bullet in enumerate(summary.bullets, start=1):
        cited = [k for k in bullet.source_sections if k in payload]
        if not cited:
            problems.append(f"bullet {i} cites no generated section")
            continue
        unverified = fact_check.verify_bullet(bullet.text, [payload[k] for k in cited])
        if unverified:
            values = ", ".join(f"{v:g}" for v in unverified)
            problems.append(f"bullet {i} cites {values} — not found in {', '.join(cited)}")
    overall_unverified = fact_check.verify_bullet(
        summary.overall_assessment, list(payload.values())
    )
    if overall_unverified:
        values = ", ".join(f"{v:g}" for v in overall_unverified)
        problems.append(f"overall assessment cites {values} — not found in any section")
    if problems:
        raise NarrativeError("Fact check failed: " + "; ".join(problems))

    text = (
        summary.overall_assessment.strip()
        + "\n\n"
        + "\n\n".join("• " + b.text.strip() for b in summary.bullets)
    )
    return NarrativeResult(
        narrative_json=summary.model_dump(),
        narrative_text=text,
        data=None,
        model=model,
        prompt_version=version,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=_cost_usd(model, input_tokens, output_tokens),
    )


async def _call_with_validation(
    output_model,
    *,
    model: str,
    system: str,
    user_content: str,
    tool_name: str,
    tool_description: str,
    input_schema: dict,
    max_tokens: int,
):
    """Call + Pydantic-validate, retrying once with the validation error."""
    total_in = total_out = 0
    last_error: Optional[str] = None
    for attempt in range(_MAX_ATTEMPTS):
        attempt_system = system
        if last_error:
            attempt_system += (
                "\n\nYour previous output failed validation — fix this and try again: " + last_error
            )
        payload, tokens_in, tokens_out = await _call_structured(
            model,
            attempt_system,
            user_content,
            tool_name,
            tool_description,
            input_schema,
            max_tokens,
        )
        total_in += tokens_in
        total_out += tokens_out
        try:
            return output_model.model_validate(payload), total_in, total_out
        except ValidationError as exc:
            last_error = str(exc)[:800]
            logger.warning(
                "report_narrative_invalid_output",
                tool=tool_name,
                attempt=attempt + 1,
                error=last_error,
            )
    raise NarrativeError(f"Model output failed schema validation: {last_error}")
