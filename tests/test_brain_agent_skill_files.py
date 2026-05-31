"""Guardrail: ensure new-pipeline tables & caveats stay present in skill files.

The brain agent discovers the database schema by reading these strings at
session start. If a future edit silently removes the new-pipeline table
entries or the metric caveats, the agent will revert to raw-SQL fallbacks
or misleading answers — this test is the canary.
"""
from app.services.brain_agent_skill_files import (
    SKILL_DOMAIN,
    SKILL_QUERIES,
    SKILL_SCHEMA,
    SKILL_SOURCES,
)


def test_schema_mentions_pipeline_tables():
    assert "generation_concentration_summaries" in SKILL_SCHEMA
    assert "peer_group_aggregates" in SKILL_SCHEMA
    assert "performance_summaries" in SKILL_SCHEMA
    assert "degradation_results" in SKILL_SCHEMA
    assert "power_curve_bins" in SKILL_SCHEMA


def test_schema_lists_peer_metric_keys():
    # Minimal sample — full list is intentionally enumerated in the schema so
    # the agent can pick the right metric_key without a second lookup.
    assert "odi_pct_underperf" in SKILL_SCHEMA
    assert "wind_norm_index_p50" in SKILL_SCHEMA
    assert "degradation_slope_pct_per_year_q50" in SKILL_SCHEMA
    assert "concentration_capture_ratio" in SKILL_SCHEMA


def test_queries_include_pipeline_examples():
    assert "generation_concentration_summaries" in SKILL_QUERIES
    assert "peer_group_aggregates" in SKILL_QUERIES
    assert "norm_index_p50" in SKILL_QUERIES
    assert "power_curve_bins" in SKILL_QUERIES
    assert "degradation_results" in SKILL_QUERIES


def test_domain_documents_odi_eur_caveat():
    # The exact wording can drift; pin down the two ideas that matter.
    assert "period-average" in SKILL_DOMAIN
    assert "odi_pct_loss_eur" in SKILL_DOMAIN


def test_domain_documents_degradation_caveats():
    assert "0.35" in SKILL_DOMAIN  # baseline_cap_pu placeholder
    assert "seasonal" in SKILL_DOMAIN.lower()  # absence of seasonal_decompose
    assert "slope_pu_per_year" in SKILL_DOMAIN  # what to quote instead


def test_domain_documents_generation_concentration():
    assert "capture_ratio" in SKILL_DOMAIN
    assert "decile" in SKILL_DOMAIN.lower()
    assert "decile_shares" in SKILL_DOMAIN


def test_domain_documents_peer_group_aggregates():
    assert "peer_group_aggregates" in SKILL_DOMAIN
    assert "bidzone" in SKILL_DOMAIN
    # Should tell the agent about the fallback path
    assert "country" in SKILL_DOMAIN.lower()


def test_sources_file_unchanged_sentinel():
    # SKILL_SOURCES is intentionally NOT part of this change; these assertions
    # catch accidental edits to it.
    assert "ELEXON" in SKILL_SOURCES
    assert "EEX" in SKILL_SOURCES  # negative-list entry
    assert "Taipower" in SKILL_SOURCES


# ── #115: schema-by-NAME surfacing in the brain-agent content ──


def test_brain_agent_skill_lists_19_schemas():
    """The domain skill file references every schema BY NAME (all 19).

    Generated from SCHEMA_NAMES (single source of truth), so this also guards
    against the agent reverting to a stale 6-schema or 18-vs-19 list.
    """
    from app.models.opportunity import SchemaCode
    from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES

    assert len(SCHEMA_NAMES) == 19
    assert len(list(SchemaCode)) == 19

    # Every human name appears verbatim in the skill file the agent reads.
    for code, name in SCHEMA_NAMES.items():
        assert name in SKILL_DOMAIN, f"{code.value} name '{name}' missing from SKILL_DOMAIN"

    # Spec hard requirement: present by name, plus INACTIVE/SUPPRESSED semantics.
    assert "Volatile Disruption Periods" in SKILL_DOMAIN
    assert "INACTIVE" in SKILL_DOMAIN
    assert "SUPPRESSED" in SKILL_DOMAIN


def test_schema_file_opportunities_row_lists_all_19_codes():
    """The opportunities-table reference in SKILL_SCHEMA lists every schema_code.

    Reliability/linkage fix: this row used to hard-list only the original 6 codes
    (OPS_01/02/03 + MKT_01/02/03), so an agent reading it believed only 6 schemas
    existed. It's now generated from SCHEMA_NAMES, so it can never go stale again.
    """
    from app.models.opportunity import SchemaCode
    from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES

    for code in SchemaCode:
        assert code.value in SKILL_SCHEMA, f"{code.value} missing from SKILL_SCHEMA"
    # The placeholder must have been interpolated (no literal token left behind).
    assert "__OPPORTUNITY_SCHEMA_CODES__" not in SKILL_SCHEMA
    # Newer tiers the row previously omitted.
    assert "SUPPRESSED" in SKILL_SCHEMA
    assert "INACTIVE" in SKILL_SCHEMA
    assert len(SCHEMA_NAMES) == 19


def test_brain_agent_system_prompts_list_all_19_schema_names():
    """Both system-prompt markdown files surface every schema by name."""
    from pathlib import Path

    from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES

    prompts_dir = Path("app/prompts")
    for fname in ("brain_agent_system.md", "brain_agent_system_client.md"):
        text = (prompts_dir / fname).read_text(encoding="utf-8")
        for code, name in SCHEMA_NAMES.items():
            assert name in text, f"{name} missing from {fname}"
        # INACTIVE schemas flagged so the agent excludes them from active findings.
        assert "INACTIVE" in text, f"INACTIVE semantics missing from {fname}"
        assert "SUPPRESSED" in text, f"SUPPRESSED semantics missing from {fname}"
