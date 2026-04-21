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
