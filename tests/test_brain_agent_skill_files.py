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


# ── SCADA gold-layer knowledgebase (schema `scada`) ──


def test_scada_skill_covers_all_gold_tables():
    """Canary: every scada gold table stays documented in SKILL_SCADA."""
    from app.services.brain_agent_skill_files import SKILL_SCADA

    for table in (
        "dim_farm",
        "dim_turbine",
        "dim_turbine_config",
        "dim_event_category",
        "dim_signal_capability",
        "completeness_daily",
        "energy_daily",
        "energy_monthly_utc",
        "availability_daily",
        "losses_daily",
        "farm_kpis_daily",
        "power_curve_bins",
        "power_curve_bins_yearly",
        "losses_hourly",
        "revenue_impact_daily",
        "settlement_recon_daily",
        "turbine_performance_yearly",
        "dim_alarm_code",
        "alarm_events",
        "alarm_code_daily",
    ):
        assert f"scada.{table}" in SKILL_SCADA, f"scada.{table} missing from SKILL_SCADA"


def test_scada_skill_pins_frame_caveat():
    """Gotcha 57, post-flip: the whole schema is UTC-keyed (date_utc); the
    residual frame caveat is against GB-LOCAL-keyed sources (settlement
    statements), and date_local must be gone entirely."""
    from app.services.brain_agent_skill_files import SKILL_SCADA

    for phrase in (
        "date_utc",
        "scada.energy_monthly_utc",
        "UTC-keyed",
        "GB-LOCAL",
    ):
        assert phrase in SKILL_SCADA, f"frame caveat phrase missing: {phrase}"
    assert "date_local" not in SKILL_SCADA, "date_local resurfaced after the UTC flip"


def test_scada_skill_pins_alarm_caveats():
    """The alarm-lane load-bearing caveats: buckets are proposals, alarm hours
    are not downtime, undocumented codes are never invented, instantaneous
    events are filtered for duration analysis."""
    from app.services.brain_agent_skill_files import (
        SKILL_SCADA,
        SKILL_SCADA_QUERIES,
    )

    assert "PROPOSALS" in SKILL_SCADA  # dim_alarm_code.status = proposed
    assert "proposed" in SKILL_SCADA and "confirmed" in SKILL_SCADA
    assert "Alarm hours are NOT downtime hours" in SKILL_SCADA
    assert "NEVER invent what a code means" in SKILL_SCADA
    assert "duration_h IS NOT NULL" in SKILL_SCADA
    assert "alarm_code_daily" in SKILL_SCADA_QUERIES
    assert "dim_alarm_code" in SKILL_SCADA_QUERIES
    assert "buckets are proposed" in SKILL_SCADA_QUERIES


def test_scada_skill_pins_the_conventions():
    """The load-bearing caveats: schema-qualify, units, ratio-of-sums, linkage."""
    from app.services.brain_agent_skill_files import SKILL_SCADA

    assert "schema-qualify" in SKILL_SCADA
    assert "hill_of_towie" in SKILL_SCADA
    assert "kelmarsh" in SKILL_SCADA
    assert "penmanshiel" in SKILL_SCADA
    assert "7309" in SKILL_SCADA  # Hill of Towie windfarm_id
    assert "ratio-of-sums" in SKILL_SCADA
    assert "kWh" in SKILL_SCADA and "MWh" in SKILL_SCADA
    assert "pre_cod" in SKILL_SCADA
    assert "IEC 61400-26" in SKILL_SCADA


def test_scada_queries_steer_to_rollups():
    """Efficiency rules: pre-aggregated tables first, indexed keys, x-schema join."""
    from app.services.brain_agent_skill_files import SKILL_SCADA_QUERIES

    assert "farm_kpis_daily" in SKILL_SCADA_QUERIES
    assert "revenue_impact_daily" in SKILL_SCADA_QUERIES
    assert "settlement_recon_daily" in SKILL_SCADA_QUERIES
    assert "public.windfarms" in SKILL_SCADA_QUERIES  # cross-schema template
    assert "never AVG" in SKILL_SCADA_QUERIES or "NEVER AVG" in SKILL_SCADA_QUERIES


def test_scada_content_stays_out_of_unconditional_skills():
    """SCADA text must live ONLY in the gated files — the static skill strings
    are written to every sandbox including prod, where schema scada may not
    exist yet."""
    for blob in (SKILL_SCHEMA, SKILL_QUERIES, SKILL_DOMAIN, SKILL_SOURCES):
        assert "scada." not in blob
        assert "hill_of_towie" not in blob
