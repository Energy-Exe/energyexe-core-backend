"""Tests for SCHEMA_NAMES — the single source of truth for human names."""

from app.models.opportunity import SchemaCode
from app.services.opportunity_schemas.schema_names import (
    SCHEMA_NAMES,
    SCHEMA_ONE_LINERS,
    format_schema_catalogue,
    get_schema_name,
)


def test_schema_names_cover_all_codes():
    """Every SchemaCode member has a non-empty SCHEMA_NAMES entry (all 19)."""
    assert len(list(SchemaCode)) == 19
    assert len(SCHEMA_NAMES) == 19
    for code in SchemaCode:
        assert code in SCHEMA_NAMES, f"missing SCHEMA_NAMES entry for {code}"
        name = SCHEMA_NAMES[code]
        assert isinstance(name, str)
        assert name.strip(), f"empty name for {code}"


def test_ops01_name_is_volatile_disruption_periods():
    """The one hard-required value asserted by the spec."""
    assert SCHEMA_NAMES[SchemaCode.OPS_01] == "Volatile Disruption Periods"


def test_one_liners_cover_all_codes():
    """One-line meanings stay in lock-step with SCHEMA_NAMES (same key set)."""
    assert set(SCHEMA_ONE_LINERS) == set(SCHEMA_NAMES)
    for code in SchemaCode:
        assert SCHEMA_ONE_LINERS[code].strip(), f"empty one-liner for {code}"


def test_get_schema_name_resolves_known_and_unknown():
    """get_schema_name maps a known code to its name and unknown/legacy → None."""
    assert get_schema_name("OPS_01") == "Volatile Disruption Periods"
    assert get_schema_name("FIN_01") == "P50 Generation Attainment"
    assert get_schema_name("LEGACY_99") is None
    assert get_schema_name("") is None


def test_format_schema_catalogue_lists_all_19_by_name():
    """The generated markdown catalogue references every schema by code + name."""
    catalogue = format_schema_catalogue()
    for code, name in SCHEMA_NAMES.items():
        assert code.value in catalogue
        assert name in catalogue
    # Domain headers present so the agent can group by domain.
    assert "Operational (OPS)" in catalogue
    assert "Market (MKT)" in catalogue
    assert "Financial (FIN)" in catalogue
    assert "Data Quality (DQ)" in catalogue
