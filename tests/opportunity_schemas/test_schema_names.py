"""Tests for SCHEMA_NAMES — the single source of truth for human names."""

from app.models.opportunity import SchemaCode
from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES


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
