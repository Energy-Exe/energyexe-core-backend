"""Pure, DB-free tests for the expanded opportunity enums (issue #88).

The ``SchemaCode`` / ``Severity`` / ``OpportunityStatus`` enums are plain
``str, Enum`` types stored in plain String columns (no Postgres native enum,
no CHECK constraint), so these can be exercised without any database.

Member-count note: the initiative is branded "6 -> 18" but the actual member
set is OPS_01..08 (8) + MKT_01..07 (7) + FIN_01..03 (3) + DQ_01 (1) = 19.
The "18" in the spec text is an arithmetic shorthand; the true count is 19,
which is what these tests assert.
"""

import importlib.util
from pathlib import Path

import pytest

from app.models.opportunity import OpportunityStatus, SchemaCode, Severity

# ─── SchemaCode ───────────────────────────────────────────────────


def test_schema_code_has_19_members():
    # OPS_01..08 (8) + MKT_01..07 (7) + FIN_01..03 (3) + DQ_01 (1) = 19.
    assert len(list(SchemaCode)) == 19


@pytest.mark.parametrize(
    "name,value",
    [
        ("OPS_04", "OPS_04"),
        ("OPS_05", "OPS_05"),
        ("OPS_06", "OPS_06"),
        ("OPS_07", "OPS_07"),
        ("OPS_08", "OPS_08"),
        ("MKT_04", "MKT_04"),
        ("MKT_05", "MKT_05"),
        ("MKT_06", "MKT_06"),
        ("MKT_07", "MKT_07"),
        ("FIN_01", "FIN_01"),
        ("FIN_02", "FIN_02"),
        ("FIN_03", "FIN_03"),
        ("DQ_01", "DQ_01"),
    ],
)
def test_new_schema_code_members_exist_with_expected_value(name, value):
    member = getattr(SchemaCode, name)
    assert member.value == value


def test_original_six_schema_codes_unchanged():
    for name in ("OPS_01", "OPS_02", "OPS_03", "MKT_01", "MKT_02", "MKT_03"):
        assert getattr(SchemaCode, name).value == name


def test_all_schema_code_values_fit_string_10_column():
    # schema_code column is String(10); every value must fit.
    assert all(len(code.value) <= 10 for code in SchemaCode)


# ─── Severity ─────────────────────────────────────────────────────


def test_severity_includes_suppressed():
    assert Severity.SUPPRESSED.value == "SUPPRESSED"


def test_all_severity_values_fit_string_15_column():
    # severity column is String(15); every value must fit.
    assert all(len(s.value) <= 15 for s in Severity)


# ─── OpportunityStatus ────────────────────────────────────────────


def test_status_includes_inactive():
    assert OpportunityStatus.INACTIVE.value == "INACTIVE"


def test_all_status_values_fit_string_15_column():
    # status column is String(15); every value must fit ("ACKNOWLEDGED" = 12).
    assert all(len(s.value) <= 15 for s in OpportunityStatus)


# ─── Migration (no-op) ────────────────────────────────────────────
#
# The plan's `test_migration_upgrade_then_downgrade_preserves_existing_rows`
# cannot run against the test SQLite DB: the `opportunities` table uses
# Postgres JSONB columns and is not created by tests/conftest.py. Since the
# migration is a documented no-op (the enum values live in plain String
# columns with no DB-level constraint, so no schema change is required), the
# honest equivalent is to assert the migration's upgrade/downgrade functions
# are importable and callable as no-ops. This still proves the migration is
# wired in and that running it cannot alter / lose existing rows.


def _load_migration_module():
    # alembic/versions is not an importable package (no __init__.py), so load
    # the migration file directly by path.
    path = (
        Path(__file__).resolve().parent.parent
        / "alembic"
        / "versions"
        / "c1f2a3b4d5e6_expand_opportunity_enums.py"
    )
    spec = importlib.util.spec_from_file_location("_expand_opportunity_enums", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_migration_revision_chains_onto_constraint_loss_head():
    mod = _load_migration_module()
    assert mod.revision == "c1f2a3b4d5e6"
    assert mod.down_revision == "b7e1c92a4f30"


def test_migration_upgrade_and_downgrade_are_callable_noops():
    """No-op migration: upgrade/downgrade are callable and touch nothing.

    Replaces the plan's row-preservation test, which would require a live
    Postgres (JSONB). A no-op upgrade/downgrade trivially preserves all
    existing rows because it issues no DDL/DML at all.
    """
    mod = _load_migration_module()
    # Calling with no bound op context must not raise — they are pure `pass`.
    assert mod.upgrade() is None
    assert mod.downgrade() is None
