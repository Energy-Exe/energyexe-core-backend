"""expand opportunity enums

Revision ID: c1f2a3b4d5e6
Revises: b7e1c92a4f30
Create Date: 2026-05-31 00:00:00.000000

Documentation / no-op migration for the Opportunity Detection 6 -> 18 (actual
member count: 19) schema expansion.

The ``opportunities`` table (created in revision ``b064d48e436b``) stores the
``schema_code`` / ``severity`` / ``status`` enum values in PLAIN string columns:

    schema_code  VARCHAR(10)   -- longest new value "OPS_04".."FIN_03" = 6 chars
    severity     VARCHAR(15)   -- longest value "SUPPRESSED"/"INDICATIVE" = 10 chars
    status       VARCHAR(15)   -- longest value "ACKNOWLEDGED" = 12 chars

There are NO Postgres native ENUM types and NO CHECK constraints tied to these
columns — the allowed values are enforced exclusively at the Python application
layer (``app/models/opportunity.py``). Adding the new members therefore requires
NO database schema change:

  * ``SchemaCode`` gains OPS_04..OPS_08, MKT_04..MKT_07, FIN_01..FIN_03, DQ_01.
  * ``Severity`` gains ``SUPPRESSED``.
  * ``OpportunityStatus`` gains ``INACTIVE``.

Every new value fits comfortably within the existing column widths, so no
``ALTER COLUMN ... TYPE`` is needed and the partial unique index
``ix_opportunities_active_unique (windfarm_id, schema_code) WHERE status='ACTIVE'``
is unchanged. Existing rows are completely unaffected by this migration.

This revision is intentionally a no-op; it exists to record the model-layer
enum expansion in the migration history so the chain stays linear and the
change is auditable.
"""
import sqlalchemy as sa  # noqa: F401

from alembic import op  # noqa: F401

# revision identifiers, used by Alembic.
revision = "c1f2a3b4d5e6"
down_revision = "b7e1c92a4f30"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """No-op: enum values live in plain String columns with no DB-level constraint.

    See the module docstring for the full rationale. The new ``SchemaCode``,
    ``Severity`` and ``OpportunityStatus`` members are application-layer only and
    fit the existing column widths, so there is nothing to migrate.
    """
    pass


def downgrade() -> None:
    """No-op: there is no schema change to reverse.

    Existing ``opportunities`` rows are untouched by the corresponding
    ``upgrade()``; downgrading is a no-op for the same reason.
    """
    pass
