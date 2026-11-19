"""opportunities: delete the old OPS_03 rows (EPR-95)

OPS_03 (misaligned contracting) is switched off in the registry, and the previous revision marked its rows
SUPERSEDED. The trigger never had the contract data it needs, so its findings were never sound: they are deleted
(decision 19 November). The only reference to an opportunity is another opportunity's triggered_by_id, which is
SET NULL on delete; none points at an OPS_03 row on staging.

Revision ID: f2c9a7d41e05
Revises: a6d2f8c3e1b7
Create Date: 2026-11-19
"""

from alembic import op

revision = "f2c9a7d41e05"
down_revision = "a6d2f8c3e1b7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DELETE FROM opportunities WHERE schema_code = 'OPS_03'")


def downgrade() -> None:
    # The deleted rows are not recoverable; the detection run would recreate findings only if OPS_03 were
    # switched back on in the registry.
    pass
