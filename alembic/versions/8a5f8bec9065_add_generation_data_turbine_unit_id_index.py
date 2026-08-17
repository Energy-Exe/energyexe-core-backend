"""Add partial index on generation_data.turbine_unit_id (EPR-107 bulk delete)

Deleting a turbine unit fires a per-row FK check (and a de-associating UPDATE)
against generation_data.turbine_unit_id, which had no index — every delete cost
a full scan of the ~25M-row table. The column is ~99% NULL, so a partial index
is tiny and serves both the RI trigger's equality probe and the bulk UPDATE.

Built CONCURRENTLY (in an autocommit block) so imports writing to
generation_data are not blocked while the index builds.

Revision ID: 8a5f8bec9065
Revises: f7a8b9c0d1e2
Create Date: 2026-08-17 15:20:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "8a5f8bec9065"
down_revision = "f7a8b9c0d1e2"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_generation_data_turbine_unit_id"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} "
            "ON generation_data (turbine_unit_id) "
            "WHERE turbine_unit_id IS NOT NULL"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
