"""Add is_deleted soft-delete flag to windfarms

Revision ID: b8d3f72c5e61
Revises: a7c2e91b4d50
Create Date: 2026-06-04

Soft delete: hides windfarms from the client-facing platform (e.g.
German/Dutch windfarms without EEX generation data) without removing
them from the DB. Admin UI always sees all windfarms and toggles this
flag; the admin Delete action remains a hard delete.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b8d3f72c5e61"
down_revision = "a7c2e91b4d50"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "windfarms",
        sa.Column(
            "is_deleted",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("windfarms", "is_deleted")
