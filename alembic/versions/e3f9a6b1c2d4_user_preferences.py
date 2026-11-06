"""user_preferences: per-user saved settings (EPR-202 default degradation horizon)

A new table only; nothing existing changes. One row per (user, key); deleting a user
deletes their preferences.

Revision ID: e3f9a6b1c2d4
Revises: d81a5c2e7f40
Create Date: 2026-11-01
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "e3f9a6b1c2d4"
down_revision = "d81a5c2e7f40"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_preferences",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "key"),
    )


def downgrade() -> None:
    op.drop_table("user_preferences")
