"""add user_consents table

Revision ID: e4a1c83d9b21
Revises: c2a7b5e91f48
Create Date: 2026-05-21 02:40:00.000000

"""

import sqlalchemy as sa
from alembic import op


revision = "e4a1c83d9b21"
down_revision = "c2a7b5e91f48"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_consents",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("document_type", sa.String(length=16), nullable=False),
        sa.Column("document_version", sa.String(length=32), nullable=False),
        sa.Column(
            "accepted_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("ip_address", sa.String(length=45), nullable=True),
        sa.Column("user_agent", sa.String(length=512), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_user_consents_id", "user_consents", ["id"], unique=False)
    op.create_index("ix_user_consents_user_id", "user_consents", ["user_id"], unique=False)
    op.create_index(
        "ix_user_consents_user_doc",
        "user_consents",
        ["user_id", "document_type"],
        unique=False,
    )
    op.create_index(
        "ix_user_consents_accepted_at",
        "user_consents",
        ["accepted_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_user_consents_accepted_at", table_name="user_consents")
    op.drop_index("ix_user_consents_user_doc", table_name="user_consents")
    op.drop_index("ix_user_consents_user_id", table_name="user_consents")
    op.drop_index("ix_user_consents_id", table_name="user_consents")
    op.drop_table("user_consents")
