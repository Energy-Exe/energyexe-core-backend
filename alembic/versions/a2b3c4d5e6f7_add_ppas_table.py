"""Add PPAs table for Power Purchase Agreements

Revision ID: a2b3c4d5e6f7
Revises: 1dde591b6ee0
Create Date: 2025-11-26 12:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a2b3c4d5e6f7"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ppas",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("windfarm_id", sa.Integer(), nullable=False),
        sa.Column("ppa_buyer", sa.String(length=255), nullable=False),
        sa.Column("ppa_size_mw", sa.DECIMAL(precision=10, scale=2), nullable=False),
        sa.Column("ppa_duration_years", sa.Integer(), nullable=True),
        sa.Column("ppa_start_date", sa.Date(), nullable=True),
        sa.Column("ppa_end_date", sa.Date(), nullable=True),
        sa.Column("ppa_notes", sa.String(length=200), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["windfarm_id"],
            ["windfarms.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "windfarm_id",
            "ppa_buyer",
            "ppa_start_date",
            "ppa_end_date",
            name="uq_ppa_windfarm_buyer_dates",
        ),
    )
    op.create_index(op.f("ix_ppas_id"), "ppas", ["id"], unique=False)
    op.create_index(op.f("ix_ppas_windfarm_id"), "ppas", ["windfarm_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_ppas_windfarm_id"), table_name="ppas")
    op.drop_index(op.f("ix_ppas_id"), table_name="ppas")
    op.drop_table("ppas")
