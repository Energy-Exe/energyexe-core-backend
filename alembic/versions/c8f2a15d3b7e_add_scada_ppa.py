"""add scada_ppa table (EPR-97)

The detailed, SCADA-only PPA structure. Deliberately separate from the Perform ``ppas`` table, which
is left exactly as it is. App-authored state written only by the API, so it lives in the ordinary
public schema with a ``scada_`` prefix — same posture as scada_finding_action — not in the
pipeline-owned ``scada`` schema.

Multi-farm PPAs are N rows sharing a ppa_code, one per windfarm; (ppa_code, windfarm_id) is unique.
Enum-ish columns are plain varchar validated in Pydantic (house convention — no DB enum, no CHECK).

Revision ID: c8f2a15d3b7e
Revises: 7b1a9c3d5e02
Create Date: 2026-09-09
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c8f2a15d3b7e"
down_revision = "7b1a9c3d5e02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scada_ppa",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("windfarm_id", sa.Integer(), nullable=False),
        sa.Column("ppa_code", sa.String(length=50), nullable=False),
        sa.Column("ppa_buyer", sa.String(length=255), nullable=False),
        sa.Column("counterparty_role", sa.String(length=30), nullable=True),
        sa.Column("settlement_mechanism", sa.String(length=30), nullable=True),
        sa.Column("volume_shape", sa.String(length=30), nullable=True),
        sa.Column("ppa_status", sa.String(length=20), server_default="Draft", nullable=False),
        sa.Column("execution_date", sa.Date(), nullable=True),
        sa.Column("effective_date", sa.Date(), nullable=True),
        sa.Column("expiration_date", sa.Date(), nullable=True),
        sa.Column("currency", sa.String(length=3), nullable=True),
        sa.Column("power_share_pct", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("pricing_model", sa.String(length=20), nullable=True),
        sa.Column("strike_price", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("floor_price", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("cap_price", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("index_name", sa.String(length=50), nullable=True),
        sa.Column("index_spread", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("indexation_type", sa.String(length=20), nullable=True),
        sa.Column("indexation_rate_pct", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("has_availability_penalties", sa.Boolean(), nullable=True),
        sa.Column("availability_guarantee_pct", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("ppa_notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["windfarm_id"], ["windfarms.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ppa_code", "windfarm_id", name="uq_scada_ppa_code_windfarm"),
    )
    op.create_index(op.f("ix_scada_ppa_id"), "scada_ppa", ["id"], unique=False)
    op.create_index(op.f("ix_scada_ppa_windfarm_id"), "scada_ppa", ["windfarm_id"], unique=False)
    op.create_index(op.f("ix_scada_ppa_ppa_code"), "scada_ppa", ["ppa_code"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_scada_ppa_ppa_code"), table_name="scada_ppa")
    op.drop_index(op.f("ix_scada_ppa_windfarm_id"), table_name="scada_ppa")
    op.drop_index(op.f("ix_scada_ppa_id"), table_name="scada_ppa")
    op.drop_table("scada_ppa")
