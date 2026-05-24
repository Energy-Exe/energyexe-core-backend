"""Add contract_revenue_eur and contract_revenue_vs_p50_target_eur on performance_summaries.

Surfaces price-weighted commercial revenue alongside the existing
constraint proxy / lost-value columns. Per the reference pipeline
(`energyexe_pipeline_full.py:1150-1166`), this is what the board-level
commercial summary shows.

Revision ID: d1f2a3b4c5e6
Revises: c8d9e0f1a2b3
Create Date: 2026-05-24
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "d1f2a3b4c5e6"
down_revision = "c8d9e0f1a2b3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "performance_summaries",
        sa.Column("contract_revenue_eur", sa.Numeric(14, 2), nullable=True),
    )
    op.add_column(
        "performance_summaries",
        sa.Column("contract_revenue_vs_p50_target_eur", sa.Numeric(14, 2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("performance_summaries", "contract_revenue_vs_p50_target_eur")
    op.drop_column("performance_summaries", "contract_revenue_eur")
