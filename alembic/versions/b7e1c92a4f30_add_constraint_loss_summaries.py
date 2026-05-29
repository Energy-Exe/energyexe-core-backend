"""Add constraint_loss_summaries table for Module 3f.

Stores infrastructure-driven energy/revenue loss per confirmed structural-
constraint period, priced against the overall_clean Q50 capability curve
(issue #82). One row per (windfarm, period).

Revision ID: b7e1c92a4f30
Revises: f3a4b5c6d7e8
Create Date: 2026-05-29
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "b7e1c92a4f30"
down_revision = "f3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "constraint_loss_summaries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "windfarm_id",
            sa.Integer(),
            sa.ForeignKey("windfarms.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_hours", sa.Integer(), nullable=False),
        sa.Column("actual_mwh", sa.Numeric(14, 2), nullable=True),
        sa.Column("expected_mwh", sa.Numeric(14, 2), nullable=True),
        sa.Column("lost_mwh", sa.Numeric(14, 2), nullable=True),
        sa.Column("lost_eur", sa.Numeric(16, 2), nullable=True),
        sa.Column("mean_q90_ratio", sa.Numeric(6, 3), nullable=True),
        sa.Column(
            "reference_curve",
            sa.String(40),
            nullable=False,
            server_default="overall_clean_q50",
        ),
        sa.Column(
            "pipeline_run_id",
            sa.Integer(),
            sa.ForeignKey("import_job_executions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "windfarm_id",
            "period_start",
            "period_end",
            name="uq_cls_windfarm_period",
        ),
    )
    op.create_index("ix_cls_windfarm", "constraint_loss_summaries", ["windfarm_id"])


def downgrade() -> None:
    op.drop_index("ix_cls_windfarm", table_name="constraint_loss_summaries")
    op.drop_table("constraint_loss_summaries")
