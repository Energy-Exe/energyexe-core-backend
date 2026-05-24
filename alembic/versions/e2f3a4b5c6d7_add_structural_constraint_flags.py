"""Add structural_constraint_flags table for Module 1b.

Stores auto-detected constraint runs (cable failures, half-BMU offline,
etc.) pending analyst review. Downstream modules will consume confirmed
flags in a follow-up milestone — for now this is write-only.

Revision ID: e2f3a4b5c6d7
Revises: e4a1c83d9b21
Create Date: 2026-05-24
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e2f3a4b5c6d7"
down_revision = "e4a1c83d9b21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "structural_constraint_flags",
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
        sa.Column("wind_bins_affected", sa.Integer(), nullable=True),
        sa.Column("mean_q90_ratio", sa.Numeric(6, 3), nullable=True),
        sa.Column("mean_q50_ratio", sa.Numeric(6, 3), nullable=True),
        sa.Column(
            "flag_trigger",
            sa.String(20),
            nullable=False,
            server_default="q90_ratio",
        ),
        sa.Column(
            "flag_source",
            sa.String(40),
            nullable=False,
            server_default="auto_constraint_detector",
        ),
        sa.Column(
            "review_status",
            sa.String(20),
            nullable=False,
            server_default="pending_review",
        ),
        sa.Column("analyst_notes", sa.Text(), nullable=True),
        sa.Column(
            "reviewed_by",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "pipeline_run_id",
            sa.Integer(),
            sa.ForeignKey("import_job_executions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "windfarm_id",
            "period_start",
            "period_end",
            name="uq_scf_windfarm_period",
        ),
    )
    op.create_index(
        "ix_structural_constraint_flags_windfarm_id",
        "structural_constraint_flags",
        ["windfarm_id"],
    )
    op.create_index("ix_scf_status", "structural_constraint_flags", ["review_status"])
    op.create_index("ix_scf_windfarm", "structural_constraint_flags", ["windfarm_id"])


def downgrade() -> None:
    op.drop_index("ix_scf_windfarm", table_name="structural_constraint_flags")
    op.drop_index("ix_scf_status", table_name="structural_constraint_flags")
    op.drop_index(
        "ix_structural_constraint_flags_windfarm_id",
        table_name="structural_constraint_flags",
    )
    op.drop_table("structural_constraint_flags")
