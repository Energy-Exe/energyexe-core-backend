"""Add reports + report_sections tables (EPR-81/82 report generation platform)

Revision ID: f7a8b9c0d1e2
Revises: e5f6a7b8c9d0
Create Date: 2026-08-17 01:30:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "f7a8b9c0d1e2"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "reports",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("report_type", sa.String(length=32), nullable=False),
        sa.Column("scope_type", sa.String(length=16), nullable=False),
        sa.Column("windfarm_id", sa.Integer(), nullable=True),
        sa.Column("portfolio_id", sa.Integer(), nullable=True),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="PENDING", nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("params", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("pdf_s3_key", sa.String(length=512), nullable=True),
        sa.Column("pdf_generated_at", sa.DateTime(), nullable=True),
        sa.Column("pdf_downloaded_at", sa.DateTime(), nullable=True),
        sa.Column("locked", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("superseded_by_id", sa.Integer(), nullable=True),
        sa.Column("requested_by_id", sa.Integer(), nullable=False),
        sa.Column("total_cost_usd", sa.Numeric(10, 4), server_default=sa.text("0"), nullable=False),
        sa.Column("total_input_tokens", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("total_output_tokens", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("generation_started_at", sa.DateTime(), nullable=True),
        sa.Column("generation_completed_at", sa.DateTime(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["windfarm_id"], ["windfarms.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["portfolio_id"], ["portfolios.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["superseded_by_id"], ["reports.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["requested_by_id"], ["users.id"]),
    )
    op.create_index("ix_reports_id", "reports", ["id"])
    op.create_index("ix_reports_report_type", "reports", ["report_type"])
    op.create_index("ix_reports_windfarm_id", "reports", ["windfarm_id"])
    op.create_index("ix_reports_portfolio_id", "reports", ["portfolio_id"])
    op.create_index("ix_reports_status", "reports", ["status"])
    op.create_index("ix_reports_requested_by_id", "reports", ["requested_by_id"])
    # One in-flight generation per logical target (concurrent POST -> IntegrityError -> 409).
    op.create_index(
        "uq_reports_inflight",
        "reports",
        ["report_type", "windfarm_id", "portfolio_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('PENDING', 'GENERATING')"),
    )
    op.create_index(
        "ix_reports_library",
        "reports",
        ["report_type", "windfarm_id", "created_at"],
        postgresql_where=sa.text("deleted_at IS NULL"),
    )

    op.create_table(
        "report_sections",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("report_id", sa.Integer(), nullable=False),
        sa.Column("section_key", sa.String(length=64), nullable=False),
        sa.Column("pass_number", sa.SmallInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("display_order", sa.SmallInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("layout", sa.String(length=16), server_default="full", nullable=False),
        sa.Column("status", sa.String(length=16), server_default="UNGENERATED", nullable=False),
        sa.Column("data", JSONB(), nullable=True),
        sa.Column("narrative_json", JSONB(), nullable=True),
        sa.Column("narrative_text", sa.Text(), nullable=True),
        sa.Column("llm_model", sa.String(length=64), nullable=True),
        sa.Column("prompt_version", sa.String(length=16), nullable=True),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("cost_usd", sa.Numeric(10, 4), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("generated_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["report_id"], ["reports.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("report_id", "section_key", name="uq_report_section_key"),
    )
    op.create_index("ix_report_sections_id", "report_sections", ["id"])
    op.create_index("ix_report_sections_report_id", "report_sections", ["report_id"])


def downgrade() -> None:
    op.drop_table("report_sections")
    op.drop_table("reports")
