"""Remove all ENTSOE historical implementation

Revision ID: remove_all_entsoe_historical
Revises: add_entsoe_fetch_history
Create Date: 2025-01-11

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "remove_all_entsoe_historical"
down_revision = "add_entsoe_fetch_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Drop all ENTSOE historical-related tables and views."""
    
    # Drop indexes first
    op.execute("DROP INDEX IF EXISTS idx_entsoe_fetch_history_created")
    op.execute("DROP INDEX IF EXISTS idx_entsoe_fetch_history_status")
    op.execute("DROP INDEX IF EXISTS ix_entsoe_fetch_history_id")
    
    # Drop the entsoe_fetch_history table
    op.execute("DROP TABLE IF EXISTS entsoe_fetch_history CASCADE")
    
    # Drop any remaining ENTSOE-related tables that might exist
    op.execute("DROP TABLE IF EXISTS power_generation_data CASCADE")
    
    # Drop any ENTSOE-related views
    op.execute("DROP VIEW IF EXISTS generation_daily_summary CASCADE")
    op.execute("DROP VIEW IF EXISTS generation_hourly_summary CASCADE")
    
    # Drop any TimescaleDB hypertables if they exist
    op.execute("DROP TABLE IF EXISTS entsoe_generation_data CASCADE")
    op.execute("DROP TABLE IF EXISTS entsoe_data_availability CASCADE")


def downgrade() -> None:
    """Recreate ENTSOE fetch history table (basic rollback)."""
    
    # Recreate entsoe_fetch_history table
    op.create_table(
        "entsoe_fetch_history",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("request_type", sa.String(length=50), nullable=False),
        sa.Column("start_datetime", sa.DateTime(), nullable=False),
        sa.Column("end_datetime", sa.DateTime(), nullable=False),
        sa.Column("area_code", sa.String(length=100), nullable=False),
        sa.Column("production_type", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("records_fetched", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("response_time_ms", sa.Integer(), nullable=True),
        sa.Column("requested_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["requested_by_user_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_entsoe_fetch_history_id"), "entsoe_fetch_history", ["id"], unique=False
    )
    op.create_index(
        "idx_entsoe_fetch_history_status", "entsoe_fetch_history", ["status"], unique=False
    )
    op.create_index(
        "idx_entsoe_fetch_history_created", "entsoe_fetch_history", ["created_at"], unique=False
    )