"""Remove ENTSOE tables and power generation data

Revision ID: cleanup_entsoe_tables
Revises: 18e12df6700a
Create Date: 2025-01-16

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "cleanup_entsoe_tables"
down_revision = "18e12df6700a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Remove ENTSOE-related tables and views."""
    
    # Drop views first (they depend on the tables)
    op.execute("DROP VIEW IF EXISTS generation_daily_summary CASCADE")
    op.execute("DROP VIEW IF EXISTS generation_hourly_summary CASCADE")
    
    # Drop indexes if they exist
    op.execute("DROP INDEX IF EXISTS idx_generation_fetch_history")
    op.execute("DROP INDEX IF EXISTS idx_generation_unit_time")
    op.execute("DROP INDEX IF EXISTS idx_generation_area_type_time")
    op.execute("DROP INDEX IF EXISTS ix_entsoe_fetch_history_id")
    
    # Drop tables
    op.execute("DROP TABLE IF EXISTS power_generation_data CASCADE")
    op.execute("DROP TABLE IF EXISTS entsoe_fetch_history CASCADE")


def downgrade() -> None:
    """Recreate ENTSOE tables (rollback)."""
    
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
        sa.Column("records_fetched", sa.Integer(), nullable=False, default=0),
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
    
    # Recreate power_generation_data table
    op.create_table(
        "power_generation_data",
        sa.Column("time", sa.DateTime(), nullable=False),
        sa.Column("area_code", sa.String(length=10), nullable=False),
        sa.Column("production_type", sa.String(length=20), nullable=False),
        sa.Column("generation_unit_code", sa.String(length=50), nullable=True),
        sa.Column(
            "generation_unit_source", sa.String(length=20), nullable=True, server_default="ENTSOE"
        ),
        sa.Column("value_mw", sa.Float(), nullable=False),
        sa.Column("data_quality_score", sa.Float(), nullable=True, server_default="1.0"),
        sa.Column("fetch_history_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["fetch_history_id"],
            ["entsoe_fetch_history.id"],
        ),
        sa.PrimaryKeyConstraint("time", "area_code", "production_type"),
    )
    
    # Recreate indexes
    op.create_index(
        "idx_generation_area_type_time",
        "power_generation_data",
        ["area_code", "production_type", "time"],
        unique=False,
    )
    op.create_index(
        "idx_generation_unit_time",
        "power_generation_data",
        ["generation_unit_code", "time"],
        unique=False,
    )
    op.create_index(
        "idx_generation_fetch_history", "power_generation_data", ["fetch_history_id"], unique=False
    )
    
    # Recreate views
    op.execute(
        """
        CREATE OR REPLACE VIEW generation_hourly_summary AS
        SELECT 
            DATE_TRUNC('hour', time) AS hour,
            area_code,
            production_type,
            AVG(value_mw) as avg_mw,
            MIN(value_mw) as min_mw,
            MAX(value_mw) as max_mw,
            COUNT(*) as data_points
        FROM power_generation_data
        GROUP BY DATE_TRUNC('hour', time), area_code, production_type
    """
    )
    
    op.execute(
        """
        CREATE OR REPLACE VIEW generation_daily_summary AS
        SELECT 
            DATE_TRUNC('day', time) AS day,
            area_code,
            production_type,
            AVG(value_mw) as avg_mw,
            MIN(value_mw) as min_mw,
            MAX(value_mw) as max_mw,
            SUM(value_mw) as total_mw,
            COUNT(*) as data_points
        FROM power_generation_data
        GROUP BY DATE_TRUNC('day', time), area_code, production_type
    """
    )