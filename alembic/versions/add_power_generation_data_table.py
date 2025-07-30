"""Add power generation data table

Revision ID: add_power_generation_data
Revises: f9527490e4ad
Create Date: 2025-07-30

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "add_power_generation_data"
down_revision = "f9527490e4ad"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create power_generation_data table
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

    # Create indexes
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

    # Create views for hourly and daily summaries (non-materialized for now)
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


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS generation_daily_summary")
    op.execute("DROP VIEW IF EXISTS generation_hourly_summary")
    op.drop_index("idx_generation_fetch_history", table_name="power_generation_data")
    op.drop_index("idx_generation_unit_time", table_name="power_generation_data")
    op.drop_index("idx_generation_area_type_time", table_name="power_generation_data")
    op.drop_table("power_generation_data")
