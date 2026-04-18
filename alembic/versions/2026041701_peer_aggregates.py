"""peer_group_aggregates table for vs-zone-average comparisons.

Cross-cutting prerequisite (PRE-B) for spec items 3, 4, 5, 6 — all of which
need to compare a windfarm's metric against the average for its bidzone /
country / owner / turbine-model peer group.

Revision ID: 2026041701_peer_agg
Revises: 2026041700_weather_no_nan
Create Date: 2026-04-17
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2026041701_peer_agg"
down_revision = "2026041700_weather_no_nan"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Use raw SQL with IF NOT EXISTS — table may have been created by a
    # previous partial migration run or manual DDL.
    op.execute("""
        CREATE TABLE IF NOT EXISTS peer_group_aggregates (
            id SERIAL PRIMARY KEY,
            group_type VARCHAR(20) NOT NULL,
            group_id INTEGER NOT NULL,
            metric_key VARCHAR(60) NOT NULL,
            period_type VARCHAR(10) NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER,
            windfarm_count INTEGER NOT NULL,
            avg_value NUMERIC(14, 4),
            p10_value NUMERIC(14, 4),
            p50_value NUMERIC(14, 4),
            p90_value NUMERIC(14, 4),
            computed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
        )
    """)
    # Constraint + index — IF NOT EXISTS via DO $$ block
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'uq_peer_group_aggregate'
            ) THEN
                ALTER TABLE peer_group_aggregates
                ADD CONSTRAINT uq_peer_group_aggregate
                UNIQUE (group_type, group_id, metric_key, period_type, year, month);
            END IF;
        END $$
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_peer_aggregate_lookup
        ON peer_group_aggregates (group_type, group_id, metric_key, year)
    """)


def downgrade() -> None:
    op.drop_index("ix_peer_aggregate_lookup", table_name="peer_group_aggregates")
    op.drop_table("peer_group_aggregates")
