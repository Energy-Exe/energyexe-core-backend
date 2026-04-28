"""generation_concentration_summaries — spec item 3 (new module).

Per (windfarm, year[, month]): hourly generation partitioned into price deciles,
capture ratio, top/bottom decile shares, and zone comparison.

Revision ID: 2026041702_genconc
Revises: 2026041701_peer_agg
Create Date: 2026-04-17
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "2026041702_genconc"
down_revision = "2026041701_peer_agg"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Use raw SQL with IF NOT EXISTS — table may have been created by a
    # previous partial migration run or manual DDL.
    op.execute("""
        CREATE TABLE IF NOT EXISTS generation_concentration_summaries (
            id SERIAL PRIMARY KEY,
            windfarm_id INTEGER NOT NULL REFERENCES windfarms(id) ON DELETE CASCADE,
            period_type VARCHAR(10) NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER,
            total_mwh NUMERIC(14, 3),
            total_hours INTEGER,
            weighted_avg_capture_price_eur NUMERIC(12, 4),
            time_weighted_avg_price_eur NUMERIC(12, 4),
            capture_ratio NUMERIC(8, 4),
            top_decile_share_pct NUMERIC(7, 3),
            top_quartile_share_pct NUMERIC(7, 3),
            bottom_decile_share_pct NUMERIC(7, 3),
            bottom_quartile_share_pct NUMERIC(7, 3),
            decile_shares JSONB,
            vs_zone_capture_ratio_diff NUMERIC(8, 4),
            vs_zone_top_decile_diff NUMERIC(7, 3),
            pipeline_run_id INTEGER REFERENCES import_job_executions(id) ON DELETE SET NULL,
            computed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
        )
    """)
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'uq_generation_concentration_wf_period'
            ) THEN
                ALTER TABLE generation_concentration_summaries
                ADD CONSTRAINT uq_generation_concentration_wf_period
                UNIQUE (windfarm_id, period_type, year, month);
            END IF;
        END $$
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_genconc_wf_year
        ON generation_concentration_summaries (windfarm_id, year)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_genconc_summaries_windfarm_id
        ON generation_concentration_summaries (windfarm_id)
    """)


def downgrade() -> None:
    op.drop_index(
        "ix_genconc_summaries_windfarm_id",
        table_name="generation_concentration_summaries",
    )
    op.drop_index(
        "ix_genconc_wf_year",
        table_name="generation_concentration_summaries",
    )
    op.drop_table("generation_concentration_summaries")
