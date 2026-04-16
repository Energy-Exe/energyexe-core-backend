"""add ppa contract fields and opportunities table

Revision ID: b064d48e436b
Revises: ee765f7be98b
Create Date: 2026-04-11 03:08:51.252108

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'b064d48e436b'
down_revision = 'ee765f7be98b'
branch_labels = None
depends_on = None


def _column_exists(conn, table, column):
    result = conn.execute(sa.text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = :table AND column_name = :col"
    ), {"table": table, "col": column})
    return result.fetchone() is not None


def _table_exists(conn, table):
    result = conn.execute(sa.text(
        "SELECT 1 FROM information_schema.tables WHERE table_name = :table"
    ), {"table": table})
    return result.fetchone() is not None


def upgrade() -> None:
    conn = op.get_bind()

    # --- PPA contract fields (idempotent) ---
    if not _column_exists(conn, 'ppas', 'contract_type'):
        op.execute("ALTER TABLE ppas ADD COLUMN contract_type VARCHAR(50)")
    if not _column_exists(conn, 'ppas', 'ppa_status'):
        op.execute("ALTER TABLE ppas ADD COLUMN ppa_status VARCHAR(30)")
    if not _column_exists(conn, 'ppas', 'ppa_price_eur_mwh'):
        op.execute("ALTER TABLE ppas ADD COLUMN ppa_price_eur_mwh NUMERIC(10,2)")
    if not _column_exists(conn, 'ppas', 'has_availability_penalties'):
        op.execute("ALTER TABLE ppas ADD COLUMN has_availability_penalties BOOLEAN")

    # --- Opportunities table (idempotent) ---
    if not _table_exists(conn, 'opportunities'):
        op.create_table(
            'opportunities',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('windfarm_id', sa.Integer(), sa.ForeignKey('windfarms.id', ondelete='CASCADE'), nullable=False, index=True),
            sa.Column('schema_code', sa.String(10), nullable=False, index=True),
            sa.Column('severity', sa.String(15), nullable=False),
            sa.Column('branch', sa.String(1), nullable=True),
            sa.Column('status', sa.String(15), nullable=False, server_default='ACTIVE', index=True),
            sa.Column('data_slots', postgresql.JSONB(), nullable=False, server_default='{}'),
            sa.Column('missing_slots', postgresql.JSONB(), nullable=False, server_default='[]'),
            sa.Column('triggered_by_id', sa.Integer(), sa.ForeignKey('opportunities.id', ondelete='SET NULL'), nullable=True),
            sa.Column('detection_period_start', sa.DateTime(), nullable=False),
            sa.Column('detection_period_end', sa.DateTime(), nullable=False),
            sa.Column('detection_run_id', sa.Integer(), sa.ForeignKey('import_job_executions.id', ondelete='SET NULL'), nullable=True),
            sa.Column('suppression_reason', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column('acknowledged_at', sa.DateTime(), nullable=True),
            sa.Column('resolved_at', sa.DateTime(), nullable=True),
        )

    # Indexes (IF NOT EXISTS)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_opportunities_windfarm_schema "
        "ON opportunities (windfarm_id, schema_code)"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_opportunities_active_unique "
        "ON opportunities (windfarm_id, schema_code) "
        "WHERE status = 'ACTIVE'"
    )


def downgrade() -> None:
    op.drop_index('ix_opportunities_active_unique', table_name='opportunities')
    op.drop_index('ix_opportunities_windfarm_schema', table_name='opportunities')
    op.drop_table('opportunities')

    op.drop_column('ppas', 'has_availability_penalties')
    op.drop_column('ppas', 'ppa_price_eur_mwh')
    op.drop_column('ppas', 'ppa_status')
    op.drop_column('ppas', 'contract_type')
