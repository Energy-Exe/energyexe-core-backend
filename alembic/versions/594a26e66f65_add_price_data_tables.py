"""add price data tables

Revision ID: 594a26e66f65
Revises: d1dbd1b2b3d1
Create Date: 2025-11-29 03:15:37.008084

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '594a26e66f65'
down_revision = 'd1dbd1b2b3d1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create price_data_raw table
    op.create_table('price_data_raw',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('source', sa.String(length=20), nullable=False),
        sa.Column('source_type', sa.String(length=20), nullable=False),
        sa.Column('price_type', sa.String(length=20), nullable=False),
        sa.Column('period_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('period_end', sa.DateTime(timezone=True), nullable=True),
        sa.Column('period_type', sa.String(length=20), nullable=True),
        sa.Column('identifier', sa.Text(), nullable=False),
        sa.Column('value_extracted', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('unit', sa.String(length=20), nullable=True),
        sa.Column('currency', sa.String(length=3), nullable=True),
        sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('source', 'identifier', 'period_start', 'price_type', name='uq_price_raw_source_identifier_period_type')
    )
    op.create_index('idx_price_raw_identifier', 'price_data_raw', ['identifier'], unique=False)
    op.create_index('idx_price_raw_period', 'price_data_raw', ['period_start', 'period_end'], unique=False)
    op.create_index('idx_price_raw_source_period', 'price_data_raw', ['source', 'period_start'], unique=False)
    op.create_index(op.f('ix_price_data_raw_identifier'), 'price_data_raw', ['identifier'], unique=False)
    op.create_index(op.f('ix_price_data_raw_price_type'), 'price_data_raw', ['price_type'], unique=False)
    op.create_index(op.f('ix_price_data_raw_source'), 'price_data_raw', ['source'], unique=False)

    # Create price_data table
    op.create_table('price_data',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('hour', sa.DateTime(timezone=True), nullable=False),
        sa.Column('windfarm_id', sa.Integer(), nullable=False),
        sa.Column('bidzone_id', sa.Integer(), nullable=True),
        sa.Column('day_ahead_price', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('intraday_price', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('currency', sa.String(length=3), nullable=False),
        sa.Column('source', sa.String(length=20), nullable=False),
        sa.Column('raw_data_ids', postgresql.ARRAY(sa.BigInteger()), nullable=True),
        sa.Column('quality_flag', sa.String(length=20), nullable=True),
        sa.Column('quality_score', sa.Numeric(precision=3, scale=2), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['bidzone_id'], ['bidzones.id'], ),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('hour', 'windfarm_id', 'source', name='uq_price_hour_windfarm_source')
    )
    op.create_index('idx_price_bidzone_hour', 'price_data', ['bidzone_id', 'hour'], unique=False)
    op.create_index('idx_price_hour_range', 'price_data', ['hour'], unique=False)
    op.create_index('idx_price_windfarm_hour', 'price_data', ['windfarm_id', 'hour'], unique=False)
    op.create_index(op.f('ix_price_data_hour'), 'price_data', ['hour'], unique=False)


def downgrade() -> None:
    # Drop price_data table
    op.drop_index(op.f('ix_price_data_hour'), table_name='price_data')
    op.drop_index('idx_price_windfarm_hour', table_name='price_data')
    op.drop_index('idx_price_hour_range', table_name='price_data')
    op.drop_index('idx_price_bidzone_hour', table_name='price_data')
    op.drop_table('price_data')

    # Drop price_data_raw table
    op.drop_index(op.f('ix_price_data_raw_source'), table_name='price_data_raw')
    op.drop_index(op.f('ix_price_data_raw_price_type'), table_name='price_data_raw')
    op.drop_index(op.f('ix_price_data_raw_identifier'), table_name='price_data_raw')
    op.drop_index('idx_price_raw_source_period', table_name='price_data_raw')
    op.drop_index('idx_price_raw_period', table_name='price_data_raw')
    op.drop_index('idx_price_raw_identifier', table_name='price_data_raw')
    op.drop_table('price_data_raw')
