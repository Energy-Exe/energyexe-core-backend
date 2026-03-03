"""add exchange_rates table

Revision ID: e7c9b2d1f3a8
Revises: bc12ec670649
Create Date: 2026-02-28 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'e7c9b2d1f3a8'
down_revision = 'bc12ec670649'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'exchange_rates',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('base_currency', sa.String(length=3), nullable=False),
        sa.Column('quote_currency', sa.String(length=3), nullable=False),
        sa.Column('rate_date', sa.Date(), nullable=False),
        sa.Column('rate', sa.Numeric(precision=12, scale=6), nullable=False),
        sa.Column('inverse_rate', sa.Numeric(precision=12, scale=6), nullable=False),
        sa.Column('source', sa.String(length=50), nullable=False, server_default='ECB'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('base_currency', 'quote_currency', 'rate_date', name='uq_exchange_rate_pair_date'),
    )
    op.create_index('ix_exchange_rates_id', 'exchange_rates', ['id'])
    op.create_index('ix_exchange_rate_quote_date', 'exchange_rates', ['quote_currency', 'rate_date'])


def downgrade() -> None:
    op.drop_index('ix_exchange_rate_quote_date', table_name='exchange_rates')
    op.drop_index('ix_exchange_rates_id', table_name='exchange_rates')
    op.drop_table('exchange_rates')
