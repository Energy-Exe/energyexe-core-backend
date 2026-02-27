"""add financial entity and financial data tables

Revision ID: bc12ec670649
Revises: f5g6h7i8j9k0
Create Date: 2026-02-27 04:05:20.930661

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'bc12ec670649'
down_revision = 'f5g6h7i8j9k0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('financial_entities',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('code', sa.String(length=100), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('entity_type', sa.String(length=50), nullable=False),
    sa.Column('registration_number', sa.String(length=100), nullable=True),
    sa.Column('country_of_incorporation', sa.String(length=100), nullable=True),
    sa.Column('parent_entity_id', sa.Integer(), nullable=True),
    sa.Column('notes', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['parent_entity_id'], ['financial_entities.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_financial_entities_code'), 'financial_entities', ['code'], unique=True)
    op.create_index(op.f('ix_financial_entities_id'), 'financial_entities', ['id'], unique=False)

    op.create_table('financial_data',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('financial_entity_id', sa.Integer(), nullable=False),
    sa.Column('period_start', sa.Date(), nullable=False),
    sa.Column('period_end', sa.Date(), nullable=False),
    sa.Column('period_length_months', sa.Numeric(precision=4, scale=1), nullable=True),
    sa.Column('currency', sa.String(length=3), nullable=False),
    sa.Column('is_synthetic', sa.Boolean(), nullable=False),
    sa.Column('reported_generation_gwh', sa.Numeric(precision=12, scale=3), nullable=True),
    sa.Column('revenue', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('other_revenue', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('total_revenue', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('cost_of_goods', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('grid_cost', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('land_cost', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('payroll_expenses', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('service_agreements', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('insurance', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('other_operating_expenses', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('total_operating_expenses', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('ebitda', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('depreciation', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('ebit', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('net_interest', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('net_other_financial', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('earnings_before_tax', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('tax', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('net_income', sa.Numeric(precision=15, scale=2), nullable=True),
    sa.Column('extra_line_items', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('comment', sa.Text(), nullable=True),
    sa.Column('source', sa.String(length=100), nullable=True),
    sa.Column('import_job_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['financial_entity_id'], ['financial_entities.id'], ),
    sa.ForeignKeyConstraint(['import_job_id'], ['import_job_executions.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('financial_entity_id', 'period_start', name='uq_financial_data_entity_period')
    )
    op.create_index('ix_financial_data_entity_period', 'financial_data', ['financial_entity_id', 'period_start'], unique=False)
    op.create_index(op.f('ix_financial_data_financial_entity_id'), 'financial_data', ['financial_entity_id'], unique=False)
    op.create_index(op.f('ix_financial_data_id'), 'financial_data', ['id'], unique=False)
    op.create_index('ix_financial_data_period_start', 'financial_data', ['period_start'], unique=False)

    op.create_table('windfarm_financial_entities',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('windfarm_id', sa.Integer(), nullable=False),
    sa.Column('financial_entity_id', sa.Integer(), nullable=False),
    sa.Column('relationship_type', sa.String(length=50), nullable=True),
    sa.Column('notes', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['financial_entity_id'], ['financial_entities.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('windfarm_id', 'financial_entity_id', name='uq_windfarm_financial_entity')
    )
    op.create_index(op.f('ix_windfarm_financial_entities_financial_entity_id'), 'windfarm_financial_entities', ['financial_entity_id'], unique=False)
    op.create_index(op.f('ix_windfarm_financial_entities_id'), 'windfarm_financial_entities', ['id'], unique=False)
    op.create_index(op.f('ix_windfarm_financial_entities_windfarm_id'), 'windfarm_financial_entities', ['windfarm_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_windfarm_financial_entities_windfarm_id'), table_name='windfarm_financial_entities')
    op.drop_index(op.f('ix_windfarm_financial_entities_id'), table_name='windfarm_financial_entities')
    op.drop_index(op.f('ix_windfarm_financial_entities_financial_entity_id'), table_name='windfarm_financial_entities')
    op.drop_table('windfarm_financial_entities')
    op.drop_index('ix_financial_data_period_start', table_name='financial_data')
    op.drop_index(op.f('ix_financial_data_id'), table_name='financial_data')
    op.drop_index(op.f('ix_financial_data_financial_entity_id'), table_name='financial_data')
    op.drop_index('ix_financial_data_entity_period', table_name='financial_data')
    op.drop_table('financial_data')
    op.drop_index(op.f('ix_financial_entities_id'), table_name='financial_entities')
    op.drop_index(op.f('ix_financial_entities_code'), table_name='financial_entities')
    op.drop_table('financial_entities')
