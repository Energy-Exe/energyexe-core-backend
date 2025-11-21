"""add_report_commentary_table

Revision ID: 60aa528a8964
Revises: c551d9abe526
Create Date: 2025-11-13 14:34:22.688525

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '60aa528a8964'
down_revision = 'c551d9abe526'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create report_commentary table
    op.create_table(
        'report_commentary',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('windfarm_id', sa.Integer(), nullable=False),
        sa.Column('section_type', sa.String(length=50), nullable=False),
        sa.Column('data_snapshot', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('date_range_start', sa.DateTime(), nullable=False),
        sa.Column('date_range_end', sa.DateTime(), nullable=False),
        sa.Column('commentary_text', sa.Text(), nullable=False),
        sa.Column('llm_provider', sa.String(length=20), nullable=False),
        sa.Column('llm_model', sa.String(length=100), nullable=False),
        sa.Column('prompt_template_version', sa.String(length=20), nullable=True, server_default='v1'),
        sa.Column('token_count_input', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('token_count_output', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('generation_cost_usd', sa.Numeric(precision=10, scale=6), nullable=False, server_default='0'),
        sa.Column('generation_duration_seconds', sa.Numeric(precision=8, scale=2), nullable=False, server_default='0'),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='published'),
        sa.Column('version', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('is_current', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes
    op.create_index('ix_report_commentary_id', 'report_commentary', ['id'])
    op.create_index('ix_report_commentary_windfarm_id', 'report_commentary', ['windfarm_id'])
    op.create_index('ix_report_commentary_section_type', 'report_commentary', ['section_type'])
    op.create_index('ix_report_commentary_date_range_start', 'report_commentary', ['date_range_start'])
    op.create_index('ix_report_commentary_date_range_end', 'report_commentary', ['date_range_end'])
    op.create_index('ix_report_commentary_is_current', 'report_commentary', ['is_current'])
    op.create_index('ix_report_commentary_created', 'report_commentary', ['created_at'])
    op.create_index(
        'ix_report_commentary_lookup',
        'report_commentary',
        ['windfarm_id', 'section_type', 'date_range_start', 'date_range_end', 'is_current']
    )


def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_report_commentary_lookup', 'report_commentary')
    op.drop_index('ix_report_commentary_created', 'report_commentary')
    op.drop_index('ix_report_commentary_is_current', 'report_commentary')
    op.drop_index('ix_report_commentary_date_range_end', 'report_commentary')
    op.drop_index('ix_report_commentary_date_range_start', 'report_commentary')
    op.drop_index('ix_report_commentary_section_type', 'report_commentary')
    op.drop_index('ix_report_commentary_windfarm_id', 'report_commentary')
    op.drop_index('ix_report_commentary_id', 'report_commentary')

    # Drop table
    op.drop_table('report_commentary') 