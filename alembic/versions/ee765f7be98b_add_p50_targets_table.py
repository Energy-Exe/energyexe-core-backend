"""add p50_targets table

Revision ID: ee765f7be98b
Revises: d5d07dd397b8
Create Date: 2026-04-09 18:45:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'ee765f7be98b'
down_revision = 'd5d07dd397b8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'p50_targets',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('windfarm_id', sa.Integer(), nullable=False),
        sa.Column('p50_target_start_date', sa.Date(), nullable=False),
        sa.Column('p50_target_end_date', sa.Date(), nullable=True),
        sa.Column('p50_target_volume_gwh', sa.DECIMAL(precision=12, scale=3), nullable=False),
        sa.Column('source', sa.String(length=500), nullable=True),
        sa.Column('comment', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('windfarm_id', 'p50_target_start_date', name='uq_p50_target_windfarm_start_date'),
    )
    op.create_index(op.f('ix_p50_targets_id'), 'p50_targets', ['id'], unique=False)
    op.create_index(op.f('ix_p50_targets_windfarm_id'), 'p50_targets', ['windfarm_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_p50_targets_windfarm_id'), table_name='p50_targets')
    op.drop_index(op.f('ix_p50_targets_id'), table_name='p50_targets')
    op.drop_table('p50_targets')
