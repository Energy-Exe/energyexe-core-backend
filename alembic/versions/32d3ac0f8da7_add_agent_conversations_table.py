"""add agent_threads table

Revision ID: 32d3ac0f8da7
Revises: e7c9b2d1f3a8
Create Date: 2026-04-06 04:27:58.718971

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '32d3ac0f8da7'
down_revision = 'e7c9b2d1f3a8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop old agent_conversations table if it exists (from earlier attempt)
    op.execute("DROP TABLE IF EXISTS agent_conversations CASCADE")

    op.create_table('agent_threads',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('title', sa.String(length=255), nullable=True),
        sa.Column('model', sa.String(length=50), nullable=True),
        sa.Column('messages', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('message_count', sa.Integer(), nullable=True),
        sa.Column('total_cost_usd', sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column('total_turns', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_agent_threads_user_id'), 'agent_threads', ['user_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_agent_threads_user_id'), table_name='agent_threads')
    op.drop_table('agent_threads')
