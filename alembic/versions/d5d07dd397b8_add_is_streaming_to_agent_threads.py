"""add is_streaming to agent_threads

Revision ID: d5d07dd397b8
Revises: 32d3ac0f8da7
Create Date: 2026-04-07 18:44:59.357095

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd5d07dd397b8'
down_revision = '32d3ac0f8da7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('agent_threads', sa.Column('is_streaming', sa.Boolean(), nullable=False, server_default=sa.text('false')))


def downgrade() -> None:
    op.drop_column('agent_threads', 'is_streaming')
