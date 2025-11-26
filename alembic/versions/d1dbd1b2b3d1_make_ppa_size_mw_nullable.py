"""make_ppa_size_mw_nullable

Revision ID: d1dbd1b2b3d1
Revises: 2c7d10e6de23
Create Date: 2025-11-26 20:31:35.565887

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd1dbd1b2b3d1'
down_revision = '2c7d10e6de23'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column('ppas', 'ppa_size_mw',
               existing_type=sa.NUMERIC(precision=10, scale=2),
               nullable=True)


def downgrade() -> None:
    op.alter_column('ppas', 'ppa_size_mw',
               existing_type=sa.NUMERIC(precision=10, scale=2),
               nullable=False)
