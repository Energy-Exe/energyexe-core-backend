"""remove unique constraint from generation units code

Revision ID: 7daf40c2a86e
Revises: a1b2c3d4e5f6
Create Date: 2025-10-09 14:14:47.499283

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7daf40c2a86e'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Remove unique constraint from generation_units.code
    # This allows multiple phases of the same windfarm to share the same code
    op.drop_index('ix_generation_units_code', table_name='generation_units')
    # Recreate as non-unique index for performance
    op.create_index('ix_generation_units_code', 'generation_units', ['code'], unique=False)


def downgrade() -> None:
    # Restore unique constraint
    op.drop_index('ix_generation_units_code', table_name='generation_units')
    op.create_index('ix_generation_units_code', 'generation_units', ['code'], unique=True) 