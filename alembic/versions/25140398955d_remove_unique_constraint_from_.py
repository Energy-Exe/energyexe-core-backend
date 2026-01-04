"""remove unique constraint from generation_units code

Revision ID: 25140398955d
Revises: 594a26e66f65
Create Date: 2026-01-05 00:39:26.486439

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '25140398955d'
down_revision = '594a26e66f65'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the unique index on code
    op.drop_index('ix_generation_units_code', table_name='generation_units')
    # Create a non-unique index for query performance
    op.create_index('ix_generation_units_code', 'generation_units', ['code'], unique=False)


def downgrade() -> None:
    # Drop the non-unique index
    op.drop_index('ix_generation_units_code', table_name='generation_units')
    # Restore the unique index
    op.create_index('ix_generation_units_code', 'generation_units', ['code'], unique=True) 