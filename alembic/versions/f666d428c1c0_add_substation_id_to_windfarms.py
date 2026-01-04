"""add_substation_id_to_windfarms

Revision ID: f666d428c1c0
Revises: 25140398955d
Create Date: 2026-01-05 01:49:30.525276

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f666d428c1c0'
down_revision = '25140398955d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add substation_id column to windfarms table
    op.add_column('windfarms', sa.Column('substation_id', sa.Integer(), nullable=True))

    # Add foreign key constraint
    op.create_foreign_key(
        'fk_windfarms_substation_id',
        'windfarms',
        'substations',
        ['substation_id'],
        ['id']
    )

    # Add index for better query performance
    op.create_index('ix_windfarms_substation_id', 'windfarms', ['substation_id'])


def downgrade() -> None:
    # Remove index
    op.drop_index('ix_windfarms_substation_id', table_name='windfarms')

    # Remove foreign key constraint
    op.drop_constraint('fk_windfarms_substation_id', 'windfarms', type_='foreignkey')

    # Remove column
    op.drop_column('windfarms', 'substation_id')