"""add start_date and end_date to turbine_units

Revision ID: a1b2c3d4e5f6
Revises: 1dde591b6ee0
Create Date: 2025-10-01 03:17:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '1dde591b6ee0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add start_date and end_date columns to turbine_units
    op.add_column('turbine_units', sa.Column('start_date', sa.Date(), nullable=True))
    op.add_column('turbine_units', sa.Column('end_date', sa.Date(), nullable=True))


def downgrade() -> None:
    # Remove start_date and end_date columns from turbine_units
    op.drop_column('turbine_units', 'end_date')
    op.drop_column('turbine_units', 'start_date')
