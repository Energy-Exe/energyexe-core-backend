"""Add consumption_mwh column to generation_data.

For ENTSOE data, generation per unit reports both 'Actual Aggregated' (generation)
and 'Actual Consumption' values. This column stores the consumption component
so that net generation = generation_mwh - consumption_mwh.

Revision ID: e4f5g6h7i8j9
Revises: d3e4f5g6h7i8
Create Date: 2026-02-12 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e4f5g6h7i8j9'
down_revision = 'd3e4f5g6h7i8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('generation_data',
        sa.Column('consumption_mwh', sa.Numeric(12, 3), nullable=True)
    )


def downgrade() -> None:
    op.drop_column('generation_data', 'consumption_mwh')
