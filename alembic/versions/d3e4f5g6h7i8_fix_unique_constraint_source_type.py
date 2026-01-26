"""Fix unique constraint to include source_type.

This allows both BOAV bid and offer records for the same BM unit and time period
to coexist in generation_data_raw table.

Revision ID: d3e4f5g6h7i8
Revises: c2d3e4f5g6h7
Create Date: 2026-01-25 08:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd3e4f5g6h7i8'
down_revision = 'c2d3e4f5g6h7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the old unique constraint/index
    op.drop_constraint(
        'uq_generation_data_raw_source_identifier_period',
        'generation_data_raw',
        type_='unique'
    )

    # Create new unique constraint including source_type
    op.create_unique_constraint(
        'uq_generation_data_raw_source_type_identifier_period',
        'generation_data_raw',
        ['source', 'source_type', 'identifier', 'period_start']
    )


def downgrade() -> None:
    # Drop the new constraint
    op.drop_constraint(
        'uq_generation_data_raw_source_type_identifier_period',
        'generation_data_raw',
        type_='unique'
    )

    # Recreate the old constraint
    op.create_unique_constraint(
        'uq_generation_data_raw_source_identifier_period',
        'generation_data_raw',
        ['source', 'identifier', 'period_start']
    )
