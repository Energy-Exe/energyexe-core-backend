"""Add curtailment tracking columns to generation_data.

Adds metered_mwh and curtailed_mwh columns to support ELEXON BOAV integration.
This enables tracking actual generation vs grid-delivered generation:
- metered_mwh: What was delivered to the grid (from B1610)
- curtailed_mwh: What was curtailed via accepted bids (from BOAV)
- generation_mwh = metered_mwh + curtailed_mwh (actual production)

Revision ID: c2d3e4f5g6h7
Revises: b1c2d3e4f5g6
Create Date: 2026-01-24 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c2d3e4f5g6h7'
down_revision = 'b1c2d3e4f5g6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add metered_mwh column - what was delivered to the grid
    op.add_column(
        'generation_data',
        sa.Column('metered_mwh', sa.Numeric(12, 3), nullable=True)
    )

    # Add curtailed_mwh column - what was curtailed via accepted bids
    op.add_column(
        'generation_data',
        sa.Column('curtailed_mwh', sa.Numeric(12, 3), nullable=True, server_default='0')
    )

    # Add index for querying curtailment data efficiently
    op.create_index(
        'idx_generation_data_curtailed',
        'generation_data',
        ['source', 'hour'],
        postgresql_where=sa.text('curtailed_mwh > 0')
    )


def downgrade() -> None:
    # Drop the partial index first
    op.drop_index('idx_generation_data_curtailed', table_name='generation_data')

    # Drop the columns
    op.drop_column('generation_data', 'curtailed_mwh')
    op.drop_column('generation_data', 'metered_mwh')
