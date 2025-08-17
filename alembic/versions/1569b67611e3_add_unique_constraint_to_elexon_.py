"""add unique constraint to elexon generation data

Revision ID: 1569b67611e3
Revises: b1ec56c9e900
Create Date: 2025-08-17 20:09:20.820380

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1569b67611e3'
down_revision = 'b1ec56c9e900'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create unique constraint on elexon_generation_data table
    # This constraint ensures uniqueness of timestamp, bm_unit, and settlement_period combination
    # Required for ON CONFLICT clause in batch upsert operations
    op.create_unique_constraint(
        'elexon_generation_data_unique',
        'elexon_generation_data',
        ['timestamp', 'bm_unit', 'settlement_period']
    )


def downgrade() -> None:
    # Drop the unique constraint
    op.drop_constraint('elexon_generation_data_unique', 'elexon_generation_data', type_='unique') 