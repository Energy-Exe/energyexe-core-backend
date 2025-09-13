"""Drop old generation data tables

Revision ID: drop_old_gen_002
Revises: unified_generation_001
Create Date: 2025-01-09

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'drop_old_gen_002'
down_revision: Union[str, None] = 'unified_generation_001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Drop old generation data tables that have been replaced by unified schema."""
    
    # Drop old generation data tables
    op.drop_table('entsoe_generation_data', if_exists=True)
    op.drop_table('elexon_generation_data', if_exists=True)
    op.drop_table('eia_generation_data', if_exists=True)
    op.drop_table('taipower_generation_data', if_exists=True)
    
    print("✅ Dropped old generation data tables:")
    print("  - entsoe_generation_data")
    print("  - elexon_generation_data")
    print("  - eia_generation_data")
    print("  - taipower_generation_data")


def downgrade() -> None:
    """Recreate old tables (structure only, no data recovery)."""
    
    # Recreate entsoe_generation_data
    op.create_table('entsoe_generation_data',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('area_code', sa.String(), nullable=False),
        sa.Column('production_type', sa.String(), nullable=False),
        sa.Column('value', sa.Float(), nullable=False),
        sa.Column('unit', sa.String(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Recreate elexon_generation_data
    op.create_table('elexon_generation_data',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('settlement_date', sa.Date(), nullable=False),
        sa.Column('settlement_period', sa.Integer(), nullable=False),
        sa.Column('bm_unit', sa.String(), nullable=False),
        sa.Column('metered_volume', sa.Float(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Recreate eia_generation_data
    op.create_table('eia_generation_data',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('plant_code', sa.String(), nullable=False),
        sa.Column('period', sa.String(), nullable=False),
        sa.Column('generation', sa.Float(), nullable=False),
        sa.Column('unit', sa.String(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Recreate taipower_generation_data
    op.create_table('taipower_generation_data',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('update_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('unit_name', sa.String(), nullable=False),
        sa.Column('net_generation', sa.Float(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    print("⚠️ Recreated old table structures (data not recovered)")