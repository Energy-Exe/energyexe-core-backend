"""Create unified generation data schema

Revision ID: unified_generation_001
Revises: 
Create Date: 2024-01-09

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision: str = 'unified_generation_001'
down_revision: Union[str, None] = '175820ad757b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Step 1: Drop old tables and their dependencies
    op.execute("DROP TABLE IF EXISTS entsoe_generation_data CASCADE")
    op.execute("DROP TABLE IF EXISTS elexon_generation_data CASCADE")
    op.execute("DROP TABLE IF EXISTS eia_generation_data CASCADE")
    op.execute("DROP TABLE IF EXISTS taipower_generation_data CASCADE")
    
    # Step 2: Create raw data table
    op.create_table(
        'generation_data_raw',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('source', sa.String(20), nullable=False),
        sa.Column('source_type', sa.String(20), nullable=False, server_default='api'),
        
        # Temporal fields
        sa.Column('period_start', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('period_end', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('period_type', sa.String(20), nullable=True),
        
        # Raw data storage
        sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        
        # Extracted key fields for indexing
        sa.Column('identifier', sa.Text(), nullable=True),
        sa.Column('value_extracted', sa.Numeric(12, 3), nullable=True),
        sa.Column('unit', sa.String(10), nullable=True),
        
        # Metadata
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for raw data
    op.create_index('idx_raw_source', 'generation_data_raw', ['source'])
    op.create_index('idx_raw_period', 'generation_data_raw', ['period_start', 'period_end'])
    op.create_index('idx_raw_identifier', 'generation_data_raw', ['identifier'])
    op.create_index('idx_raw_created', 'generation_data_raw', ['created_at'])
    
    # Step 3: Create processed hourly data table
    op.create_table(
        'generation_data',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text('gen_random_uuid()')),
        sa.Column('hour', sa.TIMESTAMP(timezone=True), nullable=False),
        
        # Relations
        sa.Column('generation_unit_id', sa.Integer(), nullable=True),
        sa.Column('windfarm_id', sa.Integer(), nullable=True),
        
        # Values
        sa.Column('generation_mwh', sa.Numeric(12, 3), nullable=False),
        sa.Column('capacity_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('capacity_factor', sa.Numeric(5, 4), nullable=True),
        
        # Source tracking
        sa.Column('source', sa.String(20), nullable=False),
        sa.Column('source_resolution', sa.String(20), nullable=True),
        sa.Column('raw_data_ids', postgresql.ARRAY(sa.BigInteger()), nullable=True),
        
        # Quality
        sa.Column('quality_flag', sa.String(20), nullable=True),
        sa.Column('quality_score', sa.Numeric(3, 2), nullable=True),
        sa.Column('completeness', sa.Numeric(3, 2), nullable=True),
        
        # Manual override fields
        sa.Column('is_manual_override', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('original_value', sa.Numeric(12, 3), nullable=True),
        sa.Column('override_reason', sa.Text(), nullable=True),
        sa.Column('override_by_id', sa.Integer(), nullable=True),
        sa.Column('override_at', sa.TIMESTAMP(timezone=True), nullable=True),
        
        # Metadata
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        
        sa.ForeignKeyConstraint(['generation_unit_id'], ['generation_units.id'], ),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ),
        sa.ForeignKeyConstraint(['override_by_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('hour', 'generation_unit_id', 'source', name='uq_generation_hour_unit_source')
    )
    
    # Create indexes for generation data
    op.create_index('idx_gen_hour', 'generation_data', ['hour'])
    op.create_index('idx_gen_unit_hour', 'generation_data', ['generation_unit_id', 'hour'])
    op.create_index('idx_gen_windfarm_hour', 'generation_data', ['windfarm_id', 'hour'])
    op.create_index('idx_gen_source', 'generation_data', ['source'])
    
    # Step 4: Create mapping table for identifiers
    op.create_table(
        'generation_unit_mapping',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('source', sa.String(20), nullable=False),
        sa.Column('source_identifier', sa.Text(), nullable=False),
        sa.Column('generation_unit_id', sa.Integer(), nullable=True),
        sa.Column('windfarm_id', sa.Integer(), nullable=True),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        
        sa.ForeignKeyConstraint(['generation_unit_id'], ['generation_units.id'], ),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('source', 'source_identifier', name='uq_source_identifier')
    )
    
    op.create_index('idx_mapping_source_id', 'generation_unit_mapping', ['source', 'source_identifier'])


def downgrade() -> None:
    op.drop_table('generation_unit_mapping')
    op.drop_table('generation_data')
    op.drop_table('generation_data_raw')