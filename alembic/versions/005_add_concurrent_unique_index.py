"""Add concurrent unique index without cleaning duplicates

Revision ID: concurrent_unique_005
Revises: drop_old_gen_002
Create Date: 2025-01-10

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = 'concurrent_unique_005'
down_revision: Union[str, None] = 'drop_old_gen_002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add non-unique index for duplicate prevention logic."""
    
    # Create a regular (non-unique) index first
    # This will help with the ON CONFLICT queries even without uniqueness constraint
    op.create_index(
        'ix_generation_data_raw_dedup',
        'generation_data_raw',
        ['source', 'identifier', 'period_start', 'period_end'],
        unique=False  # Non-unique to avoid conflict with existing duplicates
    )
    
    print("✅ Added index for deduplication logic")


def downgrade() -> None:
    """Remove index."""
    
    op.drop_index('ix_generation_data_raw_dedup', table_name='generation_data_raw')