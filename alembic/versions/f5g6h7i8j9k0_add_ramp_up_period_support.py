"""Add ramp-up period flagging support.

Adds:
- windfarms.ramp_up_end_date (Date, nullable)
- generation_units.commercial_operational_date (Date, nullable)
- generation_units.ramp_up_end_date (Date, nullable)
- generation_data.is_ramp_up (Boolean, NOT NULL, default FALSE)
- Partial index on generation_data.is_ramp_up WHERE is_ramp_up = TRUE

Revision ID: f5g6h7i8j9k0
Revises: e4f5g6h7i8j9
Create Date: 2026-02-24 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f5g6h7i8j9k0'
down_revision = 'e4f5g6h7i8j9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Windfarm: add ramp_up_end_date
    op.add_column('windfarms',
        sa.Column('ramp_up_end_date', sa.Date(), nullable=True)
    )

    # GenerationUnit: add commercial_operational_date and ramp_up_end_date
    op.add_column('generation_units',
        sa.Column('commercial_operational_date', sa.Date(), nullable=True)
    )
    op.add_column('generation_units',
        sa.Column('ramp_up_end_date', sa.Date(), nullable=True)
    )

    # GenerationData: add is_ramp_up boolean
    op.add_column('generation_data',
        sa.Column('is_ramp_up', sa.Boolean(), nullable=False, server_default='false')
    )

    # Partial index for efficient filtering of ramp-up records
    op.create_index(
        'idx_gen_is_ramp_up',
        'generation_data',
        ['is_ramp_up'],
        postgresql_where=sa.text('is_ramp_up = TRUE')
    )


def downgrade() -> None:
    op.drop_index('idx_gen_is_ramp_up', table_name='generation_data')
    op.drop_column('generation_data', 'is_ramp_up')
    op.drop_column('generation_units', 'ramp_up_end_date')
    op.drop_column('generation_units', 'commercial_operational_date')
    op.drop_column('windfarms', 'ramp_up_end_date')
