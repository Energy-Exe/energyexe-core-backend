"""Add roughness field to windfarms table

Revision ID: b3c4d5e6f7g8
Revises: a2b3c4d5e6f7
Create Date: 2025-11-26 14:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b3c4d5e6f7g8"
down_revision = "a2b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add roughness column to windfarms table
    # Values: 0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0
    op.add_column(
        "windfarms",
        sa.Column("roughness", sa.DECIMAL(precision=2, scale=1), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("windfarms", "roughness")
