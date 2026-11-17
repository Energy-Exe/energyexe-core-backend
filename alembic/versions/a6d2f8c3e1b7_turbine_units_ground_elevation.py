"""turbine_units.ground_elevation_m: height above sea level of the turbine base (EPR-96)

Nullable; 0 (sea level) and negative values are valid.

Revision ID: a6d2f8c3e1b7
Revises: e3a7c41b9d25
Create Date: 2026-11-18
"""

import sqlalchemy as sa

from alembic import op

revision = "a6d2f8c3e1b7"
down_revision = "e3a7c41b9d25"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "turbine_units",
        sa.Column("ground_elevation_m", sa.Numeric(precision=7, scale=2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("turbine_units", "ground_elevation_m")
