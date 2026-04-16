"""Widen degradation_results numeric columns to handle large intercept values.

OLS regression on year_fraction (e.g. 2020.5) produces intercepts around
-slope * 2020 which can be hundreds; the original Numeric(8,6) only allowed
|value| < 100 and caused insert failures.

Revision ID: d7f91a2b3c4e
Revises: 788b2ee80007
Create Date: 2026-04-16
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d7f91a2b3c4e"
down_revision = "788b2ee80007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("degradation_results", "intercept", type_=sa.Numeric(14, 6), existing_nullable=True)
    op.alter_column("degradation_results", "slope_pct_per_year", type_=sa.Numeric(10, 3), existing_nullable=True)
    op.alter_column("degradation_results", "slope_pu_per_year", type_=sa.Numeric(12, 8), existing_nullable=True)
    op.alter_column("degradation_results", "ci_lower_95", type_=sa.Numeric(12, 8), existing_nullable=True)
    op.alter_column("degradation_results", "ci_upper_95", type_=sa.Numeric(12, 8), existing_nullable=True)


def downgrade() -> None:
    op.alter_column("degradation_results", "ci_upper_95", type_=sa.Numeric(8, 6), existing_nullable=True)
    op.alter_column("degradation_results", "ci_lower_95", type_=sa.Numeric(8, 6), existing_nullable=True)
    op.alter_column("degradation_results", "slope_pu_per_year", type_=sa.Numeric(8, 6), existing_nullable=True)
    op.alter_column("degradation_results", "slope_pct_per_year", type_=sa.Numeric(6, 3), existing_nullable=True)
    op.alter_column("degradation_results", "intercept", type_=sa.Numeric(8, 6), existing_nullable=True)
