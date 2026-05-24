"""Add n_constraint_hours_excluded to degradation_results.

Tracks how many hours were dropped from the OLS sample because they
fell inside an active structural_constraint_flags period. Lets us
report "X hours of Y were excluded as known constraints" alongside
the slope figures.

Revision ID: f3a4b5c6d7e8
Revises: e2f3a4b5c6d7
Create Date: 2026-05-25
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f3a4b5c6d7e8"
down_revision = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "degradation_results",
        sa.Column("n_constraint_hours_excluded", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("degradation_results", "n_constraint_hours_excluded")
