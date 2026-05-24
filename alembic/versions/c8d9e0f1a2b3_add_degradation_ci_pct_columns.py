"""Add ci_lower_95_pct and ci_upper_95_pct to degradation_results.

Surfaces the relative-to-baseline confidence interval (matching how
`slope_pct_per_year` is presented) so consumers don't have to derive it
from `ci_lower_95 / baseline_cap_pu`.

Revision ID: c8d9e0f1a2b3
Revises: e4a1c83d9b21
Create Date: 2026-05-24
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c8d9e0f1a2b3"
down_revision = "e4a1c83d9b21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "degradation_results",
        sa.Column("ci_lower_95_pct", sa.Numeric(10, 3), nullable=True),
    )
    op.add_column(
        "degradation_results",
        sa.Column("ci_upper_95_pct", sa.Numeric(10, 3), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("degradation_results", "ci_upper_95_pct")
    op.drop_column("degradation_results", "ci_lower_95_pct")
