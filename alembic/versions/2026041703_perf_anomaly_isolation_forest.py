"""Add flag_isolation_forest column to performance_anomalies (Module 3b).

Spec item 5.2 — IsolationForest is the optional secondary anomaly layer per
Prioritisation 2026-03-30 Module 3b. The flag is informational only — it
combines into `flag_any_anomaly` semantically (true if EITHER MAD or
IsolationForest flagged) but does NOT influence loss MWh / EUR calculations,
which remain driven by the MAD-statistical layer alone.

Revision ID: 2026041703_iforest
Revises: 2026041702_genconc
Create Date: 2026-04-17
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2026041703_iforest"
down_revision = "2026041702_genconc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "performance_anomalies",
        sa.Column(
            "flag_isolation_forest",
            sa.Boolean(),
            nullable=True,  # NULL = not evaluated; False = evaluated normal; True = flagged
        ),
    )


def downgrade() -> None:
    op.drop_column("performance_anomalies", "flag_isolation_forest")
