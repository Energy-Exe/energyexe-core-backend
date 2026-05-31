"""Add gap_hours to data_anomalies (DQ-01 missing-generation anomaly).

Issue #109 (M6 · DQ-01 generation-gap detection). The DQ-01 detector logs a
windfarm-level ``missing_generation_data`` anomaly per consecutive missing-data
gap (>= 24h). It records the gap length in a typed integer column so the detector
(and any reporting) can read / aggregate the gap size directly rather than
parsing ``anomaly_metadata``.

This adds a single nullable ``gap_hours`` column to ``data_anomalies``; existing
rows (other anomaly types) keep ``gap_hours = NULL``. No data backfill is needed
and no constraints change. The new ``anomaly_type`` value
``'missing_generation_data'`` is application-layer only (``anomaly_type`` is a
plain ``VARCHAR(100)`` with no DB-level enum / CHECK), so it requires no schema
change.

Revision ID: d4e5f6a7b8c9
Revises: c1f2a3b4d5e6
Create Date: 2026-05-31
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e5f6a7b8c9"
down_revision = "c1f2a3b4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "data_anomalies",
        sa.Column("gap_hours", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("data_anomalies", "gap_hours")
