"""add scada_finding_action lifecycle table (Phase 7b)

Human lifecycle state (acknowledge/confirm/dismiss/resolve + note + who-acted) for a SCADA Predict
finding. App-authored, written only by the API — a normal public core table like data_anomalies,
keyed by the stable natural key (farm, trigger, scope, cls) rather than the volatile register id.

Revision ID: 7b1a9c3d5e02
Revises: c4d8e1f2a3b5
Create Date: 2026-08-30
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "7b1a9c3d5e02"
down_revision = "c4d8e1f2a3b5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scada_finding_action",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("farm", sa.String(length=64), nullable=False),
        sa.Column("trigger", sa.String(length=48), server_default="", nullable=False),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("cls", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(), nullable=True),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.Column("updated_by", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("farm", "trigger", "scope", "cls", name="uq_scada_finding_action_key"),
    )
    op.create_index(
        op.f("ix_scada_finding_action_id"), "scada_finding_action", ["id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_scada_finding_action_id"), table_name="scada_finding_action")
    op.drop_table("scada_finding_action")
