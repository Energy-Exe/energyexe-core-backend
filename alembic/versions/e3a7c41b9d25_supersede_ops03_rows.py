"""opportunities: retire OPS_03 rows (EPR-95)

OPS_03 (misaligned contracting) is switched off in the registry. The agent reads the
opportunities table directly, so rows from earlier runs are marked SUPERSEDED too.
Data only; nothing is deleted.

Revision ID: e3a7c41b9d25
Revises: b4e8d1f2a6c9
Create Date: 2026-11-18
"""

from alembic import op

revision = "e3a7c41b9d25"
down_revision = "b4e8d1f2a6c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE opportunities SET status = 'SUPERSEDED', updated_at = now() "
        "WHERE schema_code = 'OPS_03' AND status <> 'SUPERSEDED'"
    )


def downgrade() -> None:
    # Which rows were ACTIVE or ACKNOWLEDGED before is not recorded; the next
    # detection run recreates findings if OPS_03 is switched back on.
    pass
