"""Scope the reports in-flight uniqueness index per requesting user (EPR-112)

Reports became private to the user who generated them, so the global partial
unique index on (report_type, windfarm_id, portfolio_id) now rejects a second
user's generation for the same target with a 409 whose payload names a report
they cannot open. Adding requested_by_id keeps the duplicate guard — one
in-flight run per user per target — without the cross-user collision.

The reports table is small and the index is partial (PENDING/GENERATING only),
so this is a plain drop + recreate rather than a CONCURRENTLY build.

Revision ID: b3c7d21f0a94
Revises: 8a5f8bec9065
Create Date: 2026-08-20 09:40:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b3c7d21f0a94"
down_revision = "8a5f8bec9065"
branch_labels = None
depends_on = None

INDEX_NAME = "uq_reports_inflight"
WHERE = "status IN ('PENDING', 'GENERATING')"


def upgrade() -> None:
    op.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")
    op.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {INDEX_NAME} ON reports "
        f"(report_type, windfarm_id, portfolio_id, requested_by_id) WHERE {WHERE}"
    )


def downgrade() -> None:
    op.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")
    op.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {INDEX_NAME} ON reports "
        f"(report_type, windfarm_id, portfolio_id) WHERE {WHERE}"
    )
