"""agent_threads.portal: keep SCADA and Perform chats apart

Nullable: chats saved before this column list under Perform; the admin UI sends no portal.

Revision ID: b4e8d1f2a6c9
Revises: d81a5c2e7f40
Create Date: 2026-11-16
"""

import sqlalchemy as sa

from alembic import op

revision = "b4e8d1f2a6c9"
down_revision = "d81a5c2e7f40"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("agent_threads", sa.Column("portal", sa.String(length=16), nullable=True))


def downgrade() -> None:
    op.drop_column("agent_threads", "portal")
