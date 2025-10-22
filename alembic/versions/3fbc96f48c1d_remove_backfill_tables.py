"""remove backfill tables

Revision ID: 3fbc96f48c1d
Revises: 95b886d8d6ad
Create Date: 2025-10-22 02:54:05.354648

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3fbc96f48c1d'
down_revision = '95b886d8d6ad'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop backfill tables (Celery-based system removed)
    op.drop_table('backfill_tasks')
    op.drop_table('backfill_jobs')


def downgrade() -> None:
    # Note: Cannot recreate tables as models no longer exist
    # This is a one-way migration (backfill system removed)
    pass 