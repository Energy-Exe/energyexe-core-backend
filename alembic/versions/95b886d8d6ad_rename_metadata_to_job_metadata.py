"""rename metadata to job_metadata

Revision ID: 95b886d8d6ad
Revises: 1c28db0ec0d5
Create Date: 2025-10-22 01:15:58.747663

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '95b886d8d6ad'
down_revision = '1c28db0ec0d5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rename column from metadata to job_metadata
    op.alter_column('import_job_executions', 'metadata', new_column_name='job_metadata')


def downgrade() -> None:
    # Rename column back from job_metadata to metadata
    op.alter_column('import_job_executions', 'job_metadata', new_column_name='metadata') 