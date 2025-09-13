"""Add celery_task_id fields to backfill models

Revision ID: add_celery_task_id
Revises: cd816eef612f
Create Date: 2024-01-10 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_celery_task_id'
down_revision = 'cd816eef612f'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add celery_task_id to backfill_jobs table
    op.add_column('backfill_jobs', 
        sa.Column('celery_task_id', sa.String(255), nullable=True)
    )
    op.create_index('ix_backfill_jobs_celery_task_id', 'backfill_jobs', ['celery_task_id'])
    
    # Add celery_task_id to backfill_tasks table
    op.add_column('backfill_tasks',
        sa.Column('celery_task_id', sa.String(255), nullable=True)
    )
    op.create_index('ix_backfill_tasks_celery_task_id', 'backfill_tasks', ['celery_task_id'])


def downgrade() -> None:
    # Remove indexes
    op.drop_index('ix_backfill_tasks_celery_task_id', 'backfill_tasks')
    op.drop_index('ix_backfill_jobs_celery_task_id', 'backfill_jobs')
    
    # Remove columns
    op.drop_column('backfill_tasks', 'celery_task_id')
    op.drop_column('backfill_jobs', 'celery_task_id')