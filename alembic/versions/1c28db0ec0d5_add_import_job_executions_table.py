"""add import_job_executions table

Revision ID: 1c28db0ec0d5
Revises: e385cc409e83
Create Date: 2025-10-22 00:05:46.818424

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1c28db0ec0d5'
down_revision = 'e385cc409e83'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create import_job_executions table
    op.create_table(
        'import_job_executions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_name', sa.String(length=100), nullable=False),
        sa.Column('source', sa.String(length=50), nullable=False),
        sa.Column('job_type', sa.String(length=20), nullable=False),
        sa.Column('import_start_date', sa.DateTime(), nullable=False),
        sa.Column('import_end_date', sa.DateTime(), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('duration_seconds', sa.Float(), nullable=True),
        sa.Column('records_imported', sa.Integer(), nullable=False),
        sa.Column('records_updated', sa.Integer(), nullable=False),
        sa.Column('api_calls_made', sa.Integer(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('retry_count', sa.Integer(), nullable=False),
        sa.Column('max_retries', sa.Integer(), nullable=False),
        sa.Column('job_metadata', sa.JSON(), nullable=True),
        sa.Column('created_by_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['created_by_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes
    op.create_index(op.f('ix_import_job_executions_id'), 'import_job_executions', ['id'], unique=False)
    op.create_index(op.f('ix_import_job_executions_job_name'), 'import_job_executions', ['job_name'], unique=False)
    op.create_index(op.f('ix_import_job_executions_source'), 'import_job_executions', ['source'], unique=False)
    op.create_index(op.f('ix_import_job_executions_job_type'), 'import_job_executions', ['job_type'], unique=False)
    op.create_index(op.f('ix_import_job_executions_status'), 'import_job_executions', ['status'], unique=False)
    op.create_index(op.f('ix_import_job_executions_created_at'), 'import_job_executions', ['created_at'], unique=False)
    op.create_index('ix_import_jobs_source_status', 'import_job_executions', ['source', 'status'], unique=False)
    op.create_index('ix_import_jobs_latest', 'import_job_executions', ['job_name', 'started_at'], unique=False)
    op.create_index('ix_import_jobs_recent', 'import_job_executions', ['created_at'], unique=False)


def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_import_jobs_recent', table_name='import_job_executions')
    op.drop_index('ix_import_jobs_latest', table_name='import_job_executions')
    op.drop_index('ix_import_jobs_source_status', table_name='import_job_executions')
    op.drop_index(op.f('ix_import_job_executions_created_at'), table_name='import_job_executions')
    op.drop_index(op.f('ix_import_job_executions_status'), table_name='import_job_executions')
    op.drop_index(op.f('ix_import_job_executions_job_type'), table_name='import_job_executions')
    op.drop_index(op.f('ix_import_job_executions_source'), table_name='import_job_executions')
    op.drop_index(op.f('ix_import_job_executions_job_name'), table_name='import_job_executions')
    op.drop_index(op.f('ix_import_job_executions_id'), table_name='import_job_executions')

    # Drop table
    op.drop_table('import_job_executions') 