"""add weather import jobs table

Revision ID: c551d9abe526
Revises: b16106a6b685
Create Date: 2025-11-10 22:30:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'c551d9abe526'
down_revision = 'b16106a6b685'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create weather_import_jobs table
    op.create_table(
        'weather_import_jobs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_name', sa.String(length=100), nullable=False),
        sa.Column('source', sa.String(length=50), nullable=False, server_default='ERA5'),
        sa.Column('import_start_date', sa.DateTime(), nullable=False),
        sa.Column('import_end_date', sa.DateTime(), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='pending'),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('duration_seconds', sa.Float(), nullable=True),
        sa.Column('records_imported', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('files_downloaded', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('files_deleted', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('api_calls_made', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('job_metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('max_retries', sa.Integer(), nullable=False, server_default='3'),
        sa.Column('created_by_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['created_by_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes
    op.create_index(op.f('ix_weather_import_jobs_id'), 'weather_import_jobs', ['id'], unique=False)
    op.create_index(op.f('ix_weather_import_jobs_job_name'), 'weather_import_jobs', ['job_name'], unique=False)
    op.create_index(op.f('ix_weather_import_jobs_status'), 'weather_import_jobs', ['status'], unique=False)
    op.create_index(op.f('ix_weather_import_jobs_created_at'), 'weather_import_jobs', ['created_at'], unique=False)
    op.create_index('ix_weather_jobs_date_range', 'weather_import_jobs', ['import_start_date', 'import_end_date'], unique=False)


def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_weather_jobs_date_range', table_name='weather_import_jobs')
    op.drop_index(op.f('ix_weather_import_jobs_created_at'), table_name='weather_import_jobs')
    op.drop_index(op.f('ix_weather_import_jobs_status'), table_name='weather_import_jobs')
    op.drop_index(op.f('ix_weather_import_jobs_job_name'), table_name='weather_import_jobs')
    op.drop_index(op.f('ix_weather_import_jobs_id'), table_name='weather_import_jobs')

    # Drop table
    op.drop_table('weather_import_jobs')
