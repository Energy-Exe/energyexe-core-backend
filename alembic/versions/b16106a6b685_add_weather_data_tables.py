"""Add weather data tables

Revision ID: b16106a6b685
Revises: dc7801f1612b
Create Date: 2025-11-04 03:37:44.286679

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b16106a6b685'
down_revision = 'dc7801f1612b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create weather_data_raw table
    op.create_table('weather_data_raw',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('source', sa.String(length=20), nullable=False),
    sa.Column('source_type', sa.String(length=20), nullable=False),
    sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
    sa.Column('latitude', sa.Numeric(precision=6, scale=4), nullable=False),
    sa.Column('longitude', sa.Numeric(precision=7, scale=4), nullable=False),
    sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('source', 'latitude', 'longitude', 'timestamp', name='uq_weather_raw_grid_time')
    )
    op.create_index('idx_weather_raw_location', 'weather_data_raw', ['latitude', 'longitude'], unique=False)
    op.create_index('idx_weather_raw_timestamp', 'weather_data_raw', ['timestamp'], unique=False)
    op.create_index(op.f('ix_weather_data_raw_source'), 'weather_data_raw', ['source'], unique=False)
    op.create_index(op.f('ix_weather_data_raw_timestamp'), 'weather_data_raw', ['timestamp'], unique=False)

    # Create weather_data table
    op.create_table('weather_data',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('hour', sa.DateTime(timezone=True), nullable=False),
    sa.Column('windfarm_id', sa.Integer(), nullable=False),
    sa.Column('wind_speed_100m', sa.Numeric(precision=8, scale=3), nullable=False),
    sa.Column('wind_direction_deg', sa.Numeric(precision=5, scale=2), nullable=False),
    sa.Column('temperature_2m_k', sa.Numeric(precision=6, scale=2), nullable=False),
    sa.Column('temperature_2m_c', sa.Numeric(precision=5, scale=2), nullable=False),
    sa.Column('source', sa.String(length=20), nullable=False),
    sa.Column('raw_data_id', sa.BigInteger(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['raw_data_id'], ['weather_data_raw.id'], ),
    sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('hour', 'windfarm_id', 'source', name='uq_weather_hour_windfarm_source')
    )
    op.create_index('idx_weather_windfarm_hour', 'weather_data', ['windfarm_id', 'hour'], unique=False)
    op.create_index(op.f('ix_weather_data_hour'), 'weather_data', ['hour'], unique=False)


def downgrade() -> None:
    # Drop weather_data table
    op.drop_index(op.f('ix_weather_data_hour'), table_name='weather_data')
    op.drop_index('idx_weather_windfarm_hour', table_name='weather_data')
    op.drop_table('weather_data')

    # Drop weather_data_raw table
    op.drop_index(op.f('ix_weather_data_raw_timestamp'), table_name='weather_data_raw')
    op.drop_index(op.f('ix_weather_data_raw_source'), table_name='weather_data_raw')
    op.drop_index('idx_weather_raw_timestamp', table_name='weather_data_raw')
    op.drop_index('idx_weather_raw_location', table_name='weather_data_raw')
    op.drop_table('weather_data_raw') 