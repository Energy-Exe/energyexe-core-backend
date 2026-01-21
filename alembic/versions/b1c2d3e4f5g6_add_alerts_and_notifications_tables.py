"""Add alerts and notifications tables.

Revision ID: b1c2d3e4f5g6
Revises: 48453a155679
Create Date: 2026-01-21 11:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'b1c2d3e4f5g6'
down_revision = '48453a155679'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create enum types using raw SQL with IF NOT EXISTS
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE alertmetric AS ENUM ('capacity_factor', 'generation', 'price', 'capture_rate', 'wind_speed', 'data_quality');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE alertcondition AS ENUM ('above', 'below', 'change_by_percent', 'outside_range');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE alertscope AS ENUM ('specific_windfarm', 'portfolio', 'all_windfarms');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE alertseverity AS ENUM ('low', 'medium', 'high', 'critical');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE alerttriggerstatus AS ENUM ('active', 'acknowledged', 'resolved');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE notificationchannel AS ENUM ('in_app', 'email', 'email_digest');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE notificationstatus AS ENUM ('unread', 'read', 'archived');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)

    # Create alert_rules table
    op.create_table(
        'alert_rules',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('metric', postgresql.ENUM('capacity_factor', 'generation', 'price', 'capture_rate', 'wind_speed', 'data_quality', name='alertmetric', create_type=False), nullable=False),
        sa.Column('condition', postgresql.ENUM('above', 'below', 'change_by_percent', 'outside_range', name='alertcondition', create_type=False), nullable=False),
        sa.Column('threshold_value', sa.Float(), nullable=False),
        sa.Column('threshold_value_upper', sa.Float(), nullable=True),
        sa.Column('scope', postgresql.ENUM('specific_windfarm', 'portfolio', 'all_windfarms', name='alertscope', create_type=False), nullable=False, server_default='all_windfarms'),
        sa.Column('windfarm_id', sa.Integer(), nullable=True),
        sa.Column('portfolio_id', sa.Integer(), nullable=True),
        sa.Column('severity', postgresql.ENUM('low', 'medium', 'high', 'critical', name='alertseverity', create_type=False), nullable=False, server_default='medium'),
        sa.Column('channels', sa.JSON(), nullable=False, server_default='["in_app"]'),
        sa.Column('sustained_minutes', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('is_enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('last_triggered_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['portfolio_id'], ['portfolios.id'], ondelete='CASCADE'),
    )
    op.create_index('ix_alert_rules_id', 'alert_rules', ['id'])
    op.create_index('ix_alert_rules_user_id', 'alert_rules', ['user_id'])
    op.create_index('ix_alert_rules_windfarm_id', 'alert_rules', ['windfarm_id'])
    op.create_index('ix_alert_rules_portfolio_id', 'alert_rules', ['portfolio_id'])

    # Create alert_triggers table
    op.create_table(
        'alert_triggers',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('rule_id', sa.Integer(), nullable=False),
        sa.Column('windfarm_id', sa.Integer(), nullable=False),
        sa.Column('triggered_value', sa.Float(), nullable=False),
        sa.Column('threshold_value', sa.Float(), nullable=False),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('status', postgresql.ENUM('active', 'acknowledged', 'resolved', name='alerttriggerstatus', create_type=False), nullable=False, server_default='active'),
        sa.Column('triggered_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('acknowledged_at', sa.DateTime(), nullable=True),
        sa.Column('resolved_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['rule_id'], ['alert_rules.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ondelete='CASCADE'),
    )
    op.create_index('ix_alert_triggers_id', 'alert_triggers', ['id'])
    op.create_index('ix_alert_triggers_rule_id', 'alert_triggers', ['rule_id'])
    op.create_index('ix_alert_triggers_windfarm_id', 'alert_triggers', ['windfarm_id'])
    op.create_index('ix_alert_triggers_status', 'alert_triggers', ['status'])

    # Create notifications table
    op.create_table(
        'notifications',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('trigger_id', sa.Integer(), nullable=True),
        sa.Column('title', sa.String(255), nullable=False),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('severity', postgresql.ENUM('low', 'medium', 'high', 'critical', name='alertseverity', create_type=False), nullable=False, server_default='medium'),
        sa.Column('notification_type', sa.String(50), nullable=False, server_default='alert'),
        sa.Column('entity_type', sa.String(50), nullable=True),
        sa.Column('entity_id', sa.Integer(), nullable=True),
        sa.Column('channel', postgresql.ENUM('in_app', 'email', 'email_digest', name='notificationchannel', create_type=False), nullable=False, server_default='in_app'),
        sa.Column('status', postgresql.ENUM('unread', 'read', 'archived', name='notificationstatus', create_type=False), nullable=False, server_default='unread'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('read_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['trigger_id'], ['alert_triggers.id'], ondelete='SET NULL'),
    )
    op.create_index('ix_notifications_id', 'notifications', ['id'])
    op.create_index('ix_notifications_user_id', 'notifications', ['user_id'])
    op.create_index('ix_notifications_status', 'notifications', ['status'])
    op.create_index('ix_notifications_trigger_id', 'notifications', ['trigger_id'])

    # Create notification_preferences table
    op.create_table(
        'notification_preferences',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('email_enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('email_digest_enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('in_app_enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('digest_frequency_hours', sa.Integer(), nullable=False, server_default='24'),
        sa.Column('last_digest_sent_at', sa.DateTime(), nullable=True),
        sa.Column('quiet_hours_enabled', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('quiet_hours_start', sa.Integer(), nullable=True),
        sa.Column('quiet_hours_end', sa.Integer(), nullable=True),
        sa.Column('min_severity', postgresql.ENUM('low', 'medium', 'high', 'critical', name='alertseverity', create_type=False), nullable=False, server_default='low'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('user_id', name='uq_user_notification_preferences'),
    )
    op.create_index('ix_notification_preferences_id', 'notification_preferences', ['id'])
    op.create_index('ix_notification_preferences_user_id', 'notification_preferences', ['user_id'])


def downgrade() -> None:
    # Drop tables
    op.drop_table('notification_preferences')
    op.drop_table('notifications')
    op.drop_table('alert_triggers')
    op.drop_table('alert_rules')

    # Drop enum types
    op.execute('DROP TYPE IF EXISTS notificationstatus')
    op.execute('DROP TYPE IF EXISTS notificationchannel')
    op.execute('DROP TYPE IF EXISTS alerttriggerstatus')
    op.execute('DROP TYPE IF EXISTS alertseverity')
    op.execute('DROP TYPE IF EXISTS alertscope')
    op.execute('DROP TYPE IF EXISTS alertcondition')
    op.execute('DROP TYPE IF EXISTS alertmetric')
