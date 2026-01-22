"""add_client_portal_auth_system

Revision ID: a1b2c3d4e5f7
Revises: f666d428c1c0
Create Date: 2026-01-21 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f7'
down_revision = 'f666d428c1c0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add new columns to users table for client portal authentication

    # Role column (admin or client)
    op.add_column('users', sa.Column('role', sa.String(50), nullable=True))

    # Approval fields
    op.add_column('users', sa.Column('is_approved', sa.Boolean(), nullable=True))
    op.add_column('users', sa.Column('approved_at', sa.DateTime(), nullable=True))
    op.add_column('users', sa.Column('approved_by_id', sa.Integer(), nullable=True))

    # Email verification fields
    op.add_column('users', sa.Column('email_verified', sa.Boolean(), nullable=True))
    op.add_column('users', sa.Column('email_verification_token', sa.String(255), nullable=True))
    op.add_column('users', sa.Column('email_verification_sent_at', sa.DateTime(), nullable=True))

    # Additional client fields
    op.add_column('users', sa.Column('company_name', sa.String(255), nullable=True))
    op.add_column('users', sa.Column('phone', sa.String(50), nullable=True))

    # Password reset fields
    op.add_column('users', sa.Column('password_reset_token', sa.String(255), nullable=True))
    op.add_column('users', sa.Column('password_reset_sent_at', sa.DateTime(), nullable=True))

    # Add foreign key for approved_by_id
    op.create_foreign_key(
        'fk_users_approved_by_id',
        'users',
        'users',
        ['approved_by_id'],
        ['id']
    )

    # 2. Migrate existing users to be admins with full access
    # Set role='admin', is_approved=True, email_verified=True for existing users
    op.execute("""
        UPDATE users
        SET role = 'admin',
            is_approved = true,
            email_verified = true
        WHERE role IS NULL
    """)

    # 3. Make role and boolean columns non-nullable with defaults
    op.alter_column('users', 'role',
                    existing_type=sa.String(50),
                    nullable=False,
                    server_default='client')
    op.alter_column('users', 'is_approved',
                    existing_type=sa.Boolean(),
                    nullable=False,
                    server_default=sa.text('false'))
    op.alter_column('users', 'email_verified',
                    existing_type=sa.Boolean(),
                    nullable=False,
                    server_default=sa.text('false'))

    # 4. Create invitations table
    op.create_table(
        'invitations',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('email', sa.String(255), nullable=False, index=True),
        sa.Column('token', sa.String(255), nullable=False, unique=True, index=True),
        sa.Column('invited_by_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('expires_at', sa.DateTime(), nullable=False),
        sa.Column('used_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, default=datetime.utcnow),
    )

    # 5. Create user_features table
    op.create_table(
        'user_features',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('feature_key', sa.String(100), nullable=False, index=True),
        sa.Column('enabled', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, default=datetime.utcnow),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.UniqueConstraint('user_id', 'feature_key', name='uq_user_feature'),
    )


def downgrade() -> None:
    # Drop user_features table
    op.drop_table('user_features')

    # Drop invitations table
    op.drop_table('invitations')

    # Remove foreign key for approved_by_id
    op.drop_constraint('fk_users_approved_by_id', 'users', type_='foreignkey')

    # Remove columns from users table
    op.drop_column('users', 'password_reset_sent_at')
    op.drop_column('users', 'password_reset_token')
    op.drop_column('users', 'phone')
    op.drop_column('users', 'company_name')
    op.drop_column('users', 'email_verification_sent_at')
    op.drop_column('users', 'email_verification_token')
    op.drop_column('users', 'email_verified')
    op.drop_column('users', 'approved_by_id')
    op.drop_column('users', 'approved_at')
    op.drop_column('users', 'is_approved')
    op.drop_column('users', 'role')
