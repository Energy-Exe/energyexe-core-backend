"""add portfolio and favorites tables

Revision ID: 48453a155679
Revises: a1b2c3d4e5f7
Create Date: 2026-01-21 10:40:27.651567

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '48453a155679'
down_revision = 'a1b2c3d4e5f7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create the enum type if it doesn't exist
    # Using raw SQL to check and create conditionally
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE portfoliotype AS ENUM ('watchlist', 'owned', 'competitor', 'custom');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)

    # Create portfolios table
    op.create_table(
        'portfolios',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('portfolio_type', sa.Enum('watchlist', 'owned', 'competitor', 'custom', name='portfoliotype', create_type=False), nullable=False),
        sa.Column('is_default', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_portfolios_id'), 'portfolios', ['id'], unique=False)
    op.create_index(op.f('ix_portfolios_user_id'), 'portfolios', ['user_id'], unique=False)

    # Create portfolio_items table
    op.create_table(
        'portfolio_items',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('portfolio_id', sa.Integer(), nullable=False),
        sa.Column('windfarm_id', sa.Integer(), nullable=False),
        sa.Column('added_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['portfolio_id'], ['portfolios.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('portfolio_id', 'windfarm_id', name='uq_portfolio_windfarm')
    )
    op.create_index(op.f('ix_portfolio_items_id'), 'portfolio_items', ['id'], unique=False)
    op.create_index(op.f('ix_portfolio_items_portfolio_id'), 'portfolio_items', ['portfolio_id'], unique=False)
    op.create_index(op.f('ix_portfolio_items_windfarm_id'), 'portfolio_items', ['windfarm_id'], unique=False)

    # Create user_favorites table
    op.create_table(
        'user_favorites',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('windfarm_id', sa.Integer(), nullable=False),
        sa.Column('added_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.ForeignKeyConstraint(['windfarm_id'], ['windfarms.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'windfarm_id', name='uq_user_windfarm_favorite')
    )
    op.create_index(op.f('ix_user_favorites_id'), 'user_favorites', ['id'], unique=False)
    op.create_index(op.f('ix_user_favorites_user_id'), 'user_favorites', ['user_id'], unique=False)
    op.create_index(op.f('ix_user_favorites_windfarm_id'), 'user_favorites', ['windfarm_id'], unique=False)


def downgrade() -> None:
    # Drop user_favorites table
    op.drop_index(op.f('ix_user_favorites_windfarm_id'), table_name='user_favorites')
    op.drop_index(op.f('ix_user_favorites_user_id'), table_name='user_favorites')
    op.drop_index(op.f('ix_user_favorites_id'), table_name='user_favorites')
    op.drop_table('user_favorites')

    # Drop portfolio_items table
    op.drop_index(op.f('ix_portfolio_items_windfarm_id'), table_name='portfolio_items')
    op.drop_index(op.f('ix_portfolio_items_portfolio_id'), table_name='portfolio_items')
    op.drop_index(op.f('ix_portfolio_items_id'), table_name='portfolio_items')
    op.drop_table('portfolio_items')

    # Drop portfolios table
    op.drop_index(op.f('ix_portfolios_user_id'), table_name='portfolios')
    op.drop_index(op.f('ix_portfolios_id'), table_name='portfolios')
    op.drop_table('portfolios')

    # Drop the enum type
    op.execute('DROP TYPE IF EXISTS portfoliotype')
