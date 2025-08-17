"""add entsoe fetch history table

Revision ID: add_entsoe_fetch_history
Revises: cleanup_entsoe_tables
Create Date: 2025-01-11

"""
# This migration has already been applied, keeping for chain integrity

revision = "add_entsoe_fetch_history"
down_revision = "cleanup_entsoe_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Already applied."""
    pass


def downgrade() -> None:
    """No-op."""
    pass