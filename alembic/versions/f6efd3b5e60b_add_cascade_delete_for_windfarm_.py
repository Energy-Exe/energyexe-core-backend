"""Add cascade delete for windfarm relationships

Revision ID: f6efd3b5e60b
Revises: bbf9e212aca5
Create Date: 2025-07-30 20:42:29.454713

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "f6efd3b5e60b"
down_revision = "bbf9e212aca5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop existing foreign key constraints
    op.drop_constraint("turbine_units_windfarm_id_fkey", "turbine_units", type_="foreignkey")
    op.drop_constraint("windfarm_owners_windfarm_id_fkey", "windfarm_owners", type_="foreignkey")

    # Recreate with CASCADE DELETE
    op.create_foreign_key(
        "turbine_units_windfarm_id_fkey",
        "turbine_units",
        "windfarms",
        ["windfarm_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_foreign_key(
        "windfarm_owners_windfarm_id_fkey",
        "windfarm_owners",
        "windfarms",
        ["windfarm_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # Drop CASCADE constraints
    op.drop_constraint("turbine_units_windfarm_id_fkey", "turbine_units", type_="foreignkey")
    op.drop_constraint("windfarm_owners_windfarm_id_fkey", "windfarm_owners", type_="foreignkey")

    # Recreate without CASCADE
    op.create_foreign_key(
        "turbine_units_windfarm_id_fkey", "turbine_units", "windfarms", ["windfarm_id"], ["id"]
    )

    op.create_foreign_key(
        "windfarm_owners_windfarm_id_fkey", "windfarm_owners", "windfarms", ["windfarm_id"], ["id"]
    )
