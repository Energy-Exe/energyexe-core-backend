"""update_owner_types_to_new_categories

Revision ID: dc7801f1612b
Revises: 94b69633a6db
Create Date: 2025-11-03 16:52:38.766455

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dc7801f1612b'
down_revision = '94b69633a6db'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Update owner types from old categories to new categories:
    Old -> New mappings:
    - private_equity -> institutional_investor
    - utility -> energy
    - oil_and_gas -> energy
    - investment_fund -> institutional_investor
    - NULL remains NULL
    - Any other values -> other
    """
    # Map old types to new types
    op.execute("""
        UPDATE owners
        SET type = CASE
            WHEN type = 'private_equity' THEN 'institutional_investor'
            WHEN type = 'utility' THEN 'energy'
            WHEN type = 'oil_and_gas' THEN 'energy'
            WHEN type = 'investment_fund' THEN 'institutional_investor'
            WHEN type IS NULL THEN NULL
            WHEN type IN ('energy', 'institutional_investor', 'community_investors',
                          'municipality', 'private_individual', 'supply_chain_oem',
                          'other', 'unknown') THEN type
            ELSE 'other'
        END
        WHERE type IS NOT NULL;
    """)


def downgrade() -> None:
    """
    Revert owner types back to old categories:
    New -> Old mappings (best effort):
    - energy -> utility
    - institutional_investor -> private_equity
    - community_investors -> other (no equivalent)
    - municipality -> other (no equivalent)
    - private_individual -> other (no equivalent)
    - supply_chain_oem -> other (no equivalent)
    - other -> other (remains)
    - unknown -> NULL
    """
    op.execute("""
        UPDATE owners
        SET type = CASE
            WHEN type = 'energy' THEN 'utility'
            WHEN type = 'institutional_investor' THEN 'private_equity'
            WHEN type = 'unknown' THEN NULL
            WHEN type IN ('community_investors', 'municipality', 'private_individual',
                          'supply_chain_oem', 'other') THEN 'other'
            ELSE type
        END
        WHERE type IS NOT NULL;
    """) 