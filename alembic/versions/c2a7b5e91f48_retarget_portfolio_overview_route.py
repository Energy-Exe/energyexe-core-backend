"""retarget portfolio overview template to the new index route id

After splitting the portfolio hub tabs into separate routes, the overview
tab's TanStack route id picks up a trailing slash (it becomes the index
route under the layout). Move the existing template to the new id so
suggestions keep showing on the overview tab.

Revision ID: c2a7b5e91f48
Revises: b8f5a92c4d31
Create Date: 2026-05-14 00:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c2a7b5e91f48"
down_revision = "b8f5a92c4d31"
branch_labels = None
depends_on = None


OLD = "/_protected/portfolios/$portfolioId"
NEW = "/_protected/portfolios/$portfolioId/"


def upgrade() -> None:
    op.execute(
        sa.text(
            "UPDATE agent_question_templates "
            "SET route_path = :new, label = 'Portfolio — Overview' "
            "WHERE route_path = :old"
        ).bindparams(new=NEW, old=OLD)
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            "UPDATE agent_question_templates "
            "SET route_path = :old, label = 'Portfolio' "
            "WHERE route_path = :new"
        ).bindparams(new=NEW, old=OLD)
    )
