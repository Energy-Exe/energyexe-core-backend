"""scada_finding_action: asset_code joins the natural key (D-081, EPR-183)

A farm can now publish several assets (Raggovidda RG1 and RG2 are two assets of farm ``raggovidda``), so the
lifecycle key ``(farm, trigger, scope, cls)`` would merge their actions. Every existing row was written against
Hill of Towie's register, the only asset ever published, so it is backfilled ``HOT``; the server default is
dropped afterwards so a new row must name its asset. The downgrade refuses while one old key holds two assets.

Deploy order: the SCADA pipeline migration ``e5b8c2d4f6a1`` (asset_code NOT NULL on the scada tables) first.

Revision ID: d81a5c2e7f40
Revises: a7c1e4f9d2b8
Create Date: 2026-10-14
"""

import sqlalchemy as sa

from alembic import op

revision = "d81a5c2e7f40"
down_revision = "a7c1e4f9d2b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scada_finding_action",
        sa.Column("asset_code", sa.String(length=16), nullable=False, server_default="HOT"),
    )
    op.alter_column("scada_finding_action", "asset_code", server_default=None)
    op.drop_constraint("uq_scada_finding_action_key", "scada_finding_action", type_="unique")
    op.create_unique_constraint(
        "uq_scada_finding_action_key",
        "scada_finding_action",
        ["farm", "asset_code", "trigger", "scope", "cls"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    clash = bind.execute(
        sa.text(
            "SELECT 1 FROM scada_finding_action GROUP BY farm, trigger, scope, cls "
            "HAVING COUNT(DISTINCT asset_code) > 1 LIMIT 1"
        )
    ).scalar()
    if clash is not None:
        raise RuntimeError(
            "scada_finding_action holds one (farm, trigger, scope, cls) key for two assets; "
            "the pre-D-081 key cannot hold them"
        )
    op.drop_constraint("uq_scada_finding_action_key", "scada_finding_action", type_="unique")
    op.create_unique_constraint(
        "uq_scada_finding_action_key", "scada_finding_action", ["farm", "trigger", "scope", "cls"]
    )
    op.drop_column("scada_finding_action", "asset_code")
