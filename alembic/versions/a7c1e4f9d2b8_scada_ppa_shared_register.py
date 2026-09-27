"""SCADA PPA register shared per farm, visible to internal users only (EPR-143)

Aje's 2026-09-25 decision: "one official set of terms per farm". EPR-136 had made the register
private per user; this reverses it and moves the boundary to the account:

* ``users.is_internal`` (boolean, NOT NULL, default false) — an EnergyExe staff account. NO backfill:
  the email is user-editable (``PUT /users/me``), so it cannot bootstrap an entitlement. The column
  lands false everywhere and is granted per environment by hand against a vetted id list::

      -- read first, then grant exactly the ids you checked
      SELECT id, username, email, is_superuser FROM users WHERE is_superuser ORDER BY id;
      UPDATE users SET is_internal = true WHERE id IN (<vetted ids>);

  (Staging 2026-09-25: ids 1, 2, 9, 10, 12, 18 are the six @energyexe.com superusers.)
* ``uq_scada_ppa_code_windfarm_user`` → ``uq_scada_ppa_code_windfarm``: the natural key is
  ``(ppa_code, windfarm_id)`` again. The upgrade REFUSES to run while two rows share a pair
  (two creators, or a NULL creator next to a named one) and names the offenders, so nothing is
  silently merged or dropped. Staging carried no duplicates on 2026-09-25.

Revision ID: a7c1e4f9d2b8
Revises: e5b2c9d41a7f
Create Date: 2026-09-25
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "a7c1e4f9d2b8"
down_revision = "e5b2c9d41a7f"
branch_labels = None
depends_on = None

TABLE = "scada_ppa"
UQ_PER_USER = "uq_scada_ppa_code_windfarm_user"
UQ_SHARED = "uq_scada_ppa_code_windfarm"

DUPLICATE_PAIRS_SQL = f"""
SELECT ppa_code, windfarm_id, count(*) AS n
FROM {TABLE}
GROUP BY ppa_code, windfarm_id
HAVING count(*) > 1
ORDER BY ppa_code, windfarm_id
"""


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("is_internal", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    duplicates = op.get_bind().execute(sa.text(DUPLICATE_PAIRS_SQL)).fetchall()
    if duplicates:
        listed = ", ".join(f"({code!r}, windfarm {wf}: {n} rows)" for code, wf, n in duplicates)
        raise RuntimeError(
            "scada_ppa has rows sharing (ppa_code, windfarm_id); the shared register needs one "
            f"row per pair. Resolve by hand, then re-run: {listed}"
        )
    op.drop_constraint(UQ_PER_USER, TABLE, type_="unique")
    op.create_unique_constraint(UQ_SHARED, TABLE, ["ppa_code", "windfarm_id"])


def downgrade() -> None:
    # The per-user key is looser than the shared one, so recreating it cannot fail on data.
    op.drop_constraint(UQ_SHARED, TABLE, type_="unique")
    op.create_unique_constraint(UQ_PER_USER, TABLE, ["ppa_code", "windfarm_id", "created_by_id"])
    op.drop_column("users", "is_internal")
