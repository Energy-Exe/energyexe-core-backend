"""Scope the SCADA PPA register per user (EPR-136)

Every superuser could see, edit and delete every other user's PPA rows — contract terms are
confidential between colleagues, so the register becomes private to whoever entered each row:

* ``created_by_id`` (FK users.id, nullable, indexed) — the owner. Set by the API on every create
  from now on; back-filled here for existing rows from the audit trail (every create was
  ``@audit_action``-ed with the created rows in ``new_values.items[]`` and the actor in
  ``user_id``). Rows the audit log cannot attribute stay NULL and are visible to nobody until
  reassigned by hand — fail closed, never fail open.
* ``uq_scada_ppa_code_windfarm`` → ``uq_scada_ppa_code_windfarm_user``: with private rows a
  global (ppa_code, windfarm_id) key would reject one user's code against a row they cannot see,
  which is an existence leak. Same shape as b3c7d21f0a94 for reports.

Plain drop + recreate (the table is tiny), inside the one transaction alembic already opens.

Revision ID: e5b2c9d41a7f
Revises: c8f2a15d3b7e
Create Date: 2026-09-13
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e5b2c9d41a7f"
down_revision = "c8f2a15d3b7e"
branch_labels = None
depends_on = None

TABLE = "scada_ppa"
FK_NAME = "fk_scada_ppa_created_by_id_users"
IX_NAME = "ix_scada_ppa_created_by_id"
UQ_OLD = "uq_scada_ppa_code_windfarm"
UQ_NEW = "uq_scada_ppa_code_windfarm_user"

# Earliest CREATE audit row wins per PPA id. ``new_values`` is json (not jsonb) and may be NULL or
# lack ``items`` — the LATERAL simply yields nothing for those.
BACKFILL_SQL = f"""
UPDATE {TABLE} AS p
SET created_by_id = src.user_id
FROM (
    SELECT DISTINCT ON ((i->>'id')::int)
           (i->>'id')::int AS ppa_id,
           a.user_id
    FROM audit_logs AS a
    CROSS JOIN LATERAL json_array_elements(a.new_values->'items') AS i
    WHERE a.resource_type = 'scada_ppa'
      AND a.action = 'CREATE'
      AND a.user_id IS NOT NULL
      AND json_typeof(a.new_values->'items') = 'array'
      AND (i->>'id') ~ '^[0-9]+$'
    ORDER BY (i->>'id')::int, a.created_at ASC
) AS src
WHERE p.id = src.ppa_id
  AND p.created_by_id IS NULL
"""


def upgrade() -> None:
    op.add_column(TABLE, sa.Column("created_by_id", sa.Integer(), nullable=True))
    op.create_foreign_key(FK_NAME, TABLE, "users", ["created_by_id"], ["id"])
    op.create_index(IX_NAME, TABLE, ["created_by_id"], unique=False)
    op.execute(BACKFILL_SQL)
    op.drop_constraint(UQ_OLD, TABLE, type_="unique")
    op.create_unique_constraint(UQ_NEW, TABLE, ["ppa_code", "windfarm_id", "created_by_id"])


def downgrade() -> None:
    # Recreating the global key fails if two users hold the same (code, windfarm) — that is the
    # correct outcome: it means the data can no longer be expressed without ownership.
    op.drop_constraint(UQ_NEW, TABLE, type_="unique")
    op.create_unique_constraint(UQ_OLD, TABLE, ["ppa_code", "windfarm_id"])
    op.drop_index(IX_NAME, table_name=TABLE)
    op.drop_constraint(FK_NAME, TABLE, type_="foreignkey")
    op.drop_column(TABLE, "created_by_id")
