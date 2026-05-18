"""add brain_agent_ro readonly role

Revision ID: b9d8e3a5c2f1
Revises: e9f2c8a4b6d1
Create Date: 2026-04-30 00:00:00.000000

Creates a Postgres role used by the Brain Agent (admin and client profiles)
to connect with SELECT-only privileges. The agent's process env will use this
role's credentials so that any psycopg2 / asyncpg / psql connection it spawns
is grant-restricted at the database layer — server-rejects every INSERT /
UPDATE / DELETE / CREATE / DROP / ALTER / TRUNCATE / COPY regardless of which
client the agent uses.

Password is read from the BRAIN_AGENT_RO_PASSWORD env var. For local dev a
hard-coded fallback is used; production deployments MUST set this env var
before running the migration.
"""
import os

from alembic import op


revision = "b9d8e3a5c2f1"
down_revision = "e9f2c8a4b6d1"
branch_labels = None
depends_on = None


ROLE_NAME = "brain_agent_ro"
DEV_DEFAULT_PASSWORD = "brain_agent_ro_dev"  # local-dev only


def _password() -> str:
    pw = os.environ.get("BRAIN_AGENT_RO_PASSWORD")
    if pw:
        return pw
    return DEV_DEFAULT_PASSWORD


def upgrade() -> None:
    bind = op.get_bind()
    db_name = bind.engine.url.database
    pw = _password().replace("'", "''")  # escape single quotes for SQL literal

    # 1. Create the role (idempotent).
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{ROLE_NAME}') THEN
                CREATE ROLE {ROLE_NAME} WITH LOGIN PASSWORD '{pw}';
            ELSE
                ALTER ROLE {ROLE_NAME} WITH LOGIN PASSWORD '{pw}';
            END IF;
        END
        $$;
        """
    )

    # 2. Defense-in-depth: this role's sessions default to read-only transactions.
    op.execute(f"ALTER ROLE {ROLE_NAME} SET default_transaction_read_only = on;")

    # 3. Grant connect + schema usage.
    op.execute(f'GRANT CONNECT ON DATABASE "{db_name}" TO {ROLE_NAME};')
    op.execute(f"GRANT USAGE ON SCHEMA public TO {ROLE_NAME};")

    # 4. SELECT on every existing public table / view.
    op.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {ROLE_NAME};")
    op.execute(f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {ROLE_NAME};")

    # 5. SELECT on every FUTURE table the migration role creates (so we don't
    #    have to re-run grants after every schema change).
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"GRANT SELECT ON TABLES TO {ROLE_NAME};"
    )
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"GRANT SELECT ON SEQUENCES TO {ROLE_NAME};"
    )


def downgrade() -> None:
    bind = op.get_bind()
    db_name = bind.engine.url.database

    # Reverse default privileges first (must be done by the same role that set them).
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"REVOKE SELECT ON TABLES FROM {ROLE_NAME};"
    )
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"REVOKE SELECT ON SEQUENCES FROM {ROLE_NAME};"
    )

    # Revoke object-level grants.
    op.execute(f"REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM {ROLE_NAME};")
    op.execute(f"REVOKE SELECT ON ALL SEQUENCES IN SCHEMA public FROM {ROLE_NAME};")
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {ROLE_NAME};")
    op.execute(f'REVOKE CONNECT ON DATABASE "{db_name}" FROM {ROLE_NAME};')

    # Finally drop the role. DROP OWNED first to release any dangling references.
    op.execute(f"DROP OWNED BY {ROLE_NAME};")
    op.execute(f"DROP ROLE IF EXISTS {ROLE_NAME};")
