"""add brain_agent_client_ro locked-down readonly role (EPR-59)

Revision ID: c1a2b3c4d5e6
Revises: b8d3f72c5e61
Create Date: 2026-06-22 00:00:00.000000

Creates a SECOND, more restricted Postgres role used by the Brain Agent only for
``source='client'`` sessions. Unlike ``brain_agent_ro`` (SELECT on ALL tables),
this role is granted SELECT on an explicit allowlist of client-appropriate
tables only — no users / audit_logs / agent_threads / import_* / *_raw /
notification tables. Because the client agent's process env connects with this
role, it cannot read internal-table data OR enumerate internal tables via
``information_schema`` (which is privilege-filtered), even if it bypasses db.py
with its own psycopg2 connection.

Residual: pg_catalog remains readable cluster-wide, so table *names* can still
be enumerated via pg_catalog; that vector is handled by the db.py introspection
guard + the client system-prompt refusal. Internal-table *data* and
information_schema enumeration are fully blocked here.

Password is read from BRAIN_AGENT_CLIENT_RO_PASSWORD. Production deployments MUST
set this env var before running the migration; a dev fallback is used otherwise.
"""
import os

from alembic import op


revision = "c1a2b3c4d5e6"
down_revision = "b8d3f72c5e61"
branch_labels = None
depends_on = None


ROLE_NAME = "brain_agent_client_ro"
DEV_DEFAULT_PASSWORD = "brain_agent_client_ro_dev"  # local-dev only

# Explicit allowlist of client-appropriate tables. Anything NOT in this list
# (users, audit_logs, agent_threads, agent_question_templates, invitations,
# notifications*, user_*, import_job_executions, weather_import_jobs, *_raw,
# report_commentary, alert_triggers, alembic_version) is intentionally withheld.
CLIENT_ALLOWED_TABLES = [
    # core business
    "windfarms", "windfarm_owners", "owners",
    "portfolios", "portfolio_items",
    "generation_data", "price_data", "weather_data",
    "financial_data", "financial_entities", "windfarm_financial_entities",
    "ppas",
    # analytics / findings
    "opportunities",
    "power_curve_bins", "performance_anomalies", "performance_summaries",
    "degradation_results", "generation_concentration_summaries",
    "peer_group_aggregates", "constraint_loss_summaries",
    "data_anomalies", "alert_rules",
    "p50_targets", "structural_constraint_flags",
    # turbines
    "turbine_models", "turbine_units", "generation_units", "generation_unit_mapping",
    # lookups / geography / market
    "countries", "regions", "states", "bidzones", "bidzone_countries",
    "market_balance_areas", "control_areas", "exchange_rates",
    "methodology_sections",
    # infrastructure reference
    "projects", "substations", "substation_owners", "cables",
]


def _password() -> str:
    return os.environ.get("BRAIN_AGENT_CLIENT_RO_PASSWORD") or DEV_DEFAULT_PASSWORD


def _array_literal() -> str:
    return "ARRAY[" + ", ".join(f"'{t}'" for t in CLIENT_ALLOWED_TABLES) + "]"


def upgrade() -> None:
    bind = op.get_bind()
    db_name = bind.engine.url.database
    pw = _password().replace("'", "''")

    # 1. Create / update the role (idempotent).
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

    # 2. Defense-in-depth: sessions default to read-only.
    op.execute(f"ALTER ROLE {ROLE_NAME} SET default_transaction_read_only = on;")

    # 3. Connect + schema usage (needed to resolve table names).
    op.execute(f'GRANT CONNECT ON DATABASE "{db_name}" TO {ROLE_NAME};')
    op.execute(f"GRANT USAGE ON SCHEMA public TO {ROLE_NAME};")

    # 4. SELECT on the allowlist ONLY (skip any table not present in this DB).
    #    Deliberately NO `GRANT ON ALL TABLES` and NO ALTER DEFAULT PRIVILEGES —
    #    future tables must be added here explicitly to stay locked down.
    op.execute(
        f"""
        DO $$
        DECLARE t text;
        BEGIN
            FOREACH t IN ARRAY {_array_literal()} LOOP
                IF to_regclass('public.' || t) IS NOT NULL THEN
                    EXECUTE format('GRANT SELECT ON public.%I TO {ROLE_NAME}', t);
                END IF;
            END LOOP;
        END
        $$;
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    db_name = bind.engine.url.database

    op.execute(
        f"""
        DO $$
        DECLARE t text;
        BEGIN
            FOREACH t IN ARRAY {_array_literal()} LOOP
                IF to_regclass('public.' || t) IS NOT NULL THEN
                    EXECUTE format('REVOKE SELECT ON public.%I FROM {ROLE_NAME}', t);
                END IF;
            END LOOP;
        END
        $$;
        """
    )
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {ROLE_NAME};")
    op.execute(f'REVOKE CONNECT ON DATABASE "{db_name}" FROM {ROLE_NAME};')
    op.execute(f"DROP OWNED BY {ROLE_NAME};")
    op.execute(f"DROP ROLE IF EXISTS {ROLE_NAME};")
