"""EPR-143 migration ``a7c1e4f9d2b8`` against an ISOLATED PostgreSQL (never the shared staging DB).

Skipped unless ``EPR143_PG_ADMIN_URL`` names a local admin URL, e.g. after::

    docker run -d --name epr143-pg -p 55432:5432 -e POSTGRES_PASSWORD=epr143 postgres:16-alpine
    EPR143_PG_ADMIN_URL=postgresql://postgres:epr143@127.0.0.1:55432/postgres poetry run pytest tests/test_scada_ppa_migration_postgres.py

Each test creates a throw-away database in the PRE-migration shape (per-user key, no
``users.is_internal``), stamps alembic at ``e5b2c9d41a7f`` and runs the real migration through the
alembic CLI with ``DATABASE_URL`` pointed at the scratch database.
"""

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

BACKEND = Path(__file__).resolve().parents[1]
ADMIN_URL = os.environ.get("EPR143_PG_ADMIN_URL")

pytestmark = pytest.mark.skipif(
    not ADMIN_URL, reason="EPR143_PG_ADMIN_URL not set (isolated Postgres only)"
)

PRE_MIGRATION_DDL = """
CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE,
                    username VARCHAR(100) NOT NULL UNIQUE, is_superuser BOOLEAN NOT NULL DEFAULT false);
CREATE TABLE windfarms (id SERIAL PRIMARY KEY, code VARCHAR(50), name VARCHAR(255));
CREATE TABLE scada_ppa (
    id SERIAL PRIMARY KEY,
    windfarm_id INTEGER NOT NULL REFERENCES windfarms(id),
    ppa_code VARCHAR(50) NOT NULL,
    ppa_buyer VARCHAR(255) NOT NULL,
    ppa_status VARCHAR(20) NOT NULL DEFAULT 'Draft',
    created_by_id INTEGER REFERENCES users(id),
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_scada_ppa_code_windfarm_user UNIQUE (ppa_code, windfarm_id, created_by_id)
);
CREATE INDEX ix_scada_ppa_created_by_id ON scada_ppa (created_by_id);
INSERT INTO users (id, email, username, is_superuser) VALUES (7, 'a@x', 'a', true), (8, 'b@x', 'b', true);
INSERT INTO windfarms (id, code, name) VALUES (42, 'WF', 'Farm');
"""


def _alembic(direction: str, target: str, database_url: str) -> subprocess.CompletedProcess:
    # conftest exports TESTING=true, which makes Settings.database_url_async return SQLite regardless
    # of DATABASE_URL. Pin it to "false" explicitly rather than dropping the key: a repo `.env`
    # (loaded by Settings) could set TESTING=true again and the subprocess would silently run
    # the migration against SQLite ("Context impl SQLiteImpl").
    env = {**os.environ, "TESTING": "false", "DATABASE_URL": database_url}
    return subprocess.run(
        [sys.executable, "-m", "alembic", direction, target],
        cwd=BACKEND,
        env=env,
        text=True,
        capture_output=True,
    )


@pytest.fixture
def scratch_db():
    """A fresh database in the pre-migration shape, stamped at the previous head."""
    url = make_url(ADMIN_URL)
    assert url.host in ("127.0.0.1", "localhost") and url.database == "postgres"
    admin = create_engine(url.set(drivername="postgresql+psycopg2"), isolation_level="AUTOCOMMIT")
    name = "epr143_mig_" + uuid.uuid4().hex[:12]
    with admin.connect() as c:
        c.execute(text(f'CREATE DATABASE "{name}"'))
    scratch = url.set(drivername="postgresql+psycopg2", database=name)
    engine = create_engine(scratch)
    try:
        with engine.begin() as c:
            for stmt in [s for s in PRE_MIGRATION_DDL.split(";") if s.strip()]:
                c.execute(text(stmt))
        async_url = scratch.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        stamped = _alembic("stamp", "e5b2c9d41a7f", async_url)
        assert stamped.returncode == 0, stamped.stderr
        yield engine, async_url
    finally:
        engine.dispose()
        with admin.connect() as c:
            c.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def _unique_names(engine) -> set[str]:
    return {u["name"] for u in inspect(engine).get_unique_constraints("scada_ppa")}


def _seed(engine, rows):
    with engine.begin() as c:
        for row in rows:
            c.execute(
                text(
                    "INSERT INTO scada_ppa (ppa_code, windfarm_id, ppa_buyer, ppa_status, created_by_id) "
                    "VALUES (:code, :wf, 'Buyer', 'Active', :who)"
                ),
                row,
            )


def test_upgrade_refuses_two_creators_sharing_a_pair_and_names_them(scratch_db):
    engine, url = scratch_db
    _seed(engine, [dict(code="PPA-1", wf=42, who=7), dict(code="PPA-1", wf=42, who=8)])
    result = _alembic("upgrade", "head", url)
    assert result.returncode != 0
    assert (
        "sharing (ppa_code, windfarm_id)" in result.stderr
        and "'PPA-1', windfarm 42: 2 rows" in result.stderr
    )
    # nothing changed: still the per-user key, no is_internal column
    assert _unique_names(engine) == {"uq_scada_ppa_code_windfarm_user"}
    assert "is_internal" not in {c["name"] for c in inspect(engine).get_columns("users")}


def test_upgrade_refuses_a_null_creator_next_to_a_named_one(scratch_db):
    engine, url = scratch_db
    _seed(engine, [dict(code="PPA-1", wf=42, who=7), dict(code="PPA-1", wf=42, who=None)])
    result = _alembic("upgrade", "head", url)
    assert result.returncode != 0 and "'PPA-1', windfarm 42: 2 rows" in result.stderr
    assert _unique_names(engine) == {"uq_scada_ppa_code_windfarm_user"}


def test_clean_data_upgrades_then_the_shared_key_fires_then_downgrades(scratch_db):
    engine, url = scratch_db
    _seed(
        engine,
        [
            dict(code="PPA-1", wf=42, who=7),
            dict(code="PPA-2", wf=42, who=8),
            dict(code="PPA-3", wf=42, who=None),
        ],
    )
    up = _alembic("upgrade", "head", url)
    assert up.returncode == 0, up.stderr
    assert _unique_names(engine) == {"uq_scada_ppa_code_windfarm"}
    users_cols = {c["name"]: c for c in inspect(engine).get_columns("users")}
    assert "is_internal" in users_cols and not users_cols["is_internal"]["nullable"]
    with engine.connect() as c:
        assert (
            c.execute(text("SELECT bool_or(is_internal) FROM users")).scalar() is False
        ), "no backfill"
        assert c.execute(text("SELECT version_num FROM alembic_version")).scalar() == "a7c1e4f9d2b8"
    # (d) the shared key now refuses a second row for the pair, whoever enters it
    with pytest.raises(IntegrityError, match="uq_scada_ppa_code_windfarm"):
        _seed(engine, [dict(code="PPA-1", wf=42, who=8)])
    with pytest.raises(IntegrityError, match="uq_scada_ppa_code_windfarm"):
        _seed(engine, [dict(code="PPA-3", wf=42, who=None)])
    down = _alembic("downgrade", "-1", url)
    assert down.returncode == 0, down.stderr
    assert _unique_names(engine) == {"uq_scada_ppa_code_windfarm_user"}
    assert "is_internal" not in {c["name"] for c in inspect(engine).get_columns("users")}
    with engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM scada_ppa")).scalar() == 3
