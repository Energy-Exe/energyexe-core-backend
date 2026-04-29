# Brain Agent — Read-Only Postgres Role

The Brain Agent (admin and client surfaces, both routed through `/api/v1/brain-agent/*`) connects to Postgres as a dedicated `brain_agent_ro` role with `SELECT`-only grants. This is the database-layer enforcement of "the agent can read but cannot write," sitting beneath the existing `db.py` SQL validator and the system-prompt rules.

---

## Why the role exists

The agent has access to `Bash`. The system prompt explicitly tells it to write Python scripts that connect with raw `psycopg2.connect(os.environ["DATABASE_URL"])` for chart generation and complex analysis. That path bypasses the `db.py` SQL validator entirely. Without a role-level constraint, a buggy or jailbroken agent could `INSERT` / `UPDATE` / `DELETE` against any table.

The role makes that impossible at the database layer. Even if an agent flips the session-level `default_transaction_read_only` GUC off (which it can, since `SET` is not privileged), it still has no `INSERT/UPDATE/DELETE/CREATE` grants — so writes are rejected with `permission denied for table …`.

---

## How it's wired

| Layer | File | What it does |
|---|---|---|
| Migration | `alembic/versions/b9d8e3a5c2f1_add_brain_agent_ro_role.py` | Creates the role, sets `default_transaction_read_only = on` for its sessions, grants `CONNECT` + `USAGE` + `SELECT` on existing and future tables in `public`. Reads password from the `BRAIN_AGENT_RO_PASSWORD` env var; falls back to a dev default for local. Idempotent — re-running rotates the password. |
| Settings | `app/core/config.py` | `BRAIN_AGENT_RO_USER` (default `"brain_agent_ro"`) + `BRAIN_AGENT_RO_PASSWORD` (no default — empty means "not configured"). Property `database_url_agent_ro` builds the read-only DSN by swapping credentials in the existing `DATABASE_URL`. |
| Service | `app/services/brain_agent_service.py` | Agent process env uses `database_url_agent_ro` when configured. Falls back to the main `DATABASE_URL` with a session-level `PGOPTIONS=-c default_transaction_read_only=on` if the role isn't set up yet (logs `brain_agent_ro_role_not_configured` warning). Belt-and-suspenders: even when the role is in use, the same `PGOPTIONS` is still passed. |

Both admin and client agent profiles use the same read-only role.

---

## Local development

The migration runs as part of `make migrate` / `alembic upgrade head`. To make the running backend actually use the role:

```bash
# In energyexe-core-backend/.env (or your local equivalent)
BRAIN_AGENT_RO_PASSWORD=<some-strong-value>
```

If `BRAIN_AGENT_RO_PASSWORD` is unset, the backend falls back to the main DB user with `PGOPTIONS` read-only enforcement. That works but is materially weaker — the agent could disable the GUC at the session level.

After editing `.env`, restart uvicorn (or touch any watched `.py` file) so pydantic-settings re-reads it.

---

## Production deploy

Order matters. The role's password is set **at migration time** from the env var.

1. Choose a strong password (32+ chars; URL-safe is convenient since the value gets embedded in a Postgres connection URL):
   ```bash
   python -c "import secrets; print(secrets.token_urlsafe(24))"
   ```
2. Set `BRAIN_AGENT_RO_PASSWORD` in the production environment **before** running migrations.
3. Run `alembic upgrade head`. The migration's `DO $$ ... CREATE ROLE / ALTER ROLE ... $$` block uses the env var for the password.
4. Confirm the running backend has the same value in its env. The startup log should NOT show `brain_agent_ro_role_not_configured`.

---

## Rotating the password

The migration is idempotent — its `ALTER ROLE` branch picks up a new `BRAIN_AGENT_RO_PASSWORD` if you re-trigger the upgrade. But because the migration is already at head, plain `alembic upgrade head` is a no-op. Two options:

**Option A — Direct ALTER (preferred for live systems).** Run the password change directly, then update env. Less destructive than the alembic cycle.

```python
import psycopg2
from app.core.config import get_settings
conn = psycopg2.connect(get_settings().database_url_sync)
conn.autocommit = True
conn.cursor().execute("ALTER ROLE brain_agent_ro WITH PASSWORD %s", (new_password,))
```

**Option B — alembic downgrade/upgrade.** Cleaner audit trail but drops and recreates the role, terminating active connections. Only use during a planned maintenance window.

```bash
alembic downgrade -1
BRAIN_AGENT_RO_PASSWORD=<new> alembic upgrade head
```

After either path:
- Update `BRAIN_AGENT_RO_PASSWORD` in the deploy env.
- Restart the backend so it picks up the new value.

---

## Verification

The following Python script exercises every dimension of the protection. Run it after migration / rotation to confirm posture:

```python
import psycopg2
from urllib.parse import urlparse, quote
from app.core.config import get_settings

s = get_settings()
parsed = urlparse(s.database_url_sync)
host, port, path = parsed.hostname, f":{parsed.port}" if parsed.port else "", parsed.path
url = f"postgresql://{quote(s.BRAIN_AGENT_RO_USER)}:{quote(s.BRAIN_AGENT_RO_PASSWORD)}@{host}{port}{path}"

conn = psycopg2.connect(url)
cur = conn.cursor()

# 1. role identity + default GUC
cur.execute("SELECT current_user"); print(cur.fetchone()[0])               # brain_agent_ro
cur.execute("SHOW default_transaction_read_only"); print(cur.fetchone()[0]) # on

# 2. SELECT works
cur.execute("SELECT count(*) FROM windfarms"); print(cur.fetchone()[0])

# 3. writes blocked even after disabling read-only
cur.execute("SET default_transaction_read_only = off"); conn.commit()
for sql in [
    "CREATE TABLE public._probe (x int)",
    "INSERT INTO windfarms (id) VALUES (NULL)",
    "UPDATE windfarms SET id = id WHERE id = 1",
    "DELETE FROM windfarms WHERE id = -1",
]:
    try:
        cur.execute(sql); conn.commit()
        print(f"FAIL: {sql} succeeded")
    except Exception as e:
        conn.rollback()
        print(f"OK blocked: {str(e).splitlines()[0]}")

conn.close()
```

Expected: `current_user = brain_agent_ro`, `default_transaction_read_only = on`, every write attempt fails with `permission denied for …` (NOT just `cannot execute … in a read-only transaction` — the latter is the GUC, the former is the grant, which is the real moat).

---

## What the role does NOT protect against

- **Read leaks.** The role can `SELECT` from every table in `public`. The agent's system prompt is what tells it to scope queries to a single client's wind farms; the role itself doesn't enforce per-row visibility. If you need that, layer Postgres Row-Level Security on top — see the original plan file (`brain-agent-distributed-truffle.md`) for the v0 RLS sketch.
- **DDL and writes outside `public`.** The role only has grants on `public`. If we add other schemas (e.g. for partitioned analytics), grants must be repeated for those schemas.
- **Future tables in non-`public` schemas.** The `ALTER DEFAULT PRIVILEGES` clause covers `public` only. Non-`public` schemas need their own `ALTER DEFAULT PRIVILEGES … FOR …`.
- **`CREATE TEMP TABLE`.** Postgres grants temp-table creation to `PUBLIC` by default. The agent can use temp tables freely — that's intentional and harmless (they live in `pg_temp` and disappear when the session ends).

---

## Local-dev password used in this repo's history

The migration was first applied with the dev placeholder `brain_agent_ro_dev`. It has since been rotated to a 32-char URL-safe random value (kept only in the local `.env`, never committed). For your own checkout, generate your own value as described under "Local development" above.
