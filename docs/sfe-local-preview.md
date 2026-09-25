# Isolated SFE preview API

This application only serves the immutable measured-data runs published by the
pipeline into the local `energyexe_sfe_preview` PostgreSQL database. It neither
creates tables nor loads data. There are no scheduled jobs, email, agents, AWS
clients, normal application startup hooks, or ordinary `.env` loading.

Start from this backend checkout, supplying the local PostgreSQL username:

```sh
SFE_PREVIEW_DATABASE_URL=postgresql+asyncpg://YOUR_LOCAL_USER@127.0.0.1:5432/energyexe_sfe_preview \
  .venv/bin/python scripts/start_sfe_preview.py
```

The runner launches a fresh process with only basic OS and named preview settings.
It drops HOME and service credential variables. Configuration accepts only literal
loopback database hosts, this exact database name, and no connection query options.
It defaults to loopback port 8012 and fails if occupied. The API's database sessions
are transaction-level read-only. The UI origin allowlist is loopback port 3016.

Use the local fixture `admin` / `adminenergyexe` at `POST /api/v1/auth/login`.
The response includes an expiring bearer token and the fixture user. Tokens use a
random per-process signing key; restarting the API or logging out invalidates them.
This is a local demo runtime, never a deployment entrypoint.

Supported reads:

- `GET /health`
- `GET /api/v1/users/me` (`/auth/me` alias)
- `GET /api/v1/scada/farms` — empty until a run is published; no fallback farm.
- `GET /api/v1/scada/ingestion-summary?farm=lutelandet` — current immutable run.
- The same endpoint with `run_id=...` — that farm's historical stored payload.

Unsupported SCADA analyses return 409. Other application routes are absent.
Stored summaries must match the bounded v1 schema and stored farm/run identity.
Unconfirmed timestamp labels cannot expose dated energy or interval windows.

The normal application's matching summary endpoint uses its existing superuser
dependency. Normal farm metadata includes measured-run metadata for superusers
only when the pipeline-owned table exists. No migration is run by this backend.

Focused checks (avoid normal tests/conftest.py, which imports app.main):

```sh
.venv/bin/python -m pytest preview_tests --noconftest --no-cov -q
```
