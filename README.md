# EnergyExe Core Backend

FastAPI backend for the EnergyExe wind-performance platform: generation / price / weather data
ingestion from ENTSOE, Elexon, EIA, Taipower, NVE and Energistyrelsen, the six-module performance
pipeline, opportunity detection, the reports platform, the brain agent and the SCADA portal API.
Serves the admin UI (`energyexe-admin-ui`) and the client portal (`energyexe-client-ui`).

**Documentation index: [`docs/README.md`](docs/README.md).** Architecture and layering:
[`ARCHITECTURE.md`](ARCHITECTURE.md). Infrastructure and runbooks: [`infra/README.md`](infra/README.md).

## How it runs (the short version)

| | |
|---|---|
| Hosting | AWS Fargate (`eu-north-1`), one always-on task behind an ALB, Postgres on RDS, Valkey on ElastiCache. Terraform in [`infra/`](infra/README.md); staging is a separate root in `infra/staging/` ([`infra/STAGING.md`](infra/STAGING.md)). |
| Deploy flow | **Staging-first.** Push to the `staging` branch → `.github/workflows/deploy-staging.yml` builds the image and deploys staging. Merge `staging` → `master` → `deploy-aws.yml` **promotes the staging-validated image** to prod (no rebuild). Both workflows skip `**.md`-only commits. |
| Scheduling | **EventBridge in AWS**, never GitHub `schedule:` or a crontab: Lambda-triggered imports (`infra/scheduled_imports.tf`), the 01:30 UTC weather task and the 03:00 UTC pipeline task (`infra/weather_daily.tf`, `infra/pipeline_daily.tf`). Details: [`docs/operations/scheduled-jobs.md`](docs/operations/scheduled-jobs.md). |
| Error tracking | Self-hosted GlitchTip via the Sentry SDK; `SENTRY_DSN` empty = disabled. [`docs/operations/observability.md`](docs/operations/observability.md). |
| Dependencies | **The Docker image installs from `requirements.txt`** (see `Dockerfile`). **Local development uses Poetry** (`pyproject.toml` / `poetry.lock`). When you add a dependency, update both or the image will not have it. |
| Port | The app's default is **8001** (`PORT` in `app/core/config.py`; `EXPOSE 8001` and the `uvicorn --port 8001` CMD in the `Dockerfile`; `docker-compose.yml`). The **local-dev convention is 8002** — the workspace `start` skill runs `uvicorn --port 8002` and `energyexe-client-ui/src/lib/api.ts` falls back to `127.0.0.1:8002` — so a locally started API and a locally running Docker container never collide. |

## Local development

Prerequisites: Python 3.11+, Poetry, PostgreSQL 15+ (or Docker for the compose stack).

```bash
poetry install --with dev,test
cp .env.example .env            # then fill in DATABASE_URL and any API keys you need
poetry run alembic upgrade head
poetry run uvicorn app.main:app --reload --port 8002   # local convention (see "Port" above)
# or, honouring PORT from .env (default 8001):
poetry run python scripts/start.py
```

API docs: `http://localhost:8002/docs` and `/redoc` (or `:8001` if you used `scripts/start.py`
with the default `PORT`). Health: `GET /health`.

Local fixture login for the two portals: see the workspace `CLAUDE.md` ("Local Test Credentials").

### Docker Compose

`docker-compose.yml` runs Postgres, Valkey and the API (development image target, port **8001**).

```bash
docker compose up -d
docker compose exec api alembic upgrade head
curl http://localhost:8001/health
```

## Everyday commands

```bash
# Tests (SQLite in-memory via tests/conftest.py; some *_api tests expect a live server on :8001 and skip/fail without one)
poetry run pytest
poetry run pytest -q -k "test_name"
poetry run pytest --cov=app

# Lint / format
poetry run black app && poetry run isort app
poetry run flake8 app && poetry run mypy app

# Migrations — never edit an applied migration; add a new one
poetry run alembic revision --autogenerate -m "describe the change"
poetry run alembic upgrade head

# Makefile shortcuts: make install | migrate | run-api | test | lint | format
```

Note: the GitHub Actions workflows in this repo deploy; **they run no tests or linters**. Run
`poetry run pytest` locally before pushing to `staging`.

## Project layout

```
app/
├── api/v1/endpoints/   # one module per resource, mounted by app/api/v1/router.py
├── core/               # config, database, deps (auth), middleware, observability, redis, audit
├── cron/               # job bodies for the EventBridge-run ECS tasks (pipeline_daily.py)
├── models/             # SQLAlchemy 2.0 models
├── prompts/            # brain-agent system prompts; reports/<type>/<section>.txt narrative prompts
├── scada_preview/      # isolated local-only preview API for measured SCADA runs
├── schemas/            # Pydantic request/response schemas
├── services/           # business logic; reports/ and opportunity_schemas/ sub-packages
├── templates/email/    # Jinja2 transactional-email templates (Resend)
└── utils/              # small shared helpers
alembic/                # migrations
infra/                  # Terraform (prod root) + infra/staging/ (staging root)
scripts/                # seeds/, jobs/ (batch entrypoints), fixes/, operator one-offs
tests/                  # pytest suite; tests/reference/ = vendored reference pipeline
docs/                   # see docs/README.md
```

## Environment variables

`.env.example` lists the variables `app/core/config.py` reads. The ones you need locally:
`DATABASE_URL`, `SECRET_KEY`, `DEBUG`, `BACKEND_CORS_ORIGINS`; everything else (API keys,
Resend, LLM keys, GlitchTip DSN, brain-agent DB roles) is optional and off when empty. In AWS the
values come from Secrets Manager via the ECS task definition — never from a committed file.

## Contributing

1. Branch from `staging`; keep business logic in services and endpoints thin (see `ARCHITECTURE.md`).
2. `poetry run pytest` locally; format with black/isort.
3. Open a PR into `staging`. After it deploys, verify on staging, then promote with a `staging → master` merge.
4. Record user-visible changes in the workspace `UPDATES.md`; put durable lessons in `docs/history/lessons.md`, not in session logs.
