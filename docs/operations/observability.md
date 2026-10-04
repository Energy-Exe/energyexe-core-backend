# Observability

Two layers: **application errors** go to a self-hosted GlitchTip (Sentry-compatible) via the
Sentry SDK; **infrastructure signals** are CloudWatch alarms → SNS email. Both are defined in
Terraform ([`infra/glitchtip.tf`](../../infra/glitchtip.tf), [`infra/monitoring.tf`](../../infra/monitoring.tf));
the setup runbook (certs, image mirroring, secrets) is in [`infra/README.md`](../../infra/README.md).

## Application side — `app/core/observability.py`

| Function | What it does |
|---|---|
| `init_sentry(settings)` | Called once at import time in `app/main.py` and at the top of each standalone job (`scripts/jobs/run_pipeline_daily.py`, `run_weather_daily.py`). **No-op when `SENTRY_DSN` is empty**, so local dev and tests run without a tracker, and it never raises on a bad DSN. Sets `environment=SENTRY_ENVIRONMENT` (default `production`; the ECS task definitions set it explicitly) and `release=SENTRY_RELEASE`. |
| `capture_exception(exc)` | Safe wrapper used by the jobs for non-fatal failures (e.g. the monthly-view refresh in the pipeline job). |
| `cron_checkin(monitor_slug, status, check_in_id=None, monitor_config=None)` | Sentry/GlitchTip **cron monitor** check-ins (`in_progress` → `ok` / `error`). Slugs in use: `pipeline-daily` (`app/cron/pipeline_daily.py`) and `weather-daily` (`scripts/jobs/run_weather_daily.py`). The `monitor_config` carries the expected schedule and max runtime, derived from `PIPELINE_DAILY_HOUR/MINUTE` and `WEATHER_DAILY_HOUR/MINUTE` on the task definitions — those env vars must match the EventBridge cron or GlitchTip alarms on runs that happened fine. |

Settings (`app/core/config.py`): `SENTRY_DSN`, `SENTRY_ENVIRONMENT`, `SENTRY_TRACES_SAMPLE_RATE`,
`SENTRY_RELEASE`.

### Release tagging (`GIT_SHA`)

The `Dockerfile`'s production stage declares `ARG GIT_SHA=unknown` and `ENV SENTRY_RELEASE=$GIT_SHA`.
`deploy-staging.yml` passes `--build-arg GIT_SHA=${{ github.sha }}`; the prod promotion copies
that same image, so prod carries the sha staging was built from. `init_sentry` treats
`unknown`/empty as "no release" so local builds do not tag everything with a literal "unknown".

### Request ids and logging

`LoggingMiddleware` (`app/core/middleware.py`) assigns a `request_id` per request and binds it
into the structlog context; the custom exception handlers in `app/core/exceptions.py` log with it,
and GlitchTip issues carry it as a tag, so a client-reported error can be joined to the ECS log
line. Standalone scripts must call `logging.basicConfig()` **before** `structlog.configure()` or
every `logger.info()` is dropped (structlog's `filter_by_level` defers to the stdlib level, which
defaults to WARNING) — the job scripts all do this.

### Startup sweepers (`app/main.py` lifespan)

Three idempotent sweepers run at startup so a redeploy never leaves rows stuck in a running
state: `sweep_stuck_reports` (`app/services/reports/orchestrator.py`), `sweep_stuck_import_jobs`
(`app/services/import_job_service.py`) and `sweep_stuck_agent_threads`
(`app/services/brain_agent_service.py`). Each is wrapped in its own try/except; a failure logs a
warning and does not block startup. They are skipped when `TESTING=true`.

## Infrastructure side — CloudWatch alarms (`infra/monitoring.tf`)

All alarms publish to the SNS topic whose subscription email is `alert_email` in
`terraform.tfvars` (confirm the subscription email or nothing is delivered).

| Alarm | Fires when |
|---|---|
| `energyexe-core-backend-no-healthy-hosts` | 5 consecutive minutes with zero healthy ALB targets (a normal 1–2 min deploy does not trip it) |
| `energyexe-core-backend-target-5xx` | backend 5xx responses |
| `energyexe-core-backend-elb-5xx` | ALB-generated 5xx (target unreachable / timeouts) |
| `energyexe-core-backend-memory-high` | task memory utilisation high |
| `energyexe-core-backend-rds-cpu-high`, `-rds-low-memory`, `-rds-low-storage` | RDS resource pressure |
| `energyexe-glitchtip-no-healthy-hosts` | GlitchTip itself is down |
| `energyexe-core-backend-pipeline-task-failed` (`pipeline_daily.tf`) | nightly pipeline task exited non-zero or stopped abnormally |
| `energyexe-core-backend-weather-task-failed` (`weather_daily.tf`) | daily weather task exited non-zero or stopped abnormally |
| EventBridge DLQ depth (`scheduled_imports.tf`) | an import exhausted every Lambda retry |

## Where to look

```bash
aws logs tail /ecs/energyexe-core-backend          --since 1h  --profile energyexe --region eu-north-1 | grep -v /health
aws logs tail /ecs/energyexe-core-backend-pipeline --since 24h --profile energyexe --region eu-north-1
aws logs tail /ecs/energyexe-core-backend-weather  --since 24h --profile energyexe --region eu-north-1
aws logs tail /aws/lambda/energyexe-core-backend-trigger-import --since 24h --profile energyexe --region eu-north-1
```

GlitchTip UI: `https://errors.energyexe.com` (issues grouped by stack trace, tagged with
`request_id`, `environment` and `release`; cron monitors under Uptime/Crons). Import outcomes:
admin `/import-jobs`. Schedules and expected times: [`scheduled-jobs.md`](scheduled-jobs.md).

## Known gaps

- The GitHub Actions workflows deploy only; no tests or linters run in CI (see the follow-ups in
  [`docs/README.md`](../README.md)).
- The frontends' GlitchTip DSNs are separate build-time variables in their own repos.
