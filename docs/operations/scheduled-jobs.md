# Scheduled jobs

Every recurring job in this backend is scheduled by **AWS EventBridge**, declared in Terraform
under [`infra/`](../../infra/README.md). There is no in-process scheduler (the APScheduler job
was removed), no crontab, and no GitHub Actions `schedule:` (retired 2026-08-17 — see
[`.github/workflows/README.md`](../../.github/workflows/README.md) for the measurements that
justified it). **If something needs to run on a timer, it goes in Terraform.**

All times are UTC (EventBridge cron is UTC-only). Oslo is +1h in winter, +2h in summer.

## 1. Lambda-triggered imports — `infra/scheduled_imports.tf`

```
EventBridge rule (cron) → Lambda energyexe-core-backend-trigger-import {"job_name": "<job>"}
  → POST https://api.energyexe.com/api/v1/import-jobs/trigger/{job_name}
  → ImportJobExecution row; import runs synchronously inside the request
```

The job list is the map `local.import_schedules` in the `.tf` file; the key is the `job_name`
the endpoint accepts (`job_configs` in `app/api/v1/endpoints/import_jobs.py`). The same times
are mirrored in `IMPORT_SCHEDULES` in `app/services/import_job_service.py` for the admin
"/import-jobs" next-run column, and `tests/test_import_schedules.py` parses the `.tf` and fails
if they drift.

| `job_name` | EventBridge cron | UTC | Window imported | Runs |
|---|---|---|---|---|
| `taipower-hourly` | `cron(5 * * * ? *)` | hourly :05 | current snapshot (no history endpoint — a missed hour is lost) | `scripts/seeds/raw_generation_data/taipower/import_from_api.py` |
| `entsoe-daily` | `cron(10 22 * * ? *)` | 22:10 | day − 3 | `scripts/seeds/raw_generation_data/entsoe/import_from_api.py` |
| `elexon-daily` | `cron(20 22 * * ? *)` | 22:20 | day − 10 | `scripts/seeds/raw_generation_data/elexon/import_from_api.py` |
| `entsoe-prices-daily` | `cron(30 22 * * ? *)` | 22:30 | day − 2, day-ahead prices | `scripts/seeds/power_prices/import_prices_from_api.py` then `process_to_hourly.py` |
| `elexon-prices-daily` | `cron(40 22 * * ? *)` | 22:40 | day − 1, GB market index | `scripts/seeds/power_prices/elexon/import_elexon_prices.py` then `process_to_hourly.py --source ELEXON` |
| `ecb-rates-daily` | `cron(50 22 ? * MON-FRI *)` | 22:50 Mon–Fri | same day | `scripts/seeds/exchange_rates/import_ecb_rates.py` |
| `eia-monthly` | `cron(55 22 1 * ? *)` | 22:55 on the 1st | month − 2 | EIA import (monthly data) |

The commands are assembled in `ImportJobService` (`app/services/import_job_service.py`, around
the `prices_path` / `rates_path` block) and run with `subprocess.run`. The night batch sits at
22:10–22:55 UTC (≈ midnight Oslo) deliberately: the API runs one uvicorn worker and the import
blocks it, so a daytime import froze `/health` and got the task restarted by the ALB
(2026-09-05 move). Jobs are 10 min apart and never overlap the :05 Taipower run.

Reliability: 3 in-invocation attempts with backoff, then 2 async Lambda retries, then an SQS
DLQ (`energyexe-core-backend-eventbridge-dlq`) whose depth alarms to SNS. The Lambda reads the
response body's `status`, because the trigger endpoint returns **200 even when the import
failed**. Never replace the Lambda with an EventBridge API Destination (hard 5 s timeout).

Manual re-run: GitHub Actions → "Manual Data Import" (`scheduled-imports.yml`,
`workflow_dispatch` only) or `curl -X POST https://api.energyexe.com/api/v1/import-jobs/trigger/<job>`.
Keep manual windows to a few days: the endpoint is synchronous and the ALB kills the task after
150 s blocked (a 504 on a manual run is usually a false alarm — check `/import-jobs`).

## 2. Daily ERA5 weather task — `infra/weather_daily.tf` (EPR-121)

```
EventBridge cron(30 1 * * ? *)  →  ecs:RunTask  family energyexe-core-backend-weather
  →  python scripts/jobs/run_weather_daily.py        (01:30 UTC)
```

Imports **one day**, `today − WEATHER_LAG_DAYS` (default 6; ERA5T publishes ~5–6 days behind),
for every windfarm, from Copernicus CDS (`CDSAPI_URL`, `CDSAPI_KEY` from Secrets Manager),
and records a `weather_import_jobs` row (visible on the admin "Weather data → Import" page).
Exit 0 = every day processed or already complete, 1 = any failure. Runs before the pipeline on
purpose: the pipeline inner-joins generation with weather, so a missing weather day silently
drops that day from every KPI. Variables: `weather_daily_hour` (default `1`),
`weather_daily_minute` (`30`), `weather_lag_days` (`6`) in `infra/variables.tf`.

Backfills reuse the same task definition with a command override (`terraform output
weather_run_task_command`), e.g. `--start 2026-01-01 --end 2026-03-01 --windfarm-ids 8806`.
Only the scheduled, date-less shape checks in to the GlitchTip cron monitor (`weather-daily`).

## 3. Nightly performance pipeline + opportunity detection — `infra/pipeline_daily.tf`

```
EventBridge cron(0 3 * * ? *)  →  ecs:RunTask  family energyexe-core-backend-pipeline
  →  python scripts/jobs/run_pipeline_daily.py  →  app/cron/pipeline_daily.py:run_pipeline_job
```

Three phases, in order (`app/cron/pipeline_daily.py`):

1. `PerformancePipelineService.run_pipeline_batch()` — Modules 1–6 for every operational
   windfarm ([`docs/pipeline/`](../pipeline/README.md)). A batch failure is the job failure and
   **skips** detection.
2. `refresh_generation_monthly_view()` (`app/services/generation_monthly_view.py`) — refreshes
   the materialised view `mv_generation_monthly_by_windfarm` used by financial ratios, OPEX/MWh
   and the digest. Best-effort: a failure is logged and reported but never blocks detection.
   `scripts/jobs/refresh_generation_monthly.py` is the manual CLI for the same refresh (first
   population after the migration, or after a big import).
3. `OpportunityDetectionService.run_detection_job(period_months=24)` — opportunity detection
   over the fresh performance data ([`docs/features/opportunity-detection.md`](../features/opportunity-detection.md)).

Exit codes: 0 ok, 1 batch failed (detection skipped), 2 batch ok but detection failed. ECS
surfaces the code; the `energyexe-core-backend-pipeline-task-failed` alarm fires on a non-zero
exit **or** an abnormal stop (OOM, image pull, capacity). The job also checks in to the GlitchTip
cron monitor `pipeline-daily`; `PIPELINE_DAILY_HOUR` / `PIPELINE_DAILY_MINUTE` on the task
definition only tell GlitchTip when to expect the check-in — they do not schedule anything and
must match the cron (`pipeline_daily_hour` variable, default `3`).

Smoke test: `terraform output pipeline_run_task_command` plus `--overrides` with
`--windfarm-ids 7404,7200`; `--skip-detection` runs the batch only. The detection-only backstop
after a phase-3 failure is `scripts/jobs/run_detection_jobs.py opportunity-detection`.

## 4. Not scheduled — operator-run on purpose

| Script | What | When |
|---|---|---|
| `scripts/jobs/import_vinddata.py` | Energistyrelsen (DEA) monthly workbooks `Vinddata.xlsx` + `Parkproduktion.xlsx` → `generation_data`, then refreshes the monthly view. `--apply` writes, otherwise preview. | By hand each time DEA publishes (monthly); see `scripts/seeds/raw_generation_data/energistyrelsen/README.md` |
| `scripts/jobs/run_detection_shard.py` | Sharded, session-per-windfarm opportunity detection for fleet backfills (`--total-shards N --shard-index i`, `--only-missing-since`) | Fleet re-runs after a detector change |
| `scripts/jobs/run_detection_jobs.py` | `performance-pipeline` or `opportunity-detection` as a one-off | Backstop / debugging |
| `scripts/jobs/run_import_with_tracking.py` | Runs one import job locally with an `ImportJobExecution` row | Local testing of an import |
| `scripts/rerun_report_narratives.py` | Re-generate narrative sections of existing reports | After a prompt / terminology change |

EIA and Energistyrelsen are **monthly** sources; nothing imports them hourly.

## 5. Where to look when something did not run

```bash
# EventBridge rules enabled?
aws events list-rules --profile energyexe --region eu-north-1
# Lambda trigger log (imports)
aws logs tail /aws/lambda/energyexe-core-backend-trigger-import --since 24h --profile energyexe --region eu-north-1
# Task logs
aws logs tail /ecs/energyexe-core-backend-weather  --since 24h --profile energyexe --region eu-north-1
aws logs tail /ecs/energyexe-core-backend-pipeline --since 24h --profile energyexe --region eu-north-1
# Backend side
aws logs tail /ecs/energyexe-core-backend --since 1h --profile energyexe --region eu-north-1 | grep -v /health
```

Import outcomes are on the admin `/import-jobs` page; weather runs on "Weather data → Import";
GlitchTip cron monitors (`pipeline-daily`, `weather-daily`) catch a night that never ran at all
([`observability.md`](observability.md)).

## 6. Adding or changing a job

- **Import**: register it in `job_configs` (`import_jobs.py`) and the command builder in
  `import_job_service.py`; add the entry to `local.import_schedules` and mirror it in
  `IMPORT_SCHEDULES`; add it to the `scheduled-imports.yml` choice list; `terraform plan/apply`.
- **Long-running task**: copy the `weather_daily.tf` shape (own task-definition family, EventBridge
  `ecs:RunTask`, task-failure alarm, GlitchTip check-in), add a `scripts/jobs/run_*.py` entrypoint
  that sets `logging.basicConfig()` before `structlog.configure()` (otherwise a standalone script
  logs nothing at INFO) and returns a meaningful exit code.
- Record the change here and in the workspace `UPDATES.md`.
