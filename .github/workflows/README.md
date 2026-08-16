# GitHub Actions workflows

Two deploy workflows (`deploy-aws.yml`, `deploy-staging.yml`) and one manual
import trigger (`scheduled-imports.yml`). **No workflow in this repo runs on a
schedule.**

## Scheduling lives in AWS, not here

Every recurring job is scheduled by **EventBridge**, declared in Terraform at
`infra/scheduled_imports.tf` (`local.import_schedules`). This is the standing
rule for new jobs too — if something needs to run on a timer, it goes in
Terraform. Do not add a `schedule:` block to a workflow.

```
EventBridge rule (cron)
    ↓
Lambda  energyexe-core-backend-trigger-import   {"job_name": "<job>"}
    ↓ HTTP POST
https://api.energyexe.com/api/v1/import-jobs/trigger/{job_name}
    ↓
ImportJobExecution row → import runs → DB updated → visible at /import-jobs
```

| Job | Frequency | Time (UTC) | Data imported |
|-----|-----------|------------|---------------|
| **taipower-hourly** | Hourly | :05 | Current snapshot |
| **entsoe-daily** | Daily | 06:00 | 3 days ago |
| **elexon-daily** | Daily | 07:00 | 3 days ago |
| **entsoe-prices-daily** | Daily | 08:00 | 2 days ago (day-ahead, 11 bidzones) |
| **elexon-prices-daily** | Daily | 09:00 | 1 day ago (GB market index) |
| **eia-monthly** | Monthly | 1st @ 02:00 | 2 months ago |
| **ecb-rates-daily** | Weekdays | 15:00 Mon–Fri | ECB exchange rates |

The daily jobs are spaced an hour apart on purpose. The backend runs a single
task with `--workers 1` (`infra/ecs.tf`) and `execute_job` blocks on
`subprocess.run`, so two imports firing together stall each other's event loop.
Keep these times in sync with `_calculate_next_run` in
`app/services/import_job_service.py`, which drives the admin "next run" column.

The nightly *performance pipeline* (`app/cron/pipeline_daily.py`, 03:00 UTC) is
the one remaining exception: it is still scheduled in-process by APScheduler
rather than by AWS. Moving it to its own scheduled ECS task is tracked
separately.

### Why GitHub cron was retired (2026-08-17)

GitHub's `schedule:` is documented as best-effort, and measurably was:

- **taipower-hourly** fired on only 9–20 of 24 hours per day — 15–60% dropped,
  delays up to 3h17m (measured 2026-08-04..08-11). Taipower's API serves only
  the *current* snapshot with no historical endpoint, so each dropped run was an
  hour of Taiwan generation lost permanently.
- Every **daily** import landed 21–80 minutes late, on every run
  (measured 2026-08-14..08-16). Those self-heal, but the hour of spacing that
  keeps them off each other's toes was luck rather than design.

A dropped schedule also produced *no run*, and therefore no red build — the
failure was silent. EventBridge fires on the minute, the Lambda retries on the
real outcome, and anything that exhausts its retries lands in an SQS DLQ whose
depth alarms to SNS.

### Reliability of the AWS path

- **3 in-invocation attempts**, 20s backoff — covers the ~1–2 min a deploy takes
  the single backend task down.
- **2 Lambda async retries** inside a 30-minute window.
- **On-failure → SQS DLQ → CloudWatch alarm → SNS.**
- The Lambda checks the response body's `status` field, because the trigger
  endpoint returns **200 with the job row even when the import failed**. HTTP
  status alone is not the success signal.

> Never point an EventBridge **API Destination** at these endpoints. API
> Destinations enforce a hard 5-second invocation timeout and the imports take
> 7s (Taipower) to ~118s (ENTSOE prices), so every delivery is recorded failed
> despite succeeding, and retried. That is what the Lambda shim exists to avoid.

## `scheduled-imports.yml` — the manual escape hatch

Despite the filename, this workflow only runs on `workflow_dispatch`. Use it to
re-run a job by hand or to drive a short backfill.

**GitHub UI:** Actions → "Manual Data Import" → **Run workflow** → pick a job
(optionally set `start` / `end`).

```bash
gh workflow run scheduled-imports.yml -f job_name=entsoe-daily
gh workflow run scheduled-imports.yml -f job_name=entsoe-prices-daily \
  -f start=2026-08-01 -f end=2026-08-07

# or straight to the API
curl -X POST https://api.energyexe.com/api/v1/import-jobs/trigger/entsoe-daily
curl https://api.energyexe.com/api/v1/import-jobs/latest/status
```

> **Backfills:** keep the window small. The trigger endpoint runs the import
> synchronously and blocks the single worker; the ALB health check
> (`interval 30 × unhealthy_threshold 5`) kills the task after **150s** blocked.
> For anything larger than a few days, run the import scripts directly rather
> than through this endpoint.

## Monitoring

```bash
# Did the schedules fire? (one line per successful trigger)
aws logs tail /aws/lambda/energyexe-core-backend-trigger-import \
  --since 24h --profile energyexe --region eu-north-1

# Backend-side detail
aws logs tail /ecs/energyexe-core-backend --since 1h \
  --profile energyexe --region eu-north-1 | grep -v /health

# Anything lost entirely
aws sqs get-queue-attributes --profile energyexe --region eu-north-1 \
  --queue-url "$(aws sqs get-queue-url --queue-name energyexe-core-backend-eventbridge-dlq \
    --profile energyexe --region eu-north-1 --output text)" \
  --attribute-names ApproximateNumberOfMessagesVisible
```

Import results themselves are on the `/import-jobs` page in the admin UI.

## Troubleshooting

| Symptom | Check |
|---|---|
| A job stopped running | `aws events list-rules --profile energyexe --region eu-north-1` — is the rule `ENABLED`? Then the Lambda log group above |
| Job ran but imported nothing | `/import-jobs` for the error; `records_imported` reflects the *fetch* step only — for prices, confirm `price_data` actually grew |
| Endpoint 500 | CloudWatch backend logs; API keys present in Secrets Manager (`energyexe/core-backend/*`) |
| DLQ alarm fired | An hour exhausted every retry. Re-run by hand via `workflow_dispatch`; for Taipower that hour is unrecoverable |
| Manual run returns 504 | Known ALB idle-timeout false alarm — the import is still running. Check `/import-jobs`, do **not** retry |

## Adding or changing a job

1. Register it in the API's `job_configs` (`app/api/v1/endpoints/import_jobs.py`).
2. Add an entry to `local.import_schedules` in `infra/scheduled_imports.tf` —
   the key is the `job_name`; rule, target and Lambda permission fan out from it.
3. `terraform plan` and confirm nothing else is touched, then apply.
4. Add it to the `job_name` choice list in `scheduled-imports.yml` so it can be
   run by hand, and to the table above.

To disable a job, remove its entry from the map (or set the rule `state` to
`DISABLED` for a temporary pause).

Note `.github/**` is in `deploy-aws.yml`'s `paths-ignore`, so workflow-only
changes do not trigger a backend deploy.
