# GitHub Actions Scheduled Imports

`scheduled-imports.yml` triggers data imports on a cron schedule via GitHub Actions.
The backend runs on **AWS Fargate** (`https://api.energyexe.com`) — Railway was retired
2026-06-28, so everything runs exclusively on AWS.

## How It Works

```
GitHub Actions cron (runs on the master branch)
    ↓ (HTTP POST)
Public endpoint: https://api.energyexe.com/api/v1/import-jobs/trigger/{job_name}
    ↓
Creates an ImportJobExecution record → runs the import → updates the DB
    ↓
View status in the UI: /import-jobs
```

Each scheduled run is just a `curl -X POST` to the trigger endpoint (so a run finishes in
~10-15s); the backend performs the actual import asynchronously. The target URL is set once
in the workflow:

```yaml
env:
  API_URL: https://api.energyexe.com   # AWS Fargate ALB (was Railway pre-2026-06-17 cutover)
```

> Scheduling lives in **GitHub Actions cron**, separate from the backend's in-process
> APScheduler, which runs the nightly *performance pipeline* (`pipeline_daily`).

## Schedule

| Job | Frequency | Time (UTC) | Data Imported |
|-----|-----------|------------|---------------|
| **entsoe-daily** | Daily | 06:00 | 3 days ago |
| **elexon-daily** | Daily | 07:00 | 3 days ago |
| **entsoe-prices-daily** | Daily | 08:00 | 2 days ago (day-ahead prices, 11 bidzones) |
| **elexon-prices-daily** | Daily | 09:00 | 1 day ago (GB market index prices) |
| **eia-monthly** | Monthly | 1st @ 02:00 | 2 months ago |
| **ecb-rates-daily** | Weekdays | 15:00 (Mon–Fri) | ECB exchange rates |

> The four daily jobs are deliberately spaced an hour apart. The backend runs a single
> task with `--workers 1` (`infra/ecs.tf`) and `execute_job` blocks on `subprocess.run`,
> so two imports firing at once stall each other's event loop.

> GitHub-cron is best-effort — runs can be delayed or skipped under load, so exact times
> drift. That is fine for everything above: each pulls a *dated* window and catches up
> on the next run.

## Taipower is scheduled by EventBridge, not here

| Job | Frequency | Time (UTC) | Where |
|-----|-----------|------------|-------|
| **taipower-hourly** | Hourly | :05 | `infra/scheduled_imports.tf` — `cron(5 * * * ? *)` |

Taipower's API serves only the **current** snapshot, stamped on the hour, with no
historical endpoint — so a missed run is an hour of Taiwan generation lost permanently.
Measured 2026-08-04..08-11, GitHub's scheduler fired it on only 9–20 of 24 hours per day
(15–60% dropped), with delays up to 3h17m, so it moved to EventBridge: an API Destination
with `maximum_retry_attempts = 8` over a 50-minute window, and an SQS dead-letter queue
whose depth alarms to SNS. A dropped GitHub schedule produces no run and therefore no red
build; a lost hour now raises an alarm instead.

It stays listed under `workflow_dispatch` so it can still be run by hand from the Actions
UI (`gh workflow run scheduled-imports.yml -f job_name=taipower-hourly`).

## Manual trigger

**From the GitHub UI:** Actions → "Scheduled Data Imports" → **Run workflow** → pick a job.

**From the command line:**
```bash
curl -X POST https://api.energyexe.com/api/v1/import-jobs/trigger/entsoe-daily
# latest status across jobs:
curl https://api.energyexe.com/api/v1/import-jobs/latest/status
```

## Monitoring

- **GitHub Actions logs:** Actions tab → the workflow run → expand the job for the `curl` output.
- **App / import results:** the `/import-jobs` page in the UI (status, record counts, errors).
- **Backend logs (CloudWatch):**
  ```bash
  aws logs tail /ecs/energyexe-core-backend --since 1h --profile energyexe --region eu-north-1 | grep -v /health
  ```

## Troubleshooting

| Symptom | Check |
|---|---|
| Workflow not running | Repo → Settings → Actions enabled; file in `.github/workflows/*.yml`; default branch is `master` |
| Endpoint 404 | Backend deployed with latest code; `API_URL` correct; `/import-jobs` page loads (migrations ran) |
| Endpoint 500 | **CloudWatch** backend logs (above); DB migration ran; API keys present in **AWS Secrets Manager** (`energyexe/core-backend/*`) |
| Triggered but failed | `/import-jobs` page for the error; CloudWatch logs; verify the source's API key secret |

## Customization

**Change a schedule:** edit the `cron:` expressions in `scheduled-imports.yml`.

**Add a job:** add a `cron:` entry + a job guarded by `if: github.event.schedule == '<cron>'`
that POSTs to `${{ env.API_URL }}/api/v1/import-jobs/trigger/<job>`, and register the job in
the API's `job_configs`.

**Disable a job:** comment out its `cron:` line.

## Cost

GitHub Actions free tier (2,000 min/month) comfortably covers it — each run is ~1 minute,
and Taipower (hourly, ~720/month) dominates at well under the limit.
