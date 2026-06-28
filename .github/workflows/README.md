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
| **taipower-hourly** | Hourly | :05 | Current snapshot |
| **eia-monthly** | Monthly | 1st @ 02:00 | 2 months ago |
| **ecb-rates-daily** | Weekdays | 15:00 (Mon–Fri) | ECB exchange rates |

> GitHub-cron is best-effort — runs can be delayed or skipped under load, so exact times
> drift. If reliable timing is ever required, move scheduling to EventBridge Scheduler →
> the same trigger endpoints.

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
