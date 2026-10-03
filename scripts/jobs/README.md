# scripts/jobs

Entrypoints for the scheduled and operator-run batch jobs (imports, nightly pipeline, weather,
detection shards, DEA workbook import). **Scheduling lives in AWS EventBridge, not in a crontab** —
the one page that documents every schedule, entrypoint and monitor is
[`docs/operations/scheduled-jobs.md`](../../docs/operations/scheduled-jobs.md). Read that first.
