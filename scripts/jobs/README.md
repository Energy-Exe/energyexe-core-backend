# Scheduled Import Jobs System

Automated data import orchestration with monitoring dashboard and database tracking.

## Overview

This system manages scheduled imports from external data sources (ENTSOE, Taipower, ELEXON, EIA) with:
- ✅ Automatic scheduling via cron
- ✅ Database tracking of all executions
- ✅ Web UI dashboard for monitoring
- ✅ Manual job creation and execution
- ✅ Retry logic for failed jobs
- ✅ Filtering and history viewing

## Architecture

```
Cron Schedule
    ↓
run_import_with_tracking.py (wrapper script)
    ↓
Creates ImportJobExecution record in database
    ↓
Executes import script (ENTSOE/Taipower/etc.)
    ↓
Updates record with results (status, records, errors)
    ↓
View status in UI at /import-jobs
```

## Scheduled Jobs

| Job Name | Source | Schedule | Data Lag | Records/Run |
|----------|--------|----------|----------|-------------|
| entsoe-daily | ENTSOE | Daily 6 AM | 3 days | ~1,872 |
| elexon-daily | ELEXON | Daily 7 AM | 3 days | ~3,000+ |
| taipower-hourly | Taipower | Hourly :05 | Live | ~23 |
| eia-monthly | EIA | 1st @ 2 AM | 2 months | ~81,000 |

## Installation

### 1. Run Database Migration

```bash
cd energyexe-core-backend
poetry run alembic upgrade head
```

This creates the `import_job_executions` table.

### 2. Install Cron Jobs

**Edit crontab:**
```bash
crontab -e
```

**Add the following (update PROJECT_DIR path):**
```bash
# EnergyExe Scheduled Imports
PROJECT_DIR=/Users/mohammadfaisal/Documents/energyexe/energyexe-core-backend

# ENTSOE - Daily at 6 AM
0 6 * * * cd $PROJECT_DIR && poetry run python scripts/jobs/run_import_with_tracking.py entsoe-daily >> /tmp/entsoe-daily.log 2>&1

# ELEXON - Daily at 7 AM
0 7 * * * cd $PROJECT_DIR && poetry run python scripts/jobs/run_import_with_tracking.py elexon-daily >> /tmp/elexon-daily.log 2>&1

# Taipower - Every hour at :05
5 * * * * cd $PROJECT_DIR && poetry run python scripts/jobs/run_import_with_tracking.py taipower-hourly >> /tmp/taipower-hourly.log 2>&1

# EIA - Monthly on 1st at 2 AM
0 2 1 * * cd $PROJECT_DIR && poetry run python scripts/jobs/run_import_with_tracking.py eia-monthly >> /tmp/eia-monthly.log 2>&1
```

**Or install all at once:**
```bash
cd energyexe-core-backend
# Update PROJECT_DIR in crontab.txt first!
crontab scripts/jobs/crontab.txt
```

### 3. Verify Cron Installation

```bash
# List installed cron jobs
crontab -l

# Test manual execution
cd energyexe-core-backend
poetry run python scripts/jobs/run_import_with_tracking.py entsoe-daily
```

## Usage

### View Job Status (Web UI)

1. Navigate to **http://localhost:3000/import-jobs**
2. See job status cards:
   - Last run time
   - Next scheduled run
   - Success rate
   - Records imported
3. View execution history table
4. Filter by source, status, or type

### Manual Job Creation (Web UI)

1. Click "Create New Job" button
2. Select source (ENTSOE, Taipower, etc.)
3. Choose date range to import
4. Click "Create & Execute"
5. Monitor progress in real-time

### Retry Failed Jobs (Web UI)

1. Filter by status="failed"
2. Click "Retry" button on failed job
3. Job will re-execute with incremented retry counter

### Manual Execution (CLI)

Run any job manually for testing:

```bash
cd energyexe-core-backend

# Test ENTSOE import
poetry run python scripts/jobs/run_import_with_tracking.py entsoe-daily

# Test Taipower snapshot
poetry run python scripts/jobs/run_import_with_tracking.py taipower-hourly

# Test ELEXON import
poetry run python scripts/jobs/run_import_with_tracking.py elexon-daily

# Test EIA import
poetry run python scripts/jobs/run_import_with_tracking.py eia-monthly
```

## API Endpoints

```
GET  /api/v1/import-jobs                    # List executions (with filters)
GET  /api/v1/import-jobs/latest/status      # Latest status per job (dashboard)
GET  /api/v1/import-jobs/health/status      # System health check
GET  /api/v1/import-jobs/{id}               # Get job details
POST /api/v1/import-jobs                    # Create manual job
POST /api/v1/import-jobs/{id}/execute       # Execute job
POST /api/v1/import-jobs/{id}/retry         # Retry failed job
```

## Monitoring

### Check Job Logs

```bash
# View recent logs
tail -f /tmp/entsoe-daily.log
tail -f /tmp/taipower-hourly.log

# View all logs
ls -lh /tmp/*-import*.log
```

### Check Database

```python
# Count executions
SELECT source, status, COUNT(*)
FROM import_job_executions
GROUP BY source, status;

# Recent failures
SELECT * FROM import_job_executions
WHERE status = 'failed'
ORDER BY created_at DESC
LIMIT 10;

# Success rate by source
SELECT
  source,
  COUNT(*) FILTER (WHERE status = 'success') * 100.0 / COUNT(*) as success_rate
FROM import_job_executions
WHERE created_at > NOW() - INTERVAL '7 days'
GROUP BY source;
```

### Health Check API

```bash
curl http://localhost:8000/api/v1/import-jobs/health/status
```

Returns:
```json
{
  "total_jobs": 4,
  "running_jobs": 0,
  "recent_failures": 2,
  "jobs_behind_schedule": [],
  "overall_health": "healthy",
  "last_updated": "2025-10-21T12:00:00Z"
}
```

## Troubleshooting

### Job Failed - How to Debug

1. **Check UI**: `/import-jobs` → filter status=failed → view error message
2. **Check Logs**: `tail -100 /tmp/<job-name>.log`
3. **Check Database**: Query `import_job_executions` table
4. **Retry**: Click "Retry" in UI or run manually

### Job Not Running

1. **Verify cron installed**: `crontab -l`
2. **Check cron logs**: `grep CRON /var/log/syslog` (Linux) or `log show --predicate 'process == "cron"'` (Mac)
3. **Test manually**: Run `poetry run python scripts/jobs/run_import_with_tracking.py <job-name>`

### Database Connection Errors

- Ensure PostgreSQL is running
- Check `DATABASE_URL` environment variable
- Verify migration ran: `poetry run alembic current`

### Import Script Errors

- Run import script directly to see detailed errors
- Check ENTSOE README for control area issues
- Verify API keys in `.env` file

## Job-Specific Notes

### ENTSOE Daily
- Imports data from **3 days ago** (ENTSOE publication lag)
- Sources: Denmark (DK), Belgium (BE), France (FR)
- Uses control area codes (not bidding zones)
- ~1,872 records per day

### Taipower Hourly
- Fetches **live snapshot** (current generation)
- Taiwan wind farms only
- No date range - always current
- ~23 units per snapshot
- Run hourly to build historical dataset

### ELEXON Daily
- Imports UK generation data from **3 days ago**
- ~140+ wind farms
- Uses BM unit codes

### EIA Monthly
- Imports USA monthly data from **2 months ago**
- Runs once per month
- ~1,355 plants
- Takes 10-15 minutes

## Files

- `run_import_with_tracking.py` - Main wrapper script (called by cron)
- `crontab.txt` - Cron schedule configuration
- `README.md` - This file

## Logs Location

- `/tmp/entsoe-daily.log` - ENTSOE job logs
- `/tmp/elexon-daily.log` - ELEXON job logs
- `/tmp/taipower-hourly.log` - Taipower job logs
- `/tmp/eia-monthly.log` - EIA job logs

Logs are rotated automatically (30-day retention via cron cleanup).

## Next Steps

After imports complete, run aggregation:

```bash
# Aggregate imported data
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --source <SOURCE> --start <DATE> --end <DATE>
```

Consider scheduling aggregation jobs as well!
