# GitHub Actions Scheduled Imports Setup

This workflow automatically triggers data imports on schedule using GitHub Actions.

## How It Works

```
GitHub Actions (free, reliable)
    ↓ (HTTP POST)
Public API Endpoint: /api/v1/import-jobs/trigger/{job_name}
    ↓
Creates ImportJobExecution record
    ↓
Executes import script
    ↓
Updates database with results
    ↓
View status in UI: /import-jobs
```

## Setup Steps

### 1. Update API URL

Edit `.github/workflows/scheduled-imports.yml`:

```yaml
env:
  API_URL: https://your-actual-app.railway.app  # UPDATE THIS
```

Replace `your-actual-app.railway.app` with your Railway deployment URL.

### 2. Push to GitHub

```bash
git add .github/workflows/scheduled-imports.yml
git commit -m "Add scheduled import workflows"
git push
```

### 3. Verify Workflow

1. Go to GitHub repo → **Actions** tab
2. You should see "Scheduled Data Imports" workflow
3. Click on it to see scheduled runs

### 4. Manual Test (Before Waiting for Schedule)

**Test from GitHub UI:**
1. Go to Actions → "Scheduled Data Imports"
2. Click "Run workflow" button
3. Select job (e.g., "entsoe-daily")
4. Click "Run workflow"
5. Watch it execute
6. Check your `/import-jobs` page for results

**Test from command line:**
```bash
# Test the public endpoint
curl -X POST https://your-app.railway.app/api/v1/import-jobs/trigger/entsoe-daily

# Should return job execution details
```

## Schedule

| Job | Frequency | Time (UTC) | Data Imported |
|-----|-----------|------------|---------------|
| **entsoe-daily** | Daily | 6:00 AM | 3 days ago |
| **elexon-daily** | Daily | 7:00 AM | 3 days ago |
| **taipower-hourly** | Hourly | :05 minutes | Current snapshot |
| **eia-monthly** | Monthly | 1st @ 2:00 AM | 2 months ago |

## Monitoring

### GitHub Actions Logs

View execution logs:
1. GitHub repo → Actions tab
2. Click on workflow run
3. Expand job to see curl output and results

### Application Logs

View detailed results:
1. Navigate to `/import-jobs` page in your app
2. See all executions with status, records, errors
3. Filter by source, status, or date

### Check if Jobs Are Running

```bash
# Test endpoint
curl https://your-app.railway.app/api/v1/import-jobs/latest/status

# Returns latest status for all jobs
```

## Troubleshooting

### Workflow Not Running

**Check:**
1. GitHub repo → Settings → Actions → "Allow all actions" enabled
2. Workflow file is in `.github/workflows/` directory
3. File has `.yml` extension
4. Branch is `main` or `master` (default branch)

### Endpoint Returns 404

- Ensure backend is deployed with latest code
- Check API URL in workflow file
- Verify migration ran: Check `/import-jobs` page loads

### Endpoint Returns 500

- Check Railway logs for backend errors
- Ensure database migration ran
- Verify import scripts exist in deployment

### Job Triggered But Failed

1. Check `/import-jobs` page for error message
2. Check Railway logs
3. Verify API keys in Railway environment variables

## Customization

### Change Schedule Times

Edit cron expressions in workflow file:

```yaml
schedule:
  - cron: '0 6 * * *'  # Minute Hour Day Month DayOfWeek
  # Examples:
  # '*/30 * * * *'  # Every 30 minutes
  # '0 */6 * * *'   # Every 6 hours
  # '0 0 * * 0'     # Weekly on Sunday midnight
```

### Add New Job

Add new job to workflow:

```yaml
jobs:
  new-import:
    if: github.event.schedule == '0 8 * * *'
    runs-on: ubuntu-latest
    steps:
      - name: Trigger New Import
        run: |
          curl -X POST "${{ env.API_URL }}/api/v1/import-jobs/trigger/new-job-name"
```

And add to API endpoint's job_configs.

### Disable a Job

Comment out the schedule:

```yaml
schedule:
  # - cron: '0 6 * * *'  # Disabled ENTSOE
  - cron: '5 * * * *'    # Taipower still active
```

## Cost

**GitHub Actions is FREE for public repos and has generous free tier for private repos:**
- 2,000 minutes/month (free tier)
- Each job runs ~1 minute
- ENTSOE: 30 runs/month = 30 minutes
- Taipower: 720 runs/month = 720 minutes
- Total: ~800 minutes/month (well within free tier)

## Benefits

✅ Zero infrastructure (GitHub handles scheduling)
✅ Reliable (GitHub's SLA)
✅ Easy to manage (edit YAML file)
✅ Works with Railway (or any deployment)
✅ Execution logs in GitHub
✅ Manual trigger capability
✅ No Docker complexity
✅ No cron management

## Next Steps

After jobs run, check:
1. `/import-jobs` page - See execution status
2. Database - Verify records imported
3. GitHub Actions - View execution logs

That's it! Your scheduled imports are fully automated.
