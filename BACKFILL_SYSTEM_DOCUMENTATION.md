# Data Backfill System - Complete Documentation (v2.0 with Celery)

## Quick Reference

### Start Everything
```bash
# Terminal 1: API Server
make run-api

# Terminal 2: Celery Worker  
make run-worker

# Terminal 3: Flower (optional)
make run-flower

# Or all at once (requires tmux)
make run-all
```

### Check Status
- **API Docs**: http://localhost:8000/docs
- **Flower Dashboard**: http://localhost:5555 (admin/admin)
- **Worker Status**: `celery -A app.celery_app inspect active`

### Common Commands
```bash
# Check Redis connection
redis-cli ping

# View queued tasks
redis-cli llen celery

# Purge all tasks (emergency)
make dev-purge-queue

# View worker logs
# The logs appear in the terminal where you ran make run-worker
```

## Overview
The Data Backfill System is a comprehensive asynchronous solution for fetching and storing historical generation data from multiple external APIs (ENTSOE, Elexon, EIA, Taipower) for windfarms and their associated generation units. The system uses **Celery** for distributed task processing, providing scalability, reliability, and real-time monitoring.

## End-to-End Workflow

### Complete User Journey
1. **User Interface** (Frontend)
   - User navigates to `/backfill` page
   - Selects a windfarm from dropdown
   - Chooses start and end years
   - Optionally selects specific data sources (ENTSOE, Elexon, etc.)
   - Clicks "Preview" to see what will be backfilled
   - Clicks "Start Backfill" to initiate the process

2. **API Request** (Frontend → Backend)
   - Frontend sends POST request to `/api/v1/backfill/jobs`
   - Request includes: windfarm_id, start_year, end_year, sources[]

3. **Job Creation** (Backend Service)
   - Validates windfarm exists and has generation units
   - Creates BackfillJob record in database
   - Generates BackfillTask records (one per generation unit × month)
   - Automatically starts processing synchronously

4. **Data Processing** (Backend Service)
   - Iterates through each task sequentially
   - Fetches data from appropriate external API (ENTSOE/Elexon/etc.)
   - Checks for existing data to avoid duplicates
   - Stores new data in respective tables
   - Updates task status and counters
   - Handles errors with retry logic

5. **Response & Monitoring** (Backend → Frontend)
   - Returns job details with current status
   - Frontend redirects to job detail page (`/backfill/{jobId}`)
   - User can monitor progress in real-time
   - Can retry failed tasks or cancel if needed

6. **Data Availability** (Post-Processing)
   - User can check data availability at `/backfill/availability`
   - Shows monthly coverage percentages per source
   - Helps identify gaps that need backfilling

## Frontend Components

### Pages and Routes

1. **Main Backfill Page** (`/backfill/index.tsx`)
   - Configuration form for creating new jobs
   - Preview functionality before starting
   - Recent jobs list with status indicators
   - Navigation to job history

2. **Job Detail Page** (`/backfill/$jobId.tsx`)
   - Real-time job progress monitoring
   - Task breakdown by status
   - Failed task details with error messages
   - Retry and cancel functionality

3. **Job History Page** (`/backfill/history.tsx`)
   - List of all backfill jobs
   - Filtering by windfarm and status
   - Pagination support
   - Quick navigation to job details

4. **Data Availability Page** (`/backfill/availability.tsx`)
   - Monthly data coverage visualization
   - Heatmap display per source
   - Identifies gaps needing backfill

### API Client (`/lib/backfill-api.ts`)
- Type-safe API client using TypeScript interfaces
- Methods for all backfill operations:
  - `createJob()`: Start new backfill
  - `preview()`: Preview before creating
  - `listJobs()`: Get job list with filters
  - `getJobStatus()`: Get detailed job status
  - `retryTasks()`: Retry failed tasks
  - `checkAvailability()`: Check data coverage
  - `cancelJob()`: Cancel running job
  - `deleteJob()`: Delete completed job
  - `resetStuckTasks()`: Reset stuck tasks

### UI Components
- WindfarmSelector: Dropdown for windfarm selection
- Status badges: Visual indicators for job/task status
- Progress bars: Show completion percentage
- Alert messages: Display errors and warnings

## Running the System with Celery

### Prerequisites

1. **Redis/Valkey** - Message broker for Celery
   ```bash
   # Install Redis locally (macOS)
   brew install redis
   brew services start redis
   
   # Or use Docker
   docker run -d -p 6379:6379 redis:7-alpine
   ```

2. **Environment Configuration**
   ```bash
   # .env file for local development
   CELERY_BROKER_URL=redis://localhost:6379/0
   CELERY_RESULT_BACKEND=redis://localhost:6379/1
   VALKEY_PUBLIC_HOST=localhost
   VALKEY_PUBLIC_PORT=6379
   VALKEY_PASSWORD=
   ```

### Starting the Services

#### 1. Start the API Server
```bash
# Using Make
make run-api

# Or directly
poetry run python scripts/start.py
```

#### 2. Start Celery Worker
```bash
# Using Make (recommended)
make run-worker

# Or with specific options
poetry run celery -A app.celery_app worker \
  --loglevel=info \
  --queues=default,backfill,backfill_high \
  --concurrency=4

# For debugging
make run-worker-verbose
```

#### 3. Start Flower Monitoring (Optional)
```bash
# Using Make
make run-flower

# Or directly
poetry run celery -A app.celery_app flower \
  --port=5555 \
  --basic_auth=admin:admin

# Access at: http://localhost:5555
```

### Quick Start All Services
```bash
# Start all services in tmux sessions
make run-all

# Stop all services
make stop-all
```

### Monitoring & Management

#### Check Worker Status
```bash
# List active workers
poetry run celery -A app.celery_app inspect active

# View registered tasks
poetry run celery -A app.celery_app inspect registered

# Check queue depth
make dev-inspect-queue
```

#### Emergency Controls
```bash
# Purge all queued tasks
make dev-purge-queue

# Reset database (development only)
make dev-reset-db
```

## Architecture

### Core Components with Celery Integration

#### 1. Database Models (`app/models/backfill_job.py`)

**BackfillJob Model**
- **Purpose**: Represents a backfill operation for a windfarm
- **Key Fields**:
  - `id`: Primary key
  - `windfarm_id`: Foreign key to Windfarm
  - `start_date` / `end_date`: Date range for backfill
  - `status`: Current job status (pending, in_progress, completed, failed, partially_completed)
  - `total_tasks` / `completed_tasks` / `failed_tasks`: Task counters
  - `job_metadata`: JSON field for storing windfarm details and sources
  - `error_message`: Error details if job fails
  - `created_by_id`: User who initiated the job
  - Timestamps: `created_at`, `updated_at`, `started_at`, `completed_at`
- **Relationships**:
  - Many-to-One with Windfarm
  - Many-to-One with User
  - One-to-Many with BackfillTask (cascade delete)

**BackfillTask Model**
- **Purpose**: Represents an individual data fetch task within a job
- **Key Fields**:
  - `id`: Primary key
  - `job_id`: Foreign key to BackfillJob (cascade delete)
  - `generation_unit_id`: Foreign key to GenerationUnit
  - `source`: Data source (entsoe, elexon, eia, taipower)
  - `start_date` / `end_date`: Specific date range for this task (monthly chunk)
  - `status`: Task status (pending, in_progress, completed, failed, skipped)
  - `attempt_count` / `max_attempts`: Retry tracking (default max: 3)
  - `records_fetched`: Number of records successfully fetched
  - `error_message`: Error details if task fails
  - `task_metadata`: JSON field for additional context
  - Timestamps: `created_at`, `started_at`, `completed_at`
- **Relationships**:
  - Many-to-One with BackfillJob
  - Many-to-One with GenerationUnit

**Status Enums**
```python
class BackfillJobStatus:
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_COMPLETED = "partially_completed"

class BackfillTaskStatus:
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
```

#### 2. Service Layer (`app/services/backfill_service.py`)

**BackfillService Class**
Core service implementing all backfill business logic.

**Key Methods**:

1. **`create_backfill_job(request: BackfillJobCreate, current_user: User) -> BackfillJob`**
   - Creates a new backfill job with tasks
   - Validates windfarm exists and has generation units
   - Creates monthly chunks for the date range
   - Creates a task for each generation unit × month combination
   - Starts async processing via `asyncio.create_task()`
   - Returns the created job with eager-loaded tasks

2. **`_process_backfill_job(job_id: int)`**
   - Async method that processes all tasks in a job
   - Creates new database session for async context
   - Updates job status to IN_PROGRESS
   - Iterates through pending tasks and processes them
   - Updates job status based on results (COMPLETED, PARTIALLY_COMPLETED, FAILED)
   - Handles exceptions and proper session cleanup

3. **`_process_backfill_task_with_db(task: BackfillTask, db: AsyncSession)`**
   - Processes individual task
   - Updates task status to IN_PROGRESS
   - Fetches generation unit details
   - Routes to appropriate data fetcher based on source
   - Updates task with results or error
   - Implements retry logic (up to max_attempts)

4. **Data Fetchers** (per source):
   - `_fetch_entsoe_data_with_db()`: Fetches from ENTSOE API
   - `_fetch_elexon_data_with_db()`: Fetches from Elexon API
   - `_fetch_eia_data_with_db()`: Fetches from EIA API (placeholder)
   - `_fetch_taipower_data_with_db()`: Fetches from Taipower API (placeholder)

5. **`get_backfill_preview(request: BackfillJobCreate) -> BackfillPreview`**
   - Generates preview without creating job
   - Shows affected generation units
   - Calculates monthly date ranges
   - Estimates processing time

6. **`get_data_availability(windfarm_id: int, year: Optional[int]) -> DataAvailabilityResponse`**
   - Checks existing data availability
   - Returns monthly breakdown per source
   - Calculates coverage percentage

7. **`retry_failed_tasks(job_id: int, task_ids: Optional[List[int]]) -> BackfillJob`**
   - Retries specific or all failed tasks
   - Resets task status to PENDING
   - Restarts job processing

8. **`cancel_backfill_job(job_id: int) -> BackfillJob`**
   - Cancels pending/in-progress job
   - Marks all pending tasks as SKIPPED
   - Updates job status to FAILED with cancellation message

9. **`delete_backfill_job(job_id: int) -> bool`**
   - Deletes job and all tasks (cascade)
   - Only allows deletion of completed/failed jobs
   - Prevents deletion of in-progress jobs

#### 3. API Endpoints (`app/api/v1/endpoints/backfill.py`)

**Core Endpoints**:

1. **`POST /backfill/jobs`**
   - Creates new backfill job and queues to Celery
   - Request: `BackfillJobCreate` (windfarm_id, start_year, end_year, sources[])
   - Response: `BackfillJob` with celery_task_id
   - Processing starts asynchronously in background

2. **`POST /backfill/preview`**
   - Preview what will be backfilled
   - Request: `BackfillJobCreate`
   - Response: `BackfillPreview` with generation units, date ranges, task count

3. **`GET /backfill/jobs`**
   - List backfill jobs with filters
   - Query params: windfarm_id, status, limit, offset
   - Response: List[BackfillJobSummary]

4. **`GET /backfill/jobs/{job_id}`**
   - Get detailed job status with tasks
   - Response: `BackfillStatusResponse` with job, task breakdowns

5. **`POST /backfill/jobs/{job_id}/retry`**
   - Retry failed tasks
   - Request: `BackfillRetryRequest` (optional task_ids[])
   - Response: Updated `BackfillJob`

6. **`GET /backfill/availability/{windfarm_id}`**
   - Check data availability
   - Query params: year (optional)
   - Response: `DataAvailabilityResponse` with monthly coverage

7. **`POST /backfill/jobs/{job_id}/cancel`**
   - Cancel running job
   - Response: Updated `BackfillJob`

8. **`POST /backfill/jobs/{job_id}/reset-stuck`**
   - Reset stuck tasks that are in IN_PROGRESS state
   - Marks them as FAILED so they can be retried
   - Handles tasks stuck for more than 5 minutes
   - Response: Updated `BackfillJob`

9. **`POST /backfill/jobs/{job_id}/process`**
   - Manually trigger processing of a job
   - Useful for restarting jobs that were created but not processed
   - Response: Updated `BackfillJob`

10. **`DELETE /backfill/jobs/{job_id}`**
    - Delete completed/failed job
    - Response: Success confirmation

**New Celery Endpoints**:

11. **`GET /backfill/jobs/{job_id}/celery-status`**
    - Get real-time Celery task status
    - Returns: task state, progress, results
    - States: PENDING, STARTED, PROGRESS, SUCCESS, FAILURE

12. **`POST /backfill/jobs/{job_id}/refresh-progress`**
    - Trigger progress update from all tasks
    - Queues update job to Celery
    - Returns: progress task ID

## Process Flow

### 1. Job Creation Flow
```
1. User selects windfarm and date range
2. API receives BackfillJobCreate request
3. BackfillService validates:
   - Windfarm exists
   - Generation units exist
   - Date range is valid
4. Service creates:
   - BackfillJob record (status: PENDING)
   - BackfillTask records for each unit×month
5. Service commits to database
6. Service reloads job with eager-loaded tasks
7. Service automatically starts synchronous processing (not async)
8. API returns job details to user (with updated status after processing)
```

### 2. Asynchronous Processing Flow with Celery (v2.0)
```
1. Job creation queues task to Celery immediately
2. Celery worker picks up the job from Redis queue
3. Worker updates job status to IN_PROGRESS
4. Worker processes tasks in parallel using Celery chord:
   a. Each task runs independently
   b. Fetches generation unit details
   c. Determines data source
   d. Calls appropriate API client
   e. Stores fetched data (with deduplication check)
   f. Updates task status (COMPLETED/FAILED)
   g. Automatic retry on failure (exponential backoff)
5. After all tasks complete:
   - Finalization task aggregates results
   - Updates job status (COMPLETED/PARTIALLY_COMPLETED/FAILED)
6. User polls status via API or monitors in Flower
```

**Key Improvements:**
- Non-blocking asynchronous processing
- Parallel task execution for better performance
- Automatic retries with exponential backoff
- Real-time monitoring via Flower dashboard
- No request timeout issues

### 3. Monthly Chunking Strategy
```python
# Date range is split into monthly chunks to:
# 1. Avoid API rate limits
# 2. Enable granular retry on failure
# 3. Provide detailed progress tracking

Example: Jan 2023 - Mar 2023 with 2 units
Results in 6 tasks:
- Unit1: Jan 2023
- Unit1: Feb 2023
- Unit1: Mar 2023
- Unit2: Jan 2023
- Unit2: Feb 2023
- Unit2: Mar 2023
```

### 4. Error Handling & Retry Logic

**Task-Level Retry**:
- Each task has `max_attempts` (default: 3)
- On failure:
  - If attempts < max_attempts: status → PENDING (will retry)
  - If attempts >= max_attempts: status → FAILED
- Error message stored for debugging

**Job-Level Status**:
- All tasks completed: COMPLETED
- Some tasks failed: PARTIALLY_COMPLETED
- Critical error: FAILED
- User cancelled: FAILED (with cancellation message)

### 5. Data Storage per Source

**ENTSOE Data**:
```python
# Stored in ENTSOEGenerationData table
- timestamp: DateTime (timezone-aware UTC)
- area_code: String (e.g., "GB", "FR", "DK_1")
- production_type: String ("wind", "solar")
- value: Float (MW)
- unit: String ("MW")

# Special handling for per-unit data:
- EIC codes (starting with digit, containing 'W') trigger per-unit fetching
- Uses Denmark control area code (10Y1001A1001A796) for per-unit queries
- Stores data with appropriate area code (e.g., "DK_1" for Danish windfarms)
```

**Elexon Data**:
```python
# Stored in ElexonGenerationData table
- timestamp: DateTime
- bm_unit: String (generation unit code)
- generation_unit_id: Integer (FK)
- level_from: Float
- level_to: Float
- settlement_date: Date
- settlement_period: Integer
```

**Data Deduplication**:
- Before inserting, checks if record already exists
- For ENTSOE: Unique on (timestamp, area_code, production_type)
- For Elexon: Unique on (timestamp, bm_unit, settlement_period)
- Updates existing records if values differ

## Session Management Considerations

### Critical: Detached Instance Handling
```python
# Problem: After commit, SQLAlchemy objects become detached
# Solution: Reload with eager loading after commit

# Before returning job:
stmt = (
    select(BackfillJob)
    .options(selectinload(BackfillJob.tasks))
    .where(BackfillJob.id == job.id)
)
result = await self.db.execute(stmt)
job = result.scalar_one()
```

### Session Refresh During Processing
```python
# Every 5 tasks, refresh the session to avoid connection issues
task_count += 1
if task_count % 5 == 0:
    logger.info(f"Processed {task_count} tasks, refreshing database session")
    await self.db.commit()
    # Refresh job object to maintain session
    await self.db.refresh(job)
```

### Connection Health Check
```python
# Check if the session is still active before processing
try:
    await db.execute(select(1))
except Exception as e:
    logger.warning(f"Database connection issue detected, attempting to recover: {str(e)}")
    await db.rollback()
```

## API Client Integration

### ENTSOE Client Specifics
- Requires valid area codes (e.g., "GB", "FR", "DE_LU")
- Generation unit must have proper area code in `code` field
- Validates dates are not in future
- Returns pandas DataFrame with generation data

### Validation Requirements
```python
# Valid ENTSOE area codes
VALID_AREA_CODES = [
    "DE_LU", "FR", "ES", "GB", "IT", "NL", "BE", 
    "AT", "CH", "PL", "DK_1", "DK_2", "NO_1", 
    "SE_1", "SE_2", "SE_3", "SE_4"
]

# Generation unit code must match or have metadata:
if generation_unit.code not in VALID_AREA_CODES:
    # Check metadata for override
    area_code = generation_unit.metadata.get('entsoe_area_code')
    if not area_code or area_code not in VALID_AREA_CODES:
        raise ValueError(f"Invalid ENTSOE area code")
```

## Performance Considerations

1. **Monthly Chunking**: Prevents API timeouts and enables granular retry
2. **Async Processing**: Non-blocking job execution
3. **Eager Loading**: Prevents N+1 queries when loading tasks
4. **Batch Inserts**: Bulk insert fetched records per task
5. **Session Management**: Proper async session handling prevents connection leaks

## Security Considerations

1. **Authentication**: All endpoints require authenticated user
2. **Authorization**: Jobs linked to creating user
3. **Input Validation**: Pydantic schemas validate all inputs
4. **API Keys**: Stored in environment variables, not in database
5. **Error Messages**: Sanitized to prevent information leakage

## Monitoring & Debugging

### Key Metrics to Track
- Job creation rate
- Task success/failure rate
- Average processing time per task
- API call failures per source
- Retry attempt distribution

### Logging Points
```python
logger.info(f"Created backfill job {job.id} for windfarm {windfarm.name}")
logger.info(f"Processing task {task.id} for unit {generation_unit.name}")
logger.error(f"Task {task.id} failed: {str(e)}")
logger.warning(f"ENTSOE area code '{area_code}' invalid for unit {generation_unit.name}")
```

## Future Enhancements

1. **Parallel Task Processing**: Process multiple tasks concurrently
2. **Priority Queue**: Allow high-priority jobs to jump queue
3. **Scheduled Backfills**: Cron-based automatic backfills
4. **Incremental Backfill**: Only fetch missing data
5. **Data Validation**: Verify fetched data quality
6. **Notification System**: Email/webhook on completion
7. **Rate Limiting**: Per-API rate limit management
8. **Data Deduplication**: Prevent duplicate records

## Celery Task Architecture

### Task Types

1. **`process_backfill_job`** (Main Orchestrator)
   - Coordinates entire backfill operation
   - Creates parallel task group using Celery chord
   - Monitors overall progress
   - Queues individual tasks for processing

2. **`process_backfill_task`** (Worker Task)
   - Processes single generation unit × month combination
   - Fetches data from external API
   - Implements retry logic with exponential backoff
   - Updates task status in database

3. **`finalize_backfill_job`** (Completion Handler)
   - Runs after all tasks complete
   - Aggregates results from all tasks
   - Updates final job status
   - Calculates statistics

4. **`update_job_progress`** (Progress Monitor)
   - Updates job progress percentages
   - Counts task statuses
   - Can be called periodically for status updates

### Retry Mechanism

```python
# Automatic retry configuration
retry_kwargs = {
    "max_retries": 5,
    "countdown": 60,  # Initial delay: 1 minute
}

# Exponential backoff delays
# 1 min → 2 min → 4 min → 8 min → 16 min
```

### Queue Strategy

- **`default`**: General tasks
- **`backfill`**: Normal priority backfill tasks
- **`backfill_high`**: High priority backfill tasks

## Troubleshooting Guide

### Common Issues and Solutions

1. **Job Stuck in IN_PROGRESS**
   - **Cause**: Process interrupted or crashed
   - **Solution**: Use `/reset-stuck` endpoint to mark stuck tasks as failed, then retry

2. **High Failure Rate**
   - **Cause**: API rate limits or network issues
   - **Solution**: 
     - Check external API status
     - Reduce date range to create smaller jobs
     - Retry failed tasks after waiting

3. **Invalid ENTSOE Area Code Error**
   - **Cause**: Generation unit has incorrect area code
   - **Solution**: Update generation unit code to valid ENTSOE area code (e.g., "GB", "FR", "DK_1")

4. **Duplicate Data Issues**
   - **Cause**: Multiple jobs for same period
   - **Solution**: System automatically checks for existing data before inserting

5. **Session/Connection Errors**
   - **Cause**: Long-running jobs causing database connection timeout
   - **Solution**: System automatically refreshes session every 5 tasks

6. **Job Creation Timeout**
   - **Cause**: Very large date range with many generation units
   - **Solution**: Break into smaller date ranges (e.g., yearly instead of multi-year)

7. **Celery Worker Not Processing Tasks**
   - **Cause**: Worker not running or Redis connection issue
   - **Solution**: 
     - Check worker is running: `celery -A app.celery_app inspect active`
     - Verify Redis: `redis-cli ping`
     - Start worker: `make run-worker`

8. **ImportError: async_session_maker**
   - **Cause**: Incorrect import in Celery tasks
   - **Solution**: Already fixed - uses `get_session_factory()` instead

9. **Tasks Stuck in PENDING**
   - **Cause**: No workers consuming from queue
   - **Solution**: 
     - Start worker with correct queues: `make run-worker`
     - Check queue names match configuration

### Monitoring Checklist

- Check job status regularly via `/backfill/jobs` endpoint
- Monitor failed task count and error messages
- Verify data availability after completion
- Check logs for API rate limit warnings
- Monitor database connection pool usage

## Testing Recommendations

### Unit Tests
- Test job creation with various date ranges
- Test task generation logic
- Test error handling in data fetchers
- Test retry mechanism
- Test cancellation logic
- Test stuck task detection

### Integration Tests
- Test full job lifecycle
- Test synchronous processing
- Test database transactions
- Test API client interactions
- Test deduplication logic

### Load Tests
- Test with large date ranges (multiple years)
- Test with many generation units
- Test session refresh mechanism
- Test database connection pooling