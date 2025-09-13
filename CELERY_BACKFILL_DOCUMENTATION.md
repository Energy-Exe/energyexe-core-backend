# Celery-Based Backfill System Documentation

## Overview

The backfill system has been upgraded to use Celery for asynchronous task processing, providing better scalability, reliability, and monitoring capabilities. The system uses Valkey (Redis fork) as the message broker and result backend.

## Architecture

### Components

1. **Valkey (Message Broker)**: Queues tasks and stores results
2. **Celery Workers**: Process backfill tasks asynchronously
3. **Flower**: Web-based monitoring dashboard
4. **FastAPI**: REST API for job management
5. **PostgreSQL**: Stores job and task metadata

### Task Flow

```
User Request → API → Create Job → Queue to Celery → Valkey → Workers
                ↓                                        ↓
            Return Job ID                        Process Tasks
                ↓                                        ↓
            Poll Status                          Update Database
```

## Setup and Configuration

### Environment Variables

Add to your `.env` file:

```env
# Valkey/Redis Configuration
VALKEY_PUBLIC_HOST=valkey-production-515f.up.railway.app
VALKEY_PUBLIC_PORT=6379
VALKEY_PASSWORD=roKX3R37u09uQhjf~YjWnScP11nrdU7p
VALKEY_USER=default

# Celery Configuration (optional, uses Valkey settings by default)
CELERY_BROKER_URL=redis://default:password@host:port/0
CELERY_RESULT_BACKEND=redis://default:password@host:port/1
```

### Installation

```bash
# Install dependencies
poetry install

# Run database migrations
poetry run alembic upgrade head
```

## Running the System

### Start All Services

```bash
# Using Make (recommended)
make run-all  # Starts API, Worker, and Flower in tmux sessions

# Or run individually:
make run-api     # Start FastAPI server
make run-worker  # Start Celery worker
make run-flower  # Start Flower monitoring
```

### Manual Commands

```bash
# Start API server
poetry run python scripts/start.py

# Start Celery worker
poetry run celery -A app.celery_app worker \
  --loglevel=info \
  --queues=default,backfill,backfill_high \
  --concurrency=4

# Start Flower monitoring (http://localhost:5555)
poetry run celery -A app.celery_app flower \
  --port=5555 \
  --basic_auth=admin:admin
```

## API Endpoints

### Core Endpoints

- `POST /api/v1/backfill/jobs` - Create new backfill job (queues to Celery)
- `GET /api/v1/backfill/jobs/{job_id}` - Get job status
- `GET /api/v1/backfill/jobs/{job_id}/celery-status` - Get Celery task status
- `POST /api/v1/backfill/jobs/{job_id}/refresh-progress` - Update progress

### Job Management

- `POST /api/v1/backfill/jobs/{job_id}/retry` - Retry failed tasks
- `POST /api/v1/backfill/jobs/{job_id}/cancel` - Cancel running job
- `POST /api/v1/backfill/jobs/{job_id}/reset-stuck` - Reset stuck tasks
- `DELETE /api/v1/backfill/jobs/{job_id}` - Delete completed job

## Task Structure

### Task Hierarchy

```
BackfillJob (orchestrator)
    ├── BackfillTask 1 (Generation Unit 1, Month 1)
    ├── BackfillTask 2 (Generation Unit 1, Month 2)
    ├── BackfillTask 3 (Generation Unit 2, Month 1)
    └── BackfillTask N (Generation Unit N, Month N)
```

### Task Types

1. **process_backfill_job**: Main orchestrator task
   - Creates task group for parallel processing
   - Monitors overall progress
   - Finalizes job on completion

2. **process_backfill_task**: Individual data fetch task
   - Fetches data from external API
   - Stores in database
   - Updates task status

3. **finalize_backfill_job**: Completion handler
   - Aggregates results
   - Updates final job status
   - Calculates statistics

## Monitoring

### Flower Dashboard

Access at: http://localhost:5555 (default credentials: admin/admin)

Features:
- Real-time task monitoring
- Worker status and resource usage
- Task success/failure rates
- Queue depth visualization
- Task details and arguments

### Celery Task States

- **PENDING**: Task waiting in queue
- **STARTED**: Task execution started
- **PROGRESS**: Task in progress (with percentage)
- **SUCCESS**: Task completed successfully
- **FAILURE**: Task failed
- **RETRY**: Task scheduled for retry

### Progress Tracking

```javascript
// Frontend polling example
const pollJobStatus = async (jobId) => {
  const celeryStatus = await backfillApi.getCeleryStatus(jobId)
  
  if (celeryStatus.state === 'PROGRESS') {
    // Update UI with progress
    updateProgress(celeryStatus.progress)
  } else if (celeryStatus.state === 'SUCCESS') {
    // Job completed
    showSuccess(celeryStatus.result)
  } else if (celeryStatus.state === 'FAILURE') {
    // Job failed
    showError(celeryStatus.error)
  }
  
  // Continue polling if not finished
  if (!celeryStatus.ready) {
    setTimeout(() => pollJobStatus(jobId), 2000)
  }
}
```

## Error Handling

### Retry Mechanism

Tasks automatically retry with exponential backoff:
- Initial retry: 1 minute
- Subsequent retries: 2, 4, 8, 16 minutes
- Maximum retries: 5 (configurable)

### Failed Task Recovery

```bash
# Retry failed tasks via API
curl -X POST /api/v1/backfill/jobs/{job_id}/retry

# Reset stuck tasks
curl -X POST /api/v1/backfill/jobs/{job_id}/reset-stuck

# Inspect active tasks
poetry run celery -A app.celery_app inspect active

# Purge all queued tasks (emergency)
poetry run celery -A app.celery_app purge -f
```

## Performance Tuning

### Worker Configuration

```python
# app/core/celery_config.py

# Adjust concurrency based on workload
worker_concurrency = 4  # Number of concurrent workers

# Optimize for long-running tasks
task_acks_late = True
worker_prefetch_multiplier = 1

# Prevent memory leaks
worker_max_tasks_per_child = 100
```

### Queue Priorities

- **default**: Standard priority tasks
- **backfill**: Normal backfill tasks (priority: 5)
- **backfill_high**: High priority backfill (priority: 10)

### Scaling

```bash
# Run multiple workers
celery -A app.celery_app worker --queues=backfill --concurrency=8 -n worker1
celery -A app.celery_app worker --queues=backfill --concurrency=8 -n worker2

# Horizontal scaling with Docker
docker-compose up --scale celery_worker=4
```

## Troubleshooting

### Common Issues

1. **Tasks not processing**
   - Check worker is running: `celery -A app.celery_app inspect active`
   - Verify Valkey connection: `redis-cli -h host -p port ping`
   - Check queue depth: `celery -A app.celery_app inspect reserved`

2. **Task failures**
   - Check worker logs: `journalctl -u celery-worker -f`
   - View task details in Flower
   - Check error_message in database

3. **Memory issues**
   - Reduce worker concurrency
   - Enable task result expiration
   - Restart workers periodically

### Debug Commands

```bash
# Inspect all queues
celery -A app.celery_app inspect active_queues

# View task details
celery -A app.celery_app inspect registered

# Check worker stats
celery -A app.celery_app inspect stats

# View scheduled tasks
celery -A app.celery_app inspect scheduled

# Force worker shutdown
celery -A app.celery_app control shutdown
```

## Development

### Testing Celery Tasks

```python
# tests/test_backfill_tasks.py
from celery import current_app
from app.tasks.backfill import process_backfill_task

def test_backfill_task():
    # Use eager mode for testing
    current_app.conf.task_always_eager = True
    
    # Test task execution
    result = process_backfill_task.apply(args=[task_id])
    assert result.successful()
```

### Local Development

```bash
# Use local Redis/Valkey
docker run -d -p 6379:6379 valkey/valkey:7-alpine

# Update .env
VALKEY_PUBLIC_HOST=localhost
VALKEY_PUBLIC_PORT=6379
VALKEY_PASSWORD=

# Run with debug logging
make run-worker-verbose
```

## Migration from Synchronous Processing

The system maintains backward compatibility:

```python
# Force synchronous processing (not recommended)
service.create_backfill_job(request, user, use_celery=False)
```

To fully migrate:
1. Ensure Celery workers are running
2. Update frontend to poll for status
3. Remove synchronous processing code
4. Monitor performance and adjust workers

## Best Practices

1. **Task Design**
   - Keep tasks idempotent
   - Store progress in database
   - Use meaningful task IDs

2. **Error Handling**
   - Log errors with context
   - Set appropriate retry limits
   - Monitor failure patterns

3. **Performance**
   - Batch database operations
   - Use connection pooling
   - Monitor queue depths

4. **Monitoring**
   - Set up alerts for failed tasks
   - Track processing times
   - Monitor worker health

## Future Enhancements

1. **Beat Scheduler**: Automated periodic backfills
2. **Priority Queues**: Dynamic priority based on data age
3. **Result Caching**: Cache frequently accessed data
4. **WebSocket Updates**: Real-time progress updates
5. **Distributed Locking**: Prevent duplicate processing
6. **Task Chaining**: Complex workflows with dependencies