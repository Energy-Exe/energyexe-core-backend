# Railway Deployment Guide for Celery

This guide explains how to deploy the EnergyExe backend with Celery workers on Railway.

## Architecture

You'll need to create **3 separate Railway services** from the same GitHub repository:

1. **API Service** - The main FastAPI application
2. **Celery Worker** - Processes background tasks
3. **Flower (Optional)** - Monitoring dashboard for Celery

All services use the same Dockerfile but different environment variables to determine their role.

## Step-by-Step Deployment

### 1. Deploy API Service

Your existing API service should continue to work. Just ensure these environment variables are set:

```bash
RAILWAY_SERVICE_TYPE=api
PORT=<Railway provides this>
DATABASE_URL=<your-postgres-url>
REDIS_URL=redis://<your-valkey-host>:<port>
# Or if using local Redis for development
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/1
```

### 2. Deploy Celery Worker Service

Create a new Railway service from the same GitHub repo:

1. In Railway, click "New Service" 
2. Select "GitHub Repo" and choose the same repository
3. Set these environment variables:

```bash
RAILWAY_SERVICE_TYPE=worker
DATABASE_URL=<same-as-api>
REDIS_URL=<same-as-api>
CELERY_WORKER_CONCURRENCY=2
CELERY_WORKER_POOL=prefork
# Copy all other env vars from API service
```

4. **Important**: Remove the domain settings for the worker service (it doesn't need HTTP access)
5. Set health check to "None" or remove it

### 3. Deploy Flower Service (Optional)

For monitoring Celery tasks:

1. Create another new Railway service from the same repo
2. Set environment variables:

```bash
RAILWAY_SERVICE_TYPE=flower
PORT=<Railway provides this>
REDIS_URL=<same-as-api>
FLOWER_USER=admin
FLOWER_PASSWORD=<secure-password>
```

3. Add a domain to access the Flower dashboard
4. Access at: `https://your-flower-domain.railway.app`

## Environment Variables Reference

### Common Variables (All Services Need These)

```bash
# Database
DATABASE_URL=postgresql://user:pass@host:port/dbname

# Redis/Valkey (for Celery)
REDIS_URL=redis://host:port
# OR separate broker/backend URLs
CELERY_BROKER_URL=redis://host:port/0
CELERY_RESULT_BACKEND=redis://host:port/1

# Valkey credentials (if using Railway's Valkey)
VALKEY_PUBLIC_HOST=your-valkey.railway.internal
VALKEY_PUBLIC_PORT=6379
VALKEY_PASSWORD=your-password
VALKEY_USER=default

# Other configs
SECRET_KEY=<your-secret-key>
ENVIRONMENT=production
```

### Service-Specific Variables

#### API Service
```bash
RAILWAY_SERVICE_TYPE=api
API_WORKERS=2
RUN_MIGRATIONS=true  # Set to true for first deployment
```

#### Worker Service
```bash
RAILWAY_SERVICE_TYPE=worker
CELERY_WORKER_CONCURRENCY=2
CELERY_WORKER_POOL=prefork
```

#### Flower Service
```bash
RAILWAY_SERVICE_TYPE=flower
FLOWER_USER=admin
FLOWER_PASSWORD=<secure-password>
```

## Using Railway's Valkey Instance

If you're using Railway's Valkey (Redis fork) service:

1. Create a Valkey service in Railway
2. Use the internal URL for better performance:
   - Internal: `valkey.railway.internal:6379`
   - External: Use the provided public URL

3. Set environment variables:
```bash
VALKEY_PUBLIC_HOST=valkey.railway.internal
VALKEY_PUBLIC_PORT=6379
VALKEY_PASSWORD=<from-railway>
VALKEY_USER=default
```

## Monitoring and Logs

- **API Logs**: Check the API service logs in Railway
- **Worker Logs**: Check the Worker service logs to see task processing
- **Flower Dashboard**: Access via the Flower service URL with basic auth
- **Task Status**: The API endpoints `/api/v1/backfill/jobs/{id}` show task progress

## Scaling

### Horizontal Scaling
- **API**: Increase replicas in Railway service settings
- **Workers**: Deploy multiple worker services or increase `CELERY_WORKER_CONCURRENCY`

### Vertical Scaling
- Adjust Railway service resources (RAM/CPU) as needed

## Troubleshooting

### Workers Not Processing Tasks
1. Check worker logs for connection errors
2. Verify Redis/Valkey connection
3. Ensure `RAILWAY_SERVICE_TYPE=worker` is set
4. Check that queues match between API and worker

### Database Connection Errors
1. Reduce `CELERY_WORKER_CONCURRENCY` if seeing "too many connections"
2. Ensure all services have the same `DATABASE_URL`

### Memory Issues
1. Lower worker concurrency
2. Use `CELERY_WORKER_POOL=solo` for single-threaded processing
3. Increase Railway service memory limits

## Cost Optimization

1. **Development**: Use a single service with supervisor to run both API and worker
2. **Production**: Separate services for better scaling and reliability
3. **Auto-sleep**: Workers can be configured to sleep when idle (Railway Pro feature)

## Example railway.toml (Optional)

If you prefer configuration as code, create a `railway.toml`:

```toml
[build]
builder = "dockerfile"

[deploy]
healthcheckPath = "/health"
healthcheckTimeout = 30
restartPolicyType = "on-failure"
restartPolicyMaxRetries = 10

[environments.production]
RAILWAY_SERVICE_TYPE = "api"
```

## Security Notes

1. Never expose Flower without authentication
2. Use internal Railway URLs when possible
3. Rotate `SECRET_KEY` regularly
4. Use strong passwords for all services
5. Consider VPC/private networking for production