"""Celery configuration for the application."""

import os
from typing import Any, Dict

# Get settings from environment or use defaults (local Redis for development)
# Support both VALKEY_ and REDIS_ prefixes for flexibility
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", os.getenv("REDIS_PASSWORD", ""))
VALKEY_PUBLIC_HOST = os.getenv("VALKEY_PUBLIC_HOST", os.getenv("REDIS_HOST", "localhost"))
VALKEY_PUBLIC_PORT = os.getenv("VALKEY_PUBLIC_PORT", os.getenv("REDIS_PORT", "6379"))
VALKEY_USER = os.getenv("VALKEY_USER", os.getenv("REDIS_USER", ""))

# Support direct REDIS_URL if provided (Railway often provides this)
REDIS_URL = os.getenv("REDIS_URL", "")

# Broker settings (Valkey/Redis)
if REDIS_URL:
    # Use the provided REDIS_URL directly
    redis_url = REDIS_URL
elif VALKEY_PASSWORD:
    # Build connection string with auth
    redis_url = f"redis://{VALKEY_USER}:{VALKEY_PASSWORD}@{VALKEY_PUBLIC_HOST}:{VALKEY_PUBLIC_PORT}"
else:
    # Build connection string without auth
    redis_url = f"redis://{VALKEY_PUBLIC_HOST}:{VALKEY_PUBLIC_PORT}"

broker_url = os.getenv("CELERY_BROKER_URL", f"{redis_url}/0")
result_backend = os.getenv("CELERY_RESULT_BACKEND", f"{redis_url}/1")

# Task settings
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
timezone = "UTC"
enable_utc = True

# Task execution settings
task_track_started = True
task_time_limit = 3600  # 1 hour hard limit
task_soft_time_limit = 3000  # 50 minutes soft limit
task_acks_late = True  # Tasks will be acknowledged after they have been executed
worker_prefetch_multiplier = 1  # Disable prefetching for long-running tasks

# Retry settings
task_autoretry_for = (Exception,)
task_max_retries = 3
task_default_retry_delay = 60  # 1 minute

# Result backend settings
result_expires = 86400  # Results expire after 1 day
result_persistent = True  # Store results even after they're fetched

# Worker settings
worker_max_tasks_per_child = 100  # Restart worker after 100 tasks to prevent memory leaks
worker_disable_rate_limits = False
worker_send_task_events = True  # Send events for monitoring

# Beat schedule (for periodic tasks if needed)
beat_schedule: Dict[str, Any] = {}

# Queue routing
task_routes = {
    "app.tasks.backfill.*": {"queue": "backfill"},
    "app.tasks.backfill.high_priority": {"queue": "backfill_high"},
}

# Queue configuration
task_default_queue = "default"
task_queues = {
    "default": {
        "exchange": "default",
        "exchange_type": "direct",
        "routing_key": "default",
    },
    "backfill": {
        "exchange": "backfill",
        "exchange_type": "direct",
        "routing_key": "backfill",
        "priority": 5,
    },
    "backfill_high": {
        "exchange": "backfill",
        "exchange_type": "direct",
        "routing_key": "backfill_high",
        "priority": 10,
    },
}

# Monitoring
worker_send_task_events = True
task_send_sent_event = True

# Error handling
task_reject_on_worker_lost = True
task_ignore_result = False

# Optimization for long-running tasks
worker_pool = "prefork"  # Use prefork for CPU-bound tasks
worker_concurrency = 4  # Number of concurrent workers

# Redis/Valkey specific settings
redis_max_connections = 20
redis_socket_connect_timeout = 30.0
redis_socket_keepalive = True
redis_socket_keepalive_options = {
    1: 3,  # TCP_KEEPIDLE
    2: 3,  # TCP_KEEPINTVL
    3: 3,  # TCP_KEEPCNT
}
redis_retry_on_timeout = True
redis_backend_health_check_interval = 30

# Broker connection retry settings
broker_connection_retry_on_startup = True
broker_connection_max_retries = 10

# Logging
worker_log_format = "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s"
worker_task_log_format = "[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s"

# Compression
result_compression = "gzip"
task_compression = "gzip"