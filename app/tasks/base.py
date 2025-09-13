"""Base task class with common functionality."""

from typing import Any, Dict, Optional

import structlog
from celery import Task
from celery.exceptions import MaxRetriesExceededError, Retry

from app.celery_app import celery_app

logger = structlog.get_logger()


class BaseTask(Task):
    """Base task with automatic retries and error handling."""
    
    autoretry_for = (Exception,)
    retry_kwargs = {"max_retries": 3}
    retry_backoff = True
    retry_backoff_max = 600  # 10 minutes max backoff
    retry_jitter = True  # Add randomness to avoid thundering herd
    
    def __init__(self):
        """Initialize the base task."""
        super().__init__()
        self.logger = structlog.get_logger().bind(task_name=self.name)
    
    def before_start(self, task_id: str, args: tuple, kwargs: dict, **options):
        """Called before task execution starts."""
        self.logger.info(
            "Task starting",
            task_id=task_id,
            args=args,
            kwargs=kwargs,
        )
    
    def on_success(self, retval: Any, task_id: str, args: tuple, kwargs: dict, **options):
        """Called on successful task execution."""
        self.logger.info(
            "Task completed successfully",
            task_id=task_id,
            result=retval,
        )
    
    def on_failure(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo: Any, **options):
        """Called on task failure."""
        self.logger.error(
            "Task failed",
            task_id=task_id,
            error=str(exc),
            exc_info=einfo,
        )
    
    def on_retry(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo: Any, **options):
        """Called when task is retried."""
        self.logger.warning(
            "Task retrying",
            task_id=task_id,
            error=str(exc),
            retry_count=self.request.retries,
        )
    
    def update_progress(
        self,
        current: int,
        total: int,
        status: str = "Processing",
        meta: Optional[Dict[str, Any]] = None,
    ):
        """Update task progress."""
        progress_data = {
            "current": current,
            "total": total,
            "percent": round((current / total * 100) if total > 0 else 0, 2),
            "status": status,
        }
        
        if meta:
            progress_data.update(meta)
        
        self.update_state(state="PROGRESS", meta=progress_data)
        
        self.logger.info(
            "Progress updated",
            task_id=self.request.id,
            progress=progress_data,
        )
        
        return progress_data


class BackfillTask(BaseTask):
    """Specialized task for backfill operations."""
    
    # Longer timeouts for backfill operations
    soft_time_limit = 3000  # 50 minutes
    time_limit = 3600  # 1 hour
    
    # More retries for external API failures
    retry_kwargs = {
        "max_retries": 5,
        "countdown": 60,  # Wait 1 minute before first retry
    }
    
    # Specific exceptions to retry on
    autoretry_for = (
        ConnectionError,
        TimeoutError,
        Exception,  # Catch all for now, can be more specific
    )
    
    def calculate_retry_delay(self, retry_count: int) -> int:
        """Calculate exponential backoff delay."""
        # Exponential backoff: 1min, 2min, 4min, 8min, 16min
        base_delay = 60  # 1 minute
        max_delay = 960  # 16 minutes
        
        delay = min(base_delay * (2 ** retry_count), max_delay)
        
        # Add jitter (±10% randomness)
        import random
        jitter = delay * 0.1
        delay = int(delay + random.uniform(-jitter, jitter))
        
        return delay