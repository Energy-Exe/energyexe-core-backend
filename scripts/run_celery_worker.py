#!/usr/bin/env python
"""Script to run Celery worker."""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Set up environment
os.environ.setdefault("CELERY_CONFIG_MODULE", "app.core.celery_config")

from celery import Celery
from celery.bin import worker

from app.celery_app import celery_app


def main():
    """Run Celery worker."""
    print("Starting Celery worker...")
    print(f"Broker URL: {celery_app.conf.broker_url}")
    print(f"Result Backend: {celery_app.conf.result_backend}")
    
    # Create worker instance
    celery_worker = worker.worker(app=celery_app)
    
    # Configure worker options
    options = {
        "loglevel": "INFO",
        "traceback": True,
        "queues": ["default", "backfill", "backfill_high"],
        "concurrency": 4,  # Number of worker processes
        "pool": "prefork",  # Use prefork pool for better stability
        "task_events": True,  # Send task events for monitoring
        "beat": False,  # Don't run beat scheduler
        "without_gossip": True,  # Disable gossip for better performance
        "without_mingle": True,  # Disable synchronization on startup
        "without_heartbeat": True,  # Disable heartbeat for better performance
    }
    
    # Run worker
    celery_worker.run(**options)


if __name__ == "__main__":
    main()