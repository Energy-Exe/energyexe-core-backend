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

from app.celery_app import celery_app


def main():
    """Run Celery worker."""
    print("Starting Celery worker...")
    print(f"Broker URL: {celery_app.conf.broker_url}")
    print(f"Result Backend: {celery_app.conf.result_backend}")
    print()

    # Use celery_app.Worker() - the modern way to start a worker programmatically
    worker = celery_app.Worker(
        loglevel='INFO',
        queues=['default', 'backfill', 'backfill_high'],
        concurrency=4,
        pool='prefork',
        task_events=True,
        without_gossip=True,
        without_mingle=True,
        without_heartbeat=True,
    )

    # Start the worker
    worker.start()


if __name__ == "__main__":
    main()