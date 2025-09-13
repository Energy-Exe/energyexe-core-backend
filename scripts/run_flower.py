#!/usr/bin/env python
"""Script to run Flower monitoring dashboard."""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from flower.command import FlowerCommand

from app.celery_app import celery_app
from app.core.config import get_settings

settings = get_settings()


def main():
    """Run Flower monitoring dashboard."""
    print("Starting Flower monitoring dashboard...")
    print(f"Broker URL: {celery_app.conf.broker_url}")
    print("Dashboard will be available at: http://localhost:5555")
    
    # Create Flower command
    flower = FlowerCommand()
    
    # Configure Flower options
    options = [
        "flower",
        f"--broker={celery_app.conf.broker_url}",
        "--port=5555",
        "--address=0.0.0.0",
        f"--broker_api=redis://default:{settings.VALKEY_PASSWORD}@{settings.VALKEY_PUBLIC_HOST}:{settings.VALKEY_PUBLIC_PORT}/",
        "--purge_offline_workers=60",  # Remove offline workers after 60 seconds
        "--persistent=True",  # Save state between restarts
        "--db=flower.db",  # Database file for persistence
        "--max_tasks=10000",  # Maximum number of tasks to keep in memory
        "--basic_auth=admin:admin",  # Basic authentication (change in production!)
    ]
    
    # Run Flower
    flower.execute_from_commandline(options)


if __name__ == "__main__":
    main()