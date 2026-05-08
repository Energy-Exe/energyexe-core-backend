#!/usr/bin/env python3
"""Wrapper script for running anomaly/opportunity detection jobs from cron.

Usage:
    python scripts/jobs/run_detection_jobs.py performance-pipeline
    python scripts/jobs/run_detection_jobs.py opportunity-detection

Each detection service writes its own ImportJobExecution row, so this script
just opens a session, calls the service, and reports the outcome.
"""

import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from app.core.database import get_session_factory


JOBS = {
    "performance-pipeline": {
        "description": "Performance anomaly detection across all operational windfarms",
        "service_path": "app.services.performance_pipeline_service.PerformancePipelineService",
        "method": "run_pipeline_batch",
        "kwargs": {},
    },
    "opportunity-detection": {
        "description": "Opportunity detection (OPS-01..MKT-03), 24-month rolling window",
        "service_path": "app.services.opportunity_detection_service.OpportunityDetectionService",
        "method": "run_detection_job",
        "kwargs": {"period_months": 24},
    },
}


async def run(job_name: str) -> int:
    if job_name not in JOBS:
        print(f"❌ Unknown job: {job_name}")
        print(f"Available: {', '.join(JOBS.keys())}")
        return 1

    cfg = JOBS[job_name]
    module_path, class_name = cfg["service_path"].rsplit(".", 1)
    module = __import__(module_path, fromlist=[class_name])
    ServiceClass = getattr(module, class_name)

    print(f"=== {job_name}: {cfg['description']} ===")

    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        service = ServiceClass(db)
        try:
            result = await getattr(service, cfg["method"])(**cfg["kwargs"])
            print(f"✅ Completed: {result}")
            return 0
        except Exception as exc:
            print(f"❌ Failed: {exc}")
            return 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: run_detection_jobs.py <job_name>")
        for name, cfg in JOBS.items():
            print(f"  {name:25} - {cfg['description']}")
        sys.exit(1)

    sys.exit(asyncio.run(run(sys.argv[1])))
