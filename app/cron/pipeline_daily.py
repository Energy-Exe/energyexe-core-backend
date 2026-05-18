"""Daily performance-pipeline scheduler (PRE-D for spec items 1-6).

Runs the full 6-module pipeline + generation concentration + opportunity
detection for all operational windfarms every night at the configured time
(default 03:00 UTC, after weather + generation imports finish ~02:30).

Wiring: in `app/main.py` lifespan, call `start_pipeline_scheduler(app)` at
startup and `stop_pipeline_scheduler(app)` at shutdown. Scheduler is opt-in
via the `PIPELINE_DAILY_ENABLED` env var so it doesn't run on dev machines
unintentionally.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = structlog.get_logger(__name__)

# Module-level scheduler — only one instance per process.
_scheduler: Optional[AsyncIOScheduler] = None


def _is_enabled() -> bool:
    return os.getenv("PIPELINE_DAILY_ENABLED", "false").lower() == "true"


def _get_cron_kwargs() -> dict:
    """Read schedule from env (defaults: 03:00 UTC daily)."""
    return {
        "hour": int(os.getenv("PIPELINE_DAILY_HOUR", "3")),
        "minute": int(os.getenv("PIPELINE_DAILY_MINUTE", "0")),
        "timezone": "UTC",
    }


async def run_pipeline_job() -> None:
    """One full pipeline pass over all operational windfarms.

    Failure of one windfarm does not abort the rest — the orchestrator wraps
    each windfarm in its own try/except. Job-level failures are logged and
    the alert_service is notified.
    """
    job_started = datetime.now(timezone.utc)
    logger.info("pipeline_daily_job_started", at=job_started.isoformat())

    from app.core.database import get_session_factory
    from app.services.performance_pipeline_service import PerformancePipelineService

    session_factory = get_session_factory()

    try:
        async with session_factory() as db:
            svc = PerformancePipelineService(db)
            result = await svc.run_pipeline_batch()
        logger.info(
            "pipeline_daily_job_complete",
            duration_s=(datetime.now(timezone.utc) - job_started).total_seconds(),
            **result,
        )
    except Exception as exc:
        duration_s = (datetime.now(timezone.utc) - job_started).total_seconds()
        logger.error(
            "pipeline_daily_job_failed",
            duration_s=duration_s,
            error=str(exc),
        )
        # Best-effort alert. If alerting itself fails, the structured log line
        # above is the durable signal.
        try:
            async with session_factory() as db:
                from app.services.alert_service import AlertService

                # Import dynamically since alert API surface may evolve.
                alert_svc = AlertService(db) if hasattr(AlertService, "__init__") else None
                if alert_svc is not None and hasattr(alert_svc, "create_system_alert"):
                    await alert_svc.create_system_alert(
                        title="Pipeline daily job failed",
                        message=str(exc),
                        severity="HIGH",
                    )
        except Exception as alert_exc:
            logger.warning("pipeline_alert_send_failed", error=str(alert_exc))


def start_pipeline_scheduler() -> None:
    """Start the scheduler (idempotent). No-op if PIPELINE_DAILY_ENABLED is false."""
    global _scheduler

    if not _is_enabled():
        logger.info("pipeline_daily_scheduler_disabled")
        return

    if _scheduler is not None and _scheduler.running:
        logger.info("pipeline_daily_scheduler_already_running")
        return

    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        run_pipeline_job,
        trigger=CronTrigger(**_get_cron_kwargs()),
        id="pipeline_daily",
        name="Daily performance pipeline",
        replace_existing=True,
        max_instances=1,         # never overlap two pipeline runs
        coalesce=True,           # if the process was down, run once not N times
        misfire_grace_time=3600,
    )
    _scheduler.start()
    logger.info(
        "pipeline_daily_scheduler_started",
        next_run=str(_scheduler.get_job("pipeline_daily").next_run_time),
    )


def stop_pipeline_scheduler() -> None:
    """Shut down the scheduler if running."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("pipeline_daily_scheduler_stopped")
    _scheduler = None
