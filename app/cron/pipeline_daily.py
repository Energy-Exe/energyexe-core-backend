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

from app.core.observability import capture_exception, cron_checkin

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


# GlitchTip cron monitor slug — tracks whether the nightly job ran and passed.
_CRON_MONITOR_SLUG = "pipeline-daily"


def _cron_monitor_config() -> dict:
    """Schedule GlitchTip expects this job on, so it can alert on missed runs."""
    cron = _get_cron_kwargs()
    return {
        "schedule": {"type": "crontab", "value": f"{cron['minute']} {cron['hour']} * * *"},
        "timezone": "UTC",
        # Alert if no check-in lands within 30 min of the expected time, or a
        # run overruns 3h (the full-fleet pipeline + detection can be long).
        "checkin_margin": 30,
        "max_runtime": 180,
    }


async def run_pipeline_job() -> None:
    """One full pipeline pass + opportunity detection over all operational windfarms.

    Sequence:
      1. ``PerformancePipelineService.run_pipeline_batch()`` — the 6-module
         performance pipeline. Failure of one windfarm does not abort the rest;
         the orchestrator wraps each windfarm in its own try/except.
      2. ``OpportunityDetectionService.run_detection_job()`` — opportunity
         detection, run *after* the batch so it consumes fresh performance data.

    Error handling:
      * A *batch* failure is the job-level failure: it is logged as
        ``pipeline_daily_job_failed`` and the alert_service is notified.
        Detection is **skipped** in that case (it depends on batch output).
      * A *detection* failure does NOT mask a successful batch. It is logged as
        ``pipeline_daily_detection_failed`` and best-effort alerted, but the
        batch's success reporting still stands. The CLI backstop
        (``scripts/jobs/run_detection_jobs.py opportunity-detection``) remains
        available for a manual re-run.
    """
    job_started = datetime.now(timezone.utc)
    logger.info("pipeline_daily_job_started", at=job_started.isoformat())

    # Open a GlitchTip cron check-in. monitor_config auto-creates the monitor
    # and teaches GlitchTip the schedule, so it alerts if a nightly run never
    # arrives (the silent-failure class) as well as on the explicit failures
    # reported below. No-op when SENTRY_DSN is unset.
    check_in_id = cron_checkin(
        _CRON_MONITOR_SLUG, status="in_progress", monitor_config=_cron_monitor_config()
    )

    from app.core.database import get_session_factory
    from app.services.opportunity_detection_service import OpportunityDetectionService
    from app.services.performance_pipeline_service import PerformancePipelineService

    session_factory = get_session_factory()

    try:
        async with session_factory() as db:
            svc = PerformancePipelineService(db)
            result = await svc.run_pipeline_batch()
        logger.info(
            "pipeline_daily_batch_complete",
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
        # Report to GlitchTip and close the cron monitor as failed — the batch
        # is the job's deliverable, so without it we stop here.
        capture_exception(exc)
        cron_checkin(_CRON_MONITOR_SLUG, status="error", check_in_id=check_in_id)
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
        # Batch failed: skip detection (it depends on the batch's output).
        return

    # ── Opportunity detection (runs only after a successful batch) ────────
    # Isolated from the batch result: a detection failure is logged + alerted
    # but does NOT mask the batch's success reporting. The CLI backstop
    # (scripts/jobs/run_detection_jobs.py opportunity-detection) covers re-runs.
    detection_started = datetime.now(timezone.utc)
    job_status = "ok"
    try:
        async with session_factory() as db:
            detection_svc = OpportunityDetectionService(db)
            detection_result = await detection_svc.run_detection_job()
        logger.info(
            "pipeline_daily_detection_complete",
            duration_s=(datetime.now(timezone.utc) - detection_started).total_seconds(),
            **detection_result,
        )
    except Exception as exc:
        logger.error(
            "pipeline_daily_detection_failed",
            duration_s=(datetime.now(timezone.utc) - detection_started).total_seconds(),
            error=str(exc),
        )
        capture_exception(exc)
        job_status = "error"
        try:
            async with session_factory() as db:
                from app.services.alert_service import AlertService

                alert_svc = AlertService(db) if hasattr(AlertService, "__init__") else None
                if alert_svc is not None and hasattr(alert_svc, "create_system_alert"):
                    await alert_svc.create_system_alert(
                        title="Opportunity detection (daily) failed",
                        message=str(exc),
                        severity="HIGH",
                    )
        except Exception as alert_exc:
            logger.warning("detection_alert_send_failed", error=str(alert_exc))

    # Close out the cron monitor: "ok" only if both batch and detection passed.
    cron_checkin(_CRON_MONITOR_SLUG, status=job_status, check_in_id=check_in_id)
    logger.info(
        "pipeline_daily_job_complete",
        duration_s=(datetime.now(timezone.utc) - job_started).total_seconds(),
    )


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
        max_instances=1,  # never overlap two pipeline runs
        coalesce=True,  # if the process was down, run once not N times
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
