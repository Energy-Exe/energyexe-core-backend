"""Daily performance-pipeline job (PRE-D for spec items 1-6).

Runs the full 6-module pipeline + generation concentration + opportunity
detection for all operational windfarms.

**Scheduling lives in AWS**, not in this process. An EventBridge rule runs
`scripts/jobs/run_pipeline_daily.py` as a one-off ECS task (see
`infra/pipeline_daily.tf`); this module only provides the job body.

It used to run on an in-process APScheduler with the default *in-memory*
jobstore, which meant a fresh process computed `next_run_time` forward from now
with no record of a missed fire — so any deploy, OOM or crash spanning the fire
time lost that night with no recovery, and a ~3h CPU-bound job ran inside the
single uvicorn worker serving live traffic.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import structlog

from app.core.observability import capture_exception, cron_checkin

logger = structlog.get_logger(__name__)

# Exit codes — returned by run_pipeline_job() and used directly as the process
# exit status, which is what ECS reports and what the failure alarm keys on.
EXIT_OK = 0
EXIT_BATCH_FAILED = 1
EXIT_DETECTION_FAILED = 2


def _get_cron_kwargs() -> dict:
    """Read schedule from env (defaults: 03:00 UTC daily).

    This no longer *drives* anything — EventBridge owns the schedule. It only
    tells GlitchTip when to expect a check-in, so `PIPELINE_DAILY_HOUR` /
    `PIPELINE_DAILY_MINUTE` on the ECS task definition must match the cron
    expression in `infra/pipeline_daily.tf`, or GlitchTip alerts on runs that
    happened perfectly well.
    """
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
        # run overruns. max_runtime was 180 against a measured 170 (2026-08-16:
        # 8010s batch + 2201s detection) — ten minutes from false-alarming, and
        # the full-fleet runtime grows with the fleet. 300 buys real headroom.
        "checkin_margin": 30,
        "max_runtime": 300,
    }


async def run_pipeline_job(
    windfarm_ids: list[int] | None = None,
    period_months: int = 24,
    skip_detection: bool = False,
) -> int:
    """One full pipeline pass + opportunity detection over operational windfarms.

    Returns a process exit code (``EXIT_OK`` / ``EXIT_BATCH_FAILED`` /
    ``EXIT_DETECTION_FAILED``). This return value is the *only* thing that makes
    the ECS task-failure alarm meaningful — an earlier version swallowed both
    failure paths and returned ``None``, so every run looked successful.

    Sequence:
      1. ``PerformancePipelineService.run_pipeline_batch()`` — the 6-module
         performance pipeline. Failure of one windfarm does not abort the rest;
         the orchestrator wraps each windfarm in its own try/except.
      2. ``OpportunityDetectionService.run_detection_job()`` — opportunity
         detection, run *after* the batch so it consumes fresh performance data.

    Error handling:
      * A *batch* failure is the job-level failure: detection is **skipped**
        (it depends on batch output).
      * A *detection* failure does NOT mask a successful batch — the batch's
        results stand — but it still fails the job, so it is visible. The CLI
        backstop (``scripts/jobs/run_detection_jobs.py opportunity-detection``)
        is available for a manual re-run.

    ``windfarm_ids`` scopes both phases to a subset, which is what makes a
    minutes-long smoke test possible against a job that normally runs ~3h.
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
            result = await svc.run_pipeline_batch(windfarm_ids=windfarm_ids)
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
        # Batch failed: skip detection (it depends on the batch's output).
        return EXIT_BATCH_FAILED

    # ── Monthly generation aggregate (OPEX/MWh denominator for FIN-02/03 and
    # the report scorecards) ───────────────────────────────────────────────
    # Best-effort: a failed refresh leaves the previous snapshot in place and is
    # logged + reported, but never blocks detection (which would then read
    # yesterday's aggregate — annual filings do not care about one day).
    from app.services.generation_monthly_view import refresh_generation_monthly_view

    try:
        refresh_summary = await refresh_generation_monthly_view()
        logger.info("pipeline_daily_generation_monthly_refreshed", **refresh_summary)
    except Exception as exc:
        logger.error("pipeline_daily_generation_monthly_refresh_failed", error=str(exc))
        capture_exception(exc)

    # ── Opportunity detection (runs only after a successful batch) ────────
    # Isolated from the batch result: a detection failure is logged + alerted
    # but does NOT mask the batch's success reporting. The CLI backstop
    # (scripts/jobs/run_detection_jobs.py opportunity-detection) covers re-runs.
    exit_code = EXIT_OK
    if skip_detection:
        logger.info("pipeline_daily_detection_skipped")
    else:
        detection_started = datetime.now(timezone.utc)
        try:
            async with session_factory() as db:
                detection_svc = OpportunityDetectionService(db)
                detection_result = await detection_svc.run_detection_job(
                    windfarm_ids=windfarm_ids, period_months=period_months
                )
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
            exit_code = EXIT_DETECTION_FAILED

    # Close out the cron monitor: "ok" only if both batch and detection passed.
    cron_checkin(
        _CRON_MONITOR_SLUG,
        status="ok" if exit_code == EXIT_OK else "error",
        check_in_id=check_in_id,
    )
    logger.info(
        "pipeline_daily_job_complete",
        duration_s=(datetime.now(timezone.utc) - job_started).total_seconds(),
        exit_code=exit_code,
    )
    return exit_code
