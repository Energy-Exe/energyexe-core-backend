#!/usr/bin/env python3
"""Daily ERA5 weather import, as a standalone job (EPR-121).

Entrypoint for the EventBridge-scheduled ECS task in `infra/weather_daily.tf`.
Nothing scheduled weather before this: the fleet's `weather_data` stopped at
2025-12-31 when the last manual backfill ended, and the nightly pipeline
inner-joins generation with weather, so every 2026 hour was silently dropped.

    python scripts/jobs/run_weather_daily.py                       # today - WEATHER_LAG_DAYS (ERA5T lag)
    python scripts/jobs/run_weather_daily.py --date 2026-01-02
    python scripts/jobs/run_weather_daily.py --start 2026-01-01 --end 2026-03-01
    python scripts/jobs/run_weather_daily.py --windfarm-ids 8806 --start 2025-05-01 --end 2025-12-31
    python scripts/jobs/run_weather_daily.py --date 2026-08-20 --force   # re-fetch a day already stored

Every run is recorded as a `weather_import_jobs` row (visible in the admin
"Weather data → Import" page). Exit 0 = every day in the window processed or
already complete; 1 = any day failed or the job crashed. ECS surfaces the code
as `containers[0].exitCode`, which the task-failure alarm keys on.

Only the *scheduled* shape (no explicit dates) checks in to the GlitchTip cron
monitor — a multi-day backfill would otherwise trip its max-runtime.
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import structlog

sys.path.append(str(Path(__file__).parent.parent.parent))

from app.core.config import get_settings  # noqa: E402
from app.core.database import close_db, get_session_factory  # noqa: E402
from app.core.observability import capture_exception, cron_checkin, init_sentry  # noqa: E402

# MUST come before structlog.configure: structlog's filter_by_level defers to the
# stdlib logger level, and an unconfigured stdlib root is WARNING — every
# logger.info() would be dropped and the job would run invisibly (this bit the
# pipeline job first).
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

EXIT_OK = 0
EXIT_FAILED = 1

# ERA5T (the preliminary release CDS serves for recent days) trails real time by
# ~5-6 days; anything younger than that is simply not on the server yet.
DEFAULT_LAG_DAYS = 6

# GlitchTip cron monitor slug — alerts when the scheduled run fails OR never happens.
CRON_MONITOR_SLUG = "weather-daily"


def _cron_monitor_config() -> dict:
    """Schedule GlitchTip expects the job on. Must match infra/weather_daily.tf."""
    hour = int(os.getenv("WEATHER_DAILY_HOUR", "1"))
    minute = int(os.getenv("WEATHER_DAILY_MINUTE", "30"))
    return {
        "schedule": {"type": "crontab", "value": f"{minute} {hour} * * *"},
        "timezone": "UTC",
        "checkin_margin": 30,
        "max_runtime": 90,
    }


def parse_windfarm_ids(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    ids = sorted({int(part) for part in raw.split(",") if part.strip()})
    return ids or None


def resolve_window(
    today: date,
    *,
    single: date | None = None,
    start: date | None = None,
    end: date | None = None,
    lag_days: int = DEFAULT_LAG_DAYS,
) -> tuple[date, date]:
    """Pick the import window: an explicit day, an explicit range, or today - lag."""
    if single is not None:
        if start is not None or end is not None:
            raise ValueError("--date cannot be combined with --start/--end")
        return single, single
    if start is not None:
        end = end or start
        if end < start:
            raise ValueError(f"--end {end} is before --start {start}")
        return start, end
    if end is not None:
        raise ValueError("--end requires --start")
    target = today - timedelta(days=lag_days)
    return target, target


def is_scheduled_shape(args: argparse.Namespace) -> bool:
    """Only the no-explicit-dates invocation is the scheduled job GlitchTip tracks."""
    return args.date is None and args.start is None and args.end is None


async def run(args: argparse.Namespace) -> int:
    from app.models.weather_import_job import WeatherImportStatus
    from app.services.weather_import_service import WeatherImportService

    init_sentry(get_settings())

    start, end = resolve_window(
        datetime.now(timezone.utc).date(),
        single=args.date,
        start=args.start,
        end=args.end,
        lag_days=args.lag_days,
    )
    windfarm_ids = parse_windfarm_ids(args.windfarm_ids)
    scheduled = is_scheduled_shape(args)
    job_started = datetime.now(timezone.utc)
    logger.info(
        "weather_daily_job_started",
        start=str(start),
        end=str(end),
        days=(end - start).days + 1,
        windfarm_ids=windfarm_ids,
        force=args.force,
        scheduled=scheduled,
    )

    check_in_id = (
        cron_checkin(CRON_MONITOR_SLUG, status="in_progress", monitor_config=_cron_monitor_config())
        if scheduled
        else None
    )

    exit_code = EXIT_FAILED
    job_id = None
    try:
        session_factory = get_session_factory()
        async with session_factory() as db:
            svc = WeatherImportService(db)
            job = await svc.create_job(
                start_date=start,
                end_date=end,
                force_refresh=args.force,
                windfarm_ids=windfarm_ids,
            )
            job_id = job.id
            # execute_job opens its own sessions; it marks the row SUCCESS only
            # when every day was processed or already complete.
            job = await svc.execute_job(job_id)

        exit_code = EXIT_OK if job.status == WeatherImportStatus.SUCCESS else EXIT_FAILED
        logger.info(
            "weather_daily_job_result",
            job_id=job_id,
            status=str(job.status),
            records_imported=job.records_imported,
            files_downloaded=job.files_downloaded,
            api_calls=job.api_calls_made,
            error=job.error_message,
        )
    except Exception as exc:
        logger.error("weather_daily_job_failed", job_id=job_id, error=str(exc), exc_info=True)
        capture_exception(exc)
        exit_code = EXIT_FAILED
    finally:
        if scheduled:
            cron_checkin(
                CRON_MONITOR_SLUG,
                status="ok" if exit_code == EXIT_OK else "error",
                check_in_id=check_in_id,
            )
        try:
            await close_db()
        except Exception as exc:
            logger.warning("weather_daily_close_db_failed", error=str(exc))
        try:
            import sentry_sdk

            sentry_sdk.flush(timeout=10)
        except Exception as exc:
            logger.warning("weather_daily_sentry_flush_failed", error=str(exc))

    logger.info(
        "weather_daily_job_complete",
        job_id=job_id,
        exit_code=exit_code,
        duration_s=(datetime.now(timezone.utc) - job_started).total_seconds(),
    )
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", type=date.fromisoformat, help="One day (YYYY-MM-DD).")
    parser.add_argument("--start", type=date.fromisoformat, help="Range start (YYYY-MM-DD).")
    parser.add_argument(
        "--end", type=date.fromisoformat, help="Range end, inclusive (defaults to --start)."
    )
    parser.add_argument(
        "--windfarm-ids",
        help="Comma-separated windfarm ids. Scopes the farms, the CDS bounding box and the "
        "completeness check — the way to fill a newly added farm's history.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch days that already have complete data (e.g. ERA5T → final ERA5).",
    )
    parser.add_argument(
        "--lag-days",
        type=int,
        default=int(os.getenv("WEATHER_LAG_DAYS", str(DEFAULT_LAG_DAYS))),
        help="Scheduled shape only: import today minus this many days (ERA5T lag).",
    )
    args = parser.parse_args()
    try:
        return asyncio.run(run(args))
    except ValueError as exc:
        parser.error(str(exc))
        return EXIT_FAILED  # pragma: no cover - parser.error exits


if __name__ == "__main__":
    sys.exit(main())
