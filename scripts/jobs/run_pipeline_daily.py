#!/usr/bin/env python3
"""Nightly performance pipeline + opportunity detection, as a standalone job.

This is the entrypoint for the EventBridge-scheduled ECS task defined in
`infra/pipeline_daily.tf`. It replaces the in-process APScheduler job that used
to run inside the API container.

    python scripts/jobs/run_pipeline_daily.py
    python scripts/jobs/run_pipeline_daily.py --windfarm-ids 7404,7200   # smoke test
    python scripts/jobs/run_pipeline_daily.py --skip-detection

Exit codes are the point of this script: 0 = both phases passed, 1 = the batch
failed (detection skipped), 2 = the batch passed but detection failed. ECS
surfaces the code as `containers[0].exitCode`, which is what the task-failure
alarm keys on.
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import structlog

sys.path.append(str(Path(__file__).parent.parent.parent))

from app.core.config import get_settings  # noqa: E402
from app.core.database import close_db  # noqa: E402
from app.core.observability import init_sentry  # noqa: E402
from app.cron.pipeline_daily import run_pipeline_job  # noqa: E402

# MUST come before structlog.configure: the `filter_by_level` processor below
# defers to the *stdlib* logger's level, and an unconfigured stdlib root defaults
# to WARNING. The API never hits this because uvicorn configures logging itself;
# a standalone script has nothing doing that, so without this line every
# logger.info() is silently dropped and the job runs near-invisibly — no
# job_started, no batch_complete, no job_complete. Confirmed on a staging run
# that emitted 10 warnings and zero info lines.
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)

# Same JSON renderer the API uses (scripts/start.py). Without this, structlog
# falls back to its dev-console renderer and CloudWatch gets coloured
# human-formatted lines instead of parseable JSON.
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


def _parse_windfarm_ids(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(part) for part in raw.split(",") if part.strip()]


async def run(args: argparse.Namespace) -> int:
    # Must be explicit: nothing else in a standalone process initialises Sentry,
    # and without it cron_checkin() and capture_exception() are silent no-ops —
    # we would lose every failure signal and GlitchTip would never see the run.
    init_sentry(get_settings())

    try:
        return await run_pipeline_job(
            windfarm_ids=_parse_windfarm_ids(args.windfarm_ids),
            period_months=args.period_months,
            skip_detection=args.skip_detection,
        )
    except Exception as exc:  # defensive: run_pipeline_job handles its own phases
        logger.error("pipeline_daily_unhandled_error", error=str(exc), exc_info=True)
        from app.core.observability import capture_exception

        capture_exception(exc)
        return 1
    finally:
        # Leave no dangling asyncpg connections behind when the container exits.
        try:
            await close_db()
        except Exception as exc:
            logger.warning("pipeline_daily_close_db_failed", error=str(exc))

        # sentry_sdk queues envelopes on a background transport. A container that
        # exits immediately drops the *closing* cron check-in, which leaves the
        # GlitchTip monitor stuck "in_progress" until max_runtime fires — i.e. an
        # alert on every successful run. Flush before we go.
        try:
            import sentry_sdk

            sentry_sdk.flush(timeout=10)
        except Exception as exc:
            logger.warning("pipeline_daily_sentry_flush_failed", error=str(exc))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--windfarm-ids",
        help="Comma-separated windfarm ids to scope both phases to. Omit for the "
        "full operational fleet. A short list turns a ~3h job into a minutes-long "
        "smoke test.",
    )
    parser.add_argument(
        "--period-months",
        type=int,
        default=24,
        help="Rolling window for opportunity detection (default: 24).",
    )
    parser.add_argument(
        "--skip-detection",
        action="store_true",
        help="Run the performance batch only.",
    )
    args = parser.parse_args()

    try:
        _parse_windfarm_ids(args.windfarm_ids)
    except ValueError:
        parser.error("--windfarm-ids must be a comma-separated list of integers")

    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
