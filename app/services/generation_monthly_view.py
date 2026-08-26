"""Refresh of ``mv_generation_monthly_by_windfarm`` (monthly net generation).

The view is defined by migration ``c4d8e1f2a3b5`` (created ``WITH NO DATA``);
this module only refreshes it. It is the denominator source for
``app/services/financial_opex_metrics`` — 12 rows per fiscal filing instead of
~9,000 random heap reads of hourly ``generation_data``.

Refresh runs nightly from the pipeline task (``app/cron/pipeline_daily.py``)
before opportunity detection, and on demand via
``scripts/jobs/refresh_generation_monthly.py``. It scans the whole
``generation_data`` table (tens of millions of rows), so it uses its own
short-lived engine WITHOUT the app's per-query ``command_timeout`` — the main
pool's 180s bound would abort it on a cold cache.

Mode: ``REFRESH ... CONCURRENTLY`` once the view is populated (readers keep the
previous snapshot; needs the unique index the migration creates); a plain
``REFRESH`` for the very first population (CONCURRENTLY is not allowed on an
unpopulated view).
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

logger = structlog.get_logger()

MV_NAME = "mv_generation_monthly_by_windfarm"


def _refresh_engine() -> AsyncEngine:
    from app.core.config import get_settings
    from app.core.database import _pg_connect_args

    settings = get_settings()
    connect_args = _pg_connect_args(settings, "energyexe-mv-refresh")
    connect_args["command_timeout"] = None  # full-table aggregate; no per-query bound
    return create_async_engine(
        settings.database_url_async, poolclass=NullPool, connect_args=connect_args
    )


async def refresh_generation_monthly_view(
    *, concurrently: Optional[bool] = None, engine: Optional[AsyncEngine] = None
) -> Dict[str, Any]:
    """Refresh the view and return a small summary for logging.

    ``concurrently=None`` picks the mode automatically (CONCURRENTLY when the
    view is already populated). Raises when the view does not exist — run
    ``alembic upgrade head`` first.
    """
    own_engine = engine is None
    eng = engine or _refresh_engine()
    started = time.monotonic()
    try:
        async with eng.begin() as conn:
            populated = (
                await conn.execute(
                    text("SELECT ispopulated FROM pg_matviews WHERE matviewname = :name"),
                    {"name": MV_NAME},
                )
            ).scalar_one_or_none()
            if populated is None:
                raise RuntimeError(
                    f"materialized view {MV_NAME} does not exist — run `alembic upgrade head`"
                )
            mode = bool(populated) if concurrently is None else bool(concurrently)
            await conn.execute(
                text(f"REFRESH MATERIALIZED VIEW {'CONCURRENTLY ' if mode else ''}{MV_NAME}")
            )
            stats = (
                await conn.execute(
                    text(
                        f"SELECT COUNT(*) AS rows, COUNT(DISTINCT windfarm_id) AS windfarms, "
                        f"MAX(month) AS latest_month FROM {MV_NAME}"
                    )
                )
            ).one()
    finally:
        if own_engine:
            await eng.dispose()

    summary = {
        "view": MV_NAME,
        "concurrently": mode,
        "rows": int(stats.rows),
        "windfarms": int(stats.windfarms),
        "latest_month": str(stats.latest_month),
        "duration_s": round(time.monotonic() - started, 1),
    }
    logger.info("generation_monthly_view_refreshed", **summary)
    return summary


__all__ = ["MV_NAME", "refresh_generation_monthly_view"]
