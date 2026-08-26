"""Tests for the monthly generation view refresh (mocked engine, no DB)."""

from contextlib import asynccontextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.generation_monthly_view import MV_NAME, refresh_generation_monthly_view


def _engine(populated, rows=(123, 45, date(2026, 7, 1))):
    """An engine whose begin() yields a conn answering the three statements in order."""
    conn = MagicMock()
    statements = []

    async def execute(stmt, params=None):
        statements.append(str(stmt))
        res = MagicMock()
        if "pg_matviews" in str(stmt):
            res.scalar_one_or_none.return_value = populated
        elif "COUNT(*)" in str(stmt):
            res.one.return_value = SimpleNamespace(
                rows=rows[0], windfarms=rows[1], latest_month=rows[2]
            )
        return res

    conn.execute = AsyncMock(side_effect=execute)

    @asynccontextmanager
    async def begin():
        yield conn

    engine = MagicMock()
    engine.begin = begin
    engine.dispose = AsyncMock()
    return engine, statements


@pytest.mark.asyncio
async def test_populated_view_refreshes_concurrently():
    engine, statements = _engine(populated=True)
    summary = await refresh_generation_monthly_view(engine=engine)
    assert any(
        s.startswith(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {MV_NAME}") for s in statements
    )
    assert summary["concurrently"] is True
    assert summary["rows"] == 123 and summary["windfarms"] == 45
    assert summary["latest_month"] == "2026-07-01"
    engine.dispose.assert_not_awaited()  # caller-owned engine is left alone


@pytest.mark.asyncio
async def test_unpopulated_view_uses_plain_refresh():
    engine, statements = _engine(populated=False)
    summary = await refresh_generation_monthly_view(engine=engine)
    assert any(s.startswith(f"REFRESH MATERIALIZED VIEW {MV_NAME}") for s in statements)
    assert summary["concurrently"] is False


@pytest.mark.asyncio
async def test_explicit_mode_overrides_auto():
    engine, statements = _engine(populated=True)
    await refresh_generation_monthly_view(engine=engine, concurrently=False)
    assert any(s.startswith(f"REFRESH MATERIALIZED VIEW {MV_NAME}") for s in statements)
    assert not any("CONCURRENTLY" in s for s in statements)


@pytest.mark.asyncio
async def test_missing_view_raises_with_migration_hint():
    engine, _ = _engine(populated=None)
    with pytest.raises(RuntimeError, match="alembic upgrade head"):
        await refresh_generation_monthly_view(engine=engine)
