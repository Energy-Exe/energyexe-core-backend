"""Database configuration and session management."""

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool, StaticPool

from app.core.config import get_settings

logger = structlog.get_logger()

# Lazy initialization
_engine = None
_async_session_factory = None


def _pg_connect_args(settings, application_name: str) -> dict:
    """asyncpg connect_args shared by the main pool and any isolated engine.

    TCP keepalive: asyncpg is NOT libpq, so PostgreSQL's per-session keepalive
    GUCs are set via server_settings (not libpq keepalives* kwargs). This keeps
    long-running connections alive across idle gaps and lets the server detect
    dead peers — the 2026-05-30 6-way parallel run hung on silently-dropped RDS
    connections without it. command_timeout bounds any single in-query hang;
    ssl is required by AWS RDS.
    """
    return {
        "server_settings": {
            "application_name": application_name,
            "tcp_keepalives_idle": "30",
            "tcp_keepalives_interval": "10",
            "tcp_keepalives_count": "5",
        },
        "command_timeout": settings.DB_COMMAND_TIMEOUT,
        "ssl": "require",
    }


def get_engine():
    """Get or create the async engine."""
    global _engine
    if _engine is None:
        settings = get_settings()
        engine_kwargs = {
            "echo": settings.DB_ECHO,
            "future": True,
        }

        if "sqlite" in settings.database_url_async:
            # For SQLite, use StaticPool without pool size parameters
            engine_kwargs["poolclass"] = StaticPool
            engine_kwargs["connect_args"] = {"check_same_thread": False}
        else:
            # For PostgreSQL, use pool settings
            engine_kwargs["pool_size"] = settings.DB_POOL_SIZE
            engine_kwargs["max_overflow"] = settings.DB_MAX_OVERFLOW

            # Connection health settings for cloud PostgreSQL (Railway)
            # pool_pre_ping tests connections before use to catch closed connections
            engine_kwargs["pool_pre_ping"] = settings.DB_POOL_PRE_PING

            # pool_recycle ensures connections are recycled before PostgreSQL closes them
            # This prevents stale connections when cloud PostgreSQL closes idle ones
            engine_kwargs["pool_recycle"] = settings.DB_POOL_RECYCLE

            # Connection timeout settings
            engine_kwargs["pool_timeout"] = settings.DB_POOL_TIMEOUT
            # connect_args (TCP keepalives + command_timeout + ssl) are shared
            # with create_isolated_engine via _pg_connect_args — see its docstring.
            engine_kwargs["connect_args"] = _pg_connect_args(settings, "energyexe-backend")

        _engine = create_async_engine(settings.database_url_async, **engine_kwargs)
    return _engine


def create_isolated_engine():
    """Create a standalone NullPool async engine that does NOT share the global
    request-serving pool. The caller MUST ``await engine.dispose()`` when done,
    ideally in a ``finally``.

    Use for DB work that may be cancelled mid-query — e.g. wrapped in
    ``asyncio.wait_for`` — where the cancellation can interrupt AsyncSession
    cleanup before the connection is checked back in. Running such work on its
    own throwaway engine means a half-cancelled checkout can never orphan a
    connection in the shared pool that serves the API (root cause of the
    2026-06-18 pool-exhaustion incident). NullPool keeps nothing pooled (one
    fresh connection per use), so dispose() is a clean teardown.
    """
    settings = get_settings()
    if "sqlite" in settings.database_url_async:
        return create_async_engine(
            settings.database_url_async,
            echo=settings.DB_ECHO,
            future=True,
            poolclass=NullPool,
            connect_args={"check_same_thread": False},
        )
    return create_async_engine(
        settings.database_url_async,
        echo=settings.DB_ECHO,
        future=True,
        poolclass=NullPool,
        connect_args=_pg_connect_args(settings, "energyexe-backend-peeragg"),
    )


def get_session_factory():
    """Get or create the async session factory."""
    global _async_session_factory
    if _async_session_factory is None:
        _async_session_factory = async_sessionmaker(
            get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _async_session_factory


class Base(DeclarativeBase):
    """Base class for all database models."""

    pass


async def get_db() -> AsyncSession:
    """Get database session."""
    async with get_session_factory()() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """Initialize database."""
    try:
        # Import all models here to ensure they are registered
        from app.models import user  # noqa: F401

        # Create tables
        async with get_engine().begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error("Failed to initialize database", error=str(e))
        raise


async def close_db() -> None:
    """Close database connections."""
    await get_engine().dispose()
    logger.info("Database connections closed")
