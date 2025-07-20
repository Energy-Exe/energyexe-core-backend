"""Database configuration and session management."""

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import StaticPool

from app.core.config import get_settings

logger = structlog.get_logger()

# Lazy initialization
_engine = None
_async_session_factory = None

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
        
        _engine = create_async_engine(
            settings.database_url_async,
            **engine_kwargs
        )
    return _engine

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