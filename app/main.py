"""Main FastAPI application."""

import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from app.api.v1.router import api_router
from app.core.config import get_settings
from app.core.database import init_db
from app.core.exceptions import add_exception_handlers
from app.core.middleware import add_middleware

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting up application")

    # Skip database initialization during testing
    if not os.getenv("TESTING", "false").lower() == "true":
        # Initialize database
        await init_db()

    # Clone frontend repos for Brain Agent code access (non-blocking)
    try:
        from app.services.brain_agent_repo_manager import ensure_repos
        ensure_repos()
    except Exception as e:
        logger.warning("brain_agent_repo_setup_failed", error=str(e))

    # Start daily pipeline scheduler (PRE-D for spec items 1-6).
    # Opt-in via PIPELINE_DAILY_ENABLED=true so dev machines don't run it.
    try:
        from app.cron.pipeline_daily import start_pipeline_scheduler
        start_pipeline_scheduler()
    except Exception as e:
        logger.warning("pipeline_scheduler_start_failed", error=str(e))

    yield

    # Shut down scheduler so APScheduler doesn't hold the event loop open.
    try:
        from app.cron.pipeline_daily import stop_pipeline_scheduler
        stop_pipeline_scheduler()
    except Exception as e:
        logger.warning("pipeline_scheduler_stop_failed", error=str(e))

    logger.info("Shutting down application")


def create_application() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    # Skip lifespan during testing
    lifespan_context = None if os.getenv("TESTING", "false").lower() == "true" else lifespan

    app = FastAPI(
        title=settings.PROJECT_NAME,
        description="EnergyExe Core Backend API",
        version="0.1.0",
        openapi_url=f"{settings.API_V1_STR}/openapi.json" if settings.DEBUG else None,
        docs_url="/docs" if settings.DEBUG else None,
        redoc_url="/redoc" if settings.DEBUG else None,
        lifespan=lifespan_context,
        redirect_slashes=False,
    )

    # Add CORS middleware first (must be before other middleware)
    if settings.BACKEND_CORS_ORIGINS:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Add other middleware
    add_middleware(app)

    # Add trusted host middleware
    if settings.ALLOWED_HOSTS:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=settings.ALLOWED_HOSTS,
        )

    # Add exception handlers
    add_exception_handlers(app)

    # Include API router
    app.include_router(api_router, prefix=settings.API_V1_STR)

    # Add root endpoints
    @app.get("/")
    async def root():
        """Root endpoint."""
        return {"message": "EnergyExe Core Backend API", "version": "0.1.0", "status": "healthy"}

    @app.get("/health")
    async def health_check():
        """Health check endpoint with database connectivity test."""
        from sqlalchemy import text

        from app.core.database import get_session_factory

        try:
            # Test database connection
            AsyncSessionLocal = get_session_factory()
            async with AsyncSessionLocal() as db:
                await db.execute(text("SELECT 1"))

            return {
                "status": "healthy",
                "database": "connected",
            }
        except Exception as e:
            logger.error("Health check failed", error=str(e))
            return {
                "status": "unhealthy",
                "database": "error",
                "error": str(e),
            }

    return app


# Create the app instance
app = create_application()
