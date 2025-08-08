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
from app.cron.entsoe_scheduler import start_scheduler as start_entsoe_scheduler, stop_scheduler as stop_entsoe_scheduler

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting up application")

    # Skip database initialization during testing
    if not os.getenv("TESTING", "false").lower() == "true":
        # Initialize database
        await init_db()

        # Start ENTSOE scheduler
        start_entsoe_scheduler()
        logger.info("ENTSOE scheduler started")

    yield

    # Stop schedulers on shutdown
    if not os.getenv("TESTING", "false").lower() == "true":
        stop_entsoe_scheduler()
        logger.info("ENTSOE scheduler stopped")

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
        """Health check endpoint."""
        return {"status": "healthy"}

    return app


# Create the app instance
app = create_application()
