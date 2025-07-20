"""Startup script for the FastAPI application.

Note: This script should be run with Poetry:
    poetry run python scripts/start.py

Or use the Poetry command directly:
    poetry run uvicorn app.main:app --reload
"""

import structlog
import uvicorn

from app.core.config import get_settings

# Configure structured logging
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

logger = structlog.get_logger()


def main():
    """Main function to start the application."""
    settings = get_settings()

    logger.info(
        "Starting EnergyExe Core Backend",
        host=settings.HOST,
        port=settings.PORT,
        debug=settings.DEBUG,
        reload=settings.RELOAD,
    )

    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
        log_level=settings.LOG_LEVEL.lower(),
    )


if __name__ == "__main__":
    main()
