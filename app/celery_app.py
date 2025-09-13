"""Celery application instance."""

from celery import Celery
from celery.signals import setup_logging

from app.core.config import get_settings

# Get settings
settings = get_settings()

# Create Celery app
celery_app = Celery("energyexe")

# Load configuration from celery_config module
celery_app.config_from_object("app.core.celery_config")

# Auto-discover tasks in the app.tasks module
celery_app.autodiscover_tasks(["app.tasks"])


@setup_logging.connect
def config_loggers(*args, **kwargs):
    """Configure logging for Celery."""
    import structlog
    
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
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )