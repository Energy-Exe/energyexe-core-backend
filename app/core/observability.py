"""Error-tracking / observability setup.

We self-host **GlitchTip**, which is Sentry-API-compatible, so the standard
``sentry_sdk`` is used unchanged — only the DSN points at our own instance
(``errors.energyexe.com``) instead of sentry.io.

``init_sentry`` is a no-op when ``SENTRY_DSN`` is unset, so local development
and the test suite run clean without a tracker. It also never raises: a bad DSN
or a missing package must not take down application startup.
"""

from __future__ import annotations

import structlog

logger = structlog.get_logger()


def init_sentry(settings) -> None:
    """Initialize the Sentry SDK pointed at GlitchTip, if a DSN is configured."""
    dsn = getattr(settings, "SENTRY_DSN", None)
    if not dsn:
        return

    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration

        sentry_sdk.init(
            dsn=dsn,
            environment=settings.SENTRY_ENVIRONMENT,
            traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
            # Don't ship request bodies / user data to the tracker by default.
            send_default_pii=False,
            integrations=[
                StarletteIntegration(),
                FastApiIntegration(),
            ],
        )
        logger.info("sentry_initialized", environment=settings.SENTRY_ENVIRONMENT)
    except Exception as exc:  # pragma: no cover - defensive: never break startup
        logger.warning("sentry_init_failed", error=str(exc))
