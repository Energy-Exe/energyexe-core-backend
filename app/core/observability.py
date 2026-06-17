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

        # "unknown" is the Dockerfile default when no GIT_SHA build-arg was
        # passed (e.g. a local build) — treat it as "no release" rather than
        # tagging every error with a literal "unknown".
        release = getattr(settings, "SENTRY_RELEASE", None)
        if release in (None, "", "unknown"):
            release = None

        sentry_sdk.init(
            dsn=dsn,
            environment=settings.SENTRY_ENVIRONMENT,
            release=release,
            traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
            # Don't ship request bodies / user data to the tracker by default.
            send_default_pii=False,
            integrations=[
                StarletteIntegration(),
                FastApiIntegration(),
            ],
        )
        logger.info(
            "sentry_initialized",
            environment=settings.SENTRY_ENVIRONMENT,
            release=release,
        )
    except Exception as exc:  # pragma: no cover - defensive: never break startup
        logger.warning("sentry_init_failed", error=str(exc))


def capture_exception(exc: BaseException) -> None:
    """Report an exception to the tracker. No-op if Sentry isn't initialized.

    Use this where a ``try/except`` *handles* an error (logs it, alerts, and
    keeps going) instead of re-raising: Sentry's auto-capture only fires on
    exceptions that propagate out of a request, so handled errors — like a
    background job swallowing its own failure — must be reported explicitly.
    Never raises; reporting must not turn a recoverable error into a crash.
    """
    try:
        import sentry_sdk

        sentry_sdk.capture_exception(exc)
    except Exception:  # pragma: no cover - defensive: reporting must never fail a job
        pass


def cron_checkin(
    monitor_slug: str,
    status: str,
    check_in_id: str | None = None,
    monitor_config: dict | None = None,
) -> str | None:
    """Send a GlitchTip/Sentry cron check-in. No-op (returns None) if Sentry
    isn't initialized.

    Call once at the start of a scheduled job with ``status="in_progress"`` and
    a ``monitor_config`` (so GlitchTip auto-creates the monitor and learns the
    expected schedule), then again with ``status="ok"`` or ``"error"`` plus the
    returned ``check_in_id`` when it finishes. GlitchTip then alerts on failed
    runs *and* on runs that never happened (the silent-failure case). Never
    raises.
    """
    try:
        # capture_checkin lives under sentry_sdk.crons (not a top-level export)
        # in sentry-sdk 2.x.
        from sentry_sdk.crons import capture_checkin

        return capture_checkin(
            monitor_slug=monitor_slug,
            check_in_id=check_in_id,
            status=status,
            monitor_config=monitor_config,
        )
    except Exception:  # pragma: no cover - defensive
        return None
