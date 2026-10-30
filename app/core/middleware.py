"""Application middleware."""

import gzip
import time
import uuid
from typing import Callable, Optional

import sentry_sdk
import structlog
from fastapi import FastAPI, Request, Response
from starlette.datastructures import Headers, MutableHeaders
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.request_utils import get_client_ip

logger = structlog.get_logger()


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for request/response logging."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and log details."""
        request_id = str(uuid.uuid4())
        start_time = time.time()

        # Add request ID to request state
        request.state.request_id = request_id

        # Tag the per-request Sentry scope so GlitchTip issues carry the same
        # request_id we log — lets you pivot from an error to its log line.
        sentry_sdk.set_tag("request_id", request_id)

        # Log request
        logger.info(
            "Request started",
            request_id=request_id,
            method=request.method,
            url=str(request.url),
            client_ip=get_client_ip(request),
        )

        try:
            response = await call_next(request)

            # Calculate processing time
            process_time = time.time() - start_time

            # Log response
            logger.info(
                "Request completed",
                request_id=request_id,
                status_code=response.status_code,
                process_time=round(process_time, 4),
            )

            # Add headers
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time"] = str(round(process_time, 4))

            return response

        except Exception as e:
            process_time = time.time() - start_time
            logger.error(
                "Request failed",
                request_id=request_id,
                error=str(e),
                process_time=round(process_time, 4),
                exc_info=True,
            )
            raise


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware for adding security headers."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Add security headers to response."""
        response = await call_next(request)

        # Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # Add HSTS header for HTTPS connections to prevent mixed content issues
        if request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https":
            response.headers[
                "Strict-Transport-Security"
            ] = "max-age=31536000; includeSubDomains; preload"

        # Allow frames for docs endpoints, otherwise deny
        if request.url.path in ["/docs", "/redoc"]:
            response.headers["X-Frame-Options"] = "SAMEORIGIN"
        else:
            response.headers["X-Frame-Options"] = "DENY"

        return response


class GZipWholeResponseMiddleware:
    """Gzip complete responses; pass streamed ones through untouched.

    The wind farm list is ~1.7 MB of JSON and ~130 KB gzipped, and the ALB does
    not compress. Starlette 0.37's GZipMiddleware also compresses streaming
    responses, where zlib holds small chunks back and would stall the brain
    agent's and the map's server-sent events, so this only touches responses
    sent as a single body message (every JSONResponse).

    Must sit inside the BaseHTTPMiddleware layers: those re-send every response
    as a stream, which this middleware would then leave alone.
    """

    def __init__(self, app: ASGIApp, minimum_size: int = 1024, compresslevel: int = 6) -> None:
        self.app = app
        self.minimum_size = minimum_size
        self.compresslevel = compresslevel

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or "gzip" not in Headers(scope=scope).get("accept-encoding", ""):
            await self.app(scope, receive, send)
            return

        start: Optional[Message] = None
        passthrough = False

        async def send_maybe_gzipped(message: Message) -> None:
            nonlocal start, passthrough
            if message["type"] == "http.response.start":
                # Hold the headers until the first body message shows whether it streams.
                start = message
                return
            if passthrough or start is None or message["type"] != "http.response.body":
                await send(message)
                return

            initial, start = start, None
            headers = MutableHeaders(raw=initial["headers"])
            body = message.get("body", b"")
            if (
                message.get("more_body", False)
                or "content-encoding" in headers
                or len(body) < self.minimum_size
            ):
                passthrough = True
                await send(initial)
                await send(message)
                return

            body = gzip.compress(body, compresslevel=self.compresslevel)
            headers["Content-Encoding"] = "gzip"
            headers["Content-Length"] = str(len(body))
            headers.add_vary_header("Accept-Encoding")
            await send(initial)
            await send({**message, "body": body})

        await self.app(scope, receive, send_maybe_gzipped)


def add_middleware(app: FastAPI) -> None:
    """Add all middleware to the FastAPI app."""
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(LoggingMiddleware)
