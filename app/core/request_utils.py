"""Small helpers for reading request metadata behind the ALB.

The API only ever sees the load balancer's private address in ``request.client``;
the real client is the first hop of ``X-Forwarded-For`` (the ALB appends itself,
and only the ALB can reach the task, so the header is trustworthy).
"""

from typing import Optional

from fastapi import Request


def get_client_ip(request: Optional[Request]) -> Optional[str]:
    """Return the originating client IP for a request (proxy-aware)."""
    if request is None:
        return None

    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip

    return request.client.host if request.client else None


def get_user_agent(request: Optional[Request]) -> Optional[str]:
    """Return the User-Agent header, if any."""
    if request is None:
        return None
    return request.headers.get("User-Agent")
