"""Small helpers for reading request metadata behind the ALB.

The API only ever sees the load balancer's private address in ``request.client``.
The ALB *appends* the connecting client's address to ``X-Forwarded-For`` (creating
the header when absent), and it is the only thing that can reach the task, so the
**last** entry is the trustworthy one — anything before it was supplied by the
client and can be spoofed (a curl with ``X-Forwarded-For: 1.2.3.4`` must not be
able to forge the audit trail).
"""

from typing import Optional

from fastapi import Request


def get_client_ip(request: Optional[Request]) -> Optional[str]:
    """Return the originating client IP for a request (proxy-aware, last hop)."""
    if request is None:
        return None

    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        hops = [hop.strip() for hop in forwarded.split(",") if hop.strip()]
        if hops:
            return hops[-1]

    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip

    return request.client.host if request.client else None


def get_user_agent(request: Optional[Request]) -> Optional[str]:
    """Return the User-Agent header, if any."""
    if request is None:
        return None
    return request.headers.get("User-Agent")
