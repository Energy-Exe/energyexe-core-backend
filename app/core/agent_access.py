"""Agent authorization shared by HTTP entrypoints and direct service calls."""

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.user import User


def is_internal_staff(user) -> bool:
    """EPR-143: the admin agent profile is an internal-only surface (its shared read-only DB
    role has SELECT on every public table, the SCADA PPA register included)."""
    return bool(
        user is not None
        and getattr(user, "is_superuser", False)
        and getattr(user, "is_internal", False)
    )


def require_agent_access(user, *, source: str | None = None) -> None:
    if (
        user is None
        or not user.is_active
        or (user.role == "client" and (not user.email_verified or not user.is_approved))
        or (get_settings().BRAIN_AGENT_ACCESS_POLICY == "superusers" and not user.is_superuser)
        or (source == "admin" and not is_internal_staff(user))
    ):
        raise HTTPException(403, "Agent access denied")


async def require_fresh_agent_access(
    db: AsyncSession, user_id: int, *, source: str | None = None
) -> None:
    # Select columns so neither an ORM identity-map entry nor a cached agent
    # session can retain permissions after a user is deactivated or demoted.
    result = await db.execute(
        select(
            User.is_active,
            User.role,
            User.email_verified,
            User.is_approved,
            User.is_superuser,
            User.is_internal,
        ).where(User.id == user_id)
    )
    require_agent_access(result.one_or_none(), source=source)
