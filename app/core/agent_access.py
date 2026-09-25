"""Agent authorization shared by HTTP entrypoints and direct service calls."""

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.user import User


def require_agent_access(user) -> None:
    if (
        user is None
        or not user.is_active
        or (user.role == "client" and (not user.email_verified or not user.is_approved))
        or (get_settings().BRAIN_AGENT_ACCESS_POLICY == "superusers" and not user.is_superuser)
    ):
        raise HTTPException(403, "Agent access denied")


async def require_fresh_agent_access(db: AsyncSession, user_id: int) -> None:
    # Select columns so neither an ORM identity-map entry nor a cached agent
    # session can retain permissions after a user is deactivated or demoted.
    result = await db.execute(
        select(
            User.is_active,
            User.role,
            User.email_verified,
            User.is_approved,
            User.is_superuser,
        ).where(User.id == user_id)
    )
    require_agent_access(result.one_or_none())
