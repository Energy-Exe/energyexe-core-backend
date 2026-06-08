"""Dependency injection utilities."""

from typing import Optional

import structlog
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.core.exceptions import AuthenticationException
from app.core.security import verify_token
from app.models.user import User
from app.services.user import UserService

logger = structlog.get_logger()
security = HTTPBearer()
optional_security = HTTPBearer(auto_error=False)


async def get_db() -> AsyncSession:
    """Get database session."""
    async with get_session_factory()() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_current_user(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> User:
    """Get current authenticated user.

    This validates the JWT token and returns the user if:
    1. Token is valid
    2. User exists
    3. User is active
    4. User's email is verified
    5. User is approved (for client users)
    """
    token = credentials.credentials
    username = verify_token(token)

    if username is None:
        raise AuthenticationException("Could not validate credentials")

    user_service = UserService(db)
    user = await user_service.get_by_username(username)

    if user is None:
        raise AuthenticationException("User not found")

    if not user.is_active:
        raise AuthenticationException("Account has been deactivated")

    # Check email verification for client users
    if user.role == "client" and not user.email_verified:
        raise AuthenticationException("Email not verified")

    # Check approval for client users
    if user.role == "client" and not user.is_approved:
        raise AuthenticationException("Account pending approval")

    return user


async def get_current_user_optional(
    db: AsyncSession = Depends(get_db),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(optional_security),
) -> Optional[User]:
    """Get the current user if a valid token is supplied, otherwise None.

    Never raises on missing/invalid credentials — used by endpoints that are
    readable without auth but apply role-based filtering (e.g. windfarm
    client-visibility).
    """
    if credentials is None:
        return None

    username = verify_token(credentials.credentials)
    if username is None:
        return None

    user_service = UserService(db)
    user = await user_service.get_by_username(username)

    if user is None or not user.is_active:
        return None

    return user


def is_client_request(user: Optional[User]) -> bool:
    """Whether the requester should be treated as a client for visibility filtering.

    Unauthenticated requests are treated as clients (safe default); admin-ui
    always sends a token, so internal users are unaffected.
    """
    if user is None:
        return True
    return not user.is_superuser and user.role != "admin"


def exclude_deleted(user: Optional[User], include_deleted: bool) -> bool:
    """Soft-deleted windfarms are excluded for EVERYONE by default — even admins
    (so an admin browsing the client portal sees what clients see). Only an
    explicit include_deleted=true from an admin requester returns them
    (used by the admin panel).
    """
    return not (include_deleted and not is_client_request(user))


async def get_current_user_basic(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> User:
    """Get current user with basic validation only.

    This is for endpoints that need to work even if user is not yet approved.
    Only validates:
    1. Token is valid
    2. User exists
    3. User is active
    """
    token = credentials.credentials
    username = verify_token(token)

    if username is None:
        raise AuthenticationException("Could not validate credentials")

    user_service = UserService(db)
    user = await user_service.get_by_username(username)

    if user is None:
        raise AuthenticationException("User not found")

    if not user.is_active:
        raise AuthenticationException("Account has been deactivated")

    return user


async def get_current_active_user(
    current_user: User = Depends(get_current_user),
) -> User:
    """Get current active user."""
    if not current_user.is_active:
        raise AuthenticationException("Inactive user")
    return current_user


async def get_current_superuser(
    current_user: User = Depends(get_current_user),
) -> User:
    """Get current superuser."""
    if not current_user.is_superuser:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not enough permissions")
    return current_user


async def get_current_admin_user(
    current_user: User = Depends(get_current_user),
) -> User:
    """Get current admin user (superuser or admin role)."""
    if not current_user.is_superuser and current_user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return current_user
