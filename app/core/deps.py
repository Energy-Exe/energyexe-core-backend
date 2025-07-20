"""Dependency injection utilities."""

from typing import Generator

import structlog
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.core.exceptions import AuthenticationException
from app.core.security import verify_token
from app.models.user import User
from app.services.user import UserService

logger = structlog.get_logger()
security = HTTPBearer()


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
    """Get current authenticated user."""
    token = credentials.credentials
    username = verify_token(token)
    
    if username is None:
        raise AuthenticationException("Could not validate credentials")
    
    user_service = UserService(db)
    user = await user_service.get_by_username(username)
    
    if user is None:
        raise AuthenticationException("User not found")
    
    if not user.is_active:
        raise AuthenticationException("Inactive user")
    
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
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    return current_user 