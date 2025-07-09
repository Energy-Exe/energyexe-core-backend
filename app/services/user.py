"""User service for business logic."""

from typing import List, Optional

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundException, ValidationException
from app.core.security import get_password_hash, verify_password
from app.models.user import User
from app.schemas.user import UserCreate, UserUpdate

logger = structlog.get_logger()


class UserService:
    """Service for user-related operations."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_by_id(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        result = await self.db.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()
    
    async def get_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        result = await self.db.execute(select(User).where(User.email == email))
        return result.scalar_one_or_none()
    
    async def get_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        result = await self.db.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()
    
    async def get_all(self, skip: int = 0, limit: int = 100) -> List[User]:
        """Get all users with pagination."""
        result = await self.db.execute(
            select(User).offset(skip).limit(limit).order_by(User.id)
        )
        return list(result.scalars().all())
    
    async def create(self, user_data: UserCreate) -> User:
        """Create a new user."""
        # Check if user already exists
        existing_user = await self.get_by_email(user_data.email)
        if existing_user:
            raise ValidationException("User with this email already exists")
        
        existing_user = await self.get_by_username(user_data.username)
        if existing_user:
            raise ValidationException("User with this username already exists")
        
        # Create new user
        hashed_password = get_password_hash(user_data.password)
        user = User(
            email=user_data.email,
            username=user_data.username,
            hashed_password=hashed_password,
            first_name=user_data.first_name,
            last_name=user_data.last_name,
            is_active=user_data.is_active,
        )
        
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)
        
        logger.info("User created", user_id=user.id, username=user.username)
        return user
    
    async def update(self, user_id: int, user_data: UserUpdate) -> User:
        """Update a user."""
        user = await self.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")
        
        # Check for email conflicts
        if user_data.email and user_data.email != user.email:
            existing_user = await self.get_by_email(user_data.email)
            if existing_user:
                raise ValidationException("User with this email already exists")
        
        # Check for username conflicts
        if user_data.username and user_data.username != user.username:
            existing_user = await self.get_by_username(user_data.username)
            if existing_user:
                raise ValidationException("User with this username already exists")
        
        # Update user fields
        update_data = user_data.model_dump(exclude_unset=True)
        
        if "password" in update_data:
            update_data["hashed_password"] = get_password_hash(update_data.pop("password"))
        
        for field, value in update_data.items():
            setattr(user, field, value)
        
        await self.db.commit()
        await self.db.refresh(user)
        
        logger.info("User updated", user_id=user.id, username=user.username)
        return user
    
    async def delete(self, user_id: int) -> bool:
        """Delete a user."""
        user = await self.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")
        
        await self.db.delete(user)
        await self.db.commit()
        
        logger.info("User deleted", user_id=user_id)
        return True
    
    async def authenticate(self, username: str, password: str) -> Optional[User]:
        """Authenticate a user."""
        user = await self.get_by_username(username)
        
        if not user:
            # Try email as well
            user = await self.get_by_email(username)
        
        if not user or not verify_password(password, user.hashed_password):
            return None
        
        if not user.is_active:
            return None
        
        return user 