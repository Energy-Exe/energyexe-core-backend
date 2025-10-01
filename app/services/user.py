"""User service for business logic."""

from typing import List, Optional

import structlog
from sqlalchemy import or_, select
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

    async def get_all(
        self, skip: int = 0, limit: int = 100, search: Optional[str] = None
    ) -> List[User]:
        """Get all users with pagination and optional search."""
        query = select(User)

        # Add search functionality if search term is provided
        if search:
            search_term = f"%{search}%"
            query = query.where(
                or_(
                    User.username.ilike(search_term),
                    User.email.ilike(search_term),
                    User.first_name.ilike(search_term),
                    User.last_name.ilike(search_term),
                )
            )

        query = query.offset(skip).limit(limit).order_by(User.id)
        result = await self.db.execute(query)
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
        """Authenticate a user by username or email and password."""
        # Try to get user by username first
        user = await self.get_by_username(username)

        # If not found by username, try email
        if not user:
            user = await self.get_by_email(username)

        if not user:
            return None

        if not verify_password(password, user.hashed_password):
            return None

        # Rehash password if it's longer than 72 bytes (bcrypt limit)
        # This automatically migrates old passwords hashed with passlib
        if len(password.encode('utf-8')) > 72:
            new_hash = get_password_hash(password)
            user.hashed_password = new_hash
            await self.db.commit()
            await self.db.refresh(user)
            logger.info("Password rehashed for user", user_id=user.id, username=user.username)

        return user
