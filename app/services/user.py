"""User service for business logic."""

import secrets
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import structlog
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import NotFoundException, ValidationException
from app.core.security import get_password_hash, verify_password
from app.models.user import User
from app.models.user_feature import DEFAULT_FEATURES, UserFeature
from app.schemas.user import ClientRegister, UserCreate, UserUpdate

logger = structlog.get_logger()
settings = get_settings()


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

    # Client Portal Methods

    async def register_client(self, client_data: ClientRegister) -> tuple[User, str]:
        """Register a new client user with email verification.

        Returns:
            Tuple of (user, verification_token)
        """
        # Check if user already exists
        existing_user = await self.get_by_email(client_data.email)
        if existing_user:
            raise ValidationException("User with this email already exists")

        # Generate username from email
        username = client_data.email.split("@")[0]
        base_username = username
        counter = 1
        while await self.get_by_username(username):
            username = f"{base_username}{counter}"
            counter += 1

        # Generate verification token
        verification_token = secrets.token_urlsafe(32)

        # Create new client user
        hashed_password = get_password_hash(client_data.password)
        user = User(
            email=client_data.email,
            username=username,
            hashed_password=hashed_password,
            first_name=client_data.first_name,
            last_name=client_data.last_name,
            company_name=client_data.company_name,
            phone=client_data.phone,
            role="client",
            is_active=True,
            is_approved=False,
            email_verified=False,
            email_verification_token=verification_token,
            email_verification_sent_at=datetime.utcnow(),
        )

        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)

        logger.info("Client registered", user_id=user.id, email=user.email)
        return user, verification_token

    async def verify_email(self, token: str) -> User:
        """Verify a user's email address."""
        result = await self.db.execute(
            select(User).where(User.email_verification_token == token)
        )
        user = result.scalar_one_or_none()

        if not user:
            raise ValidationException("Invalid or expired verification token")

        # Check if token is expired (24 hours)
        if user.email_verification_sent_at:
            expiry = user.email_verification_sent_at + timedelta(
                hours=settings.EMAIL_VERIFICATION_EXPIRE_HOURS
            )
            if datetime.utcnow() > expiry:
                raise ValidationException("Verification token has expired")

        # Mark email as verified
        user.email_verified = True
        user.email_verification_token = None
        user.email_verification_sent_at = None

        await self.db.commit()
        await self.db.refresh(user)

        logger.info("Email verified", user_id=user.id, email=user.email)
        return user

    async def resend_verification(self, email: str) -> tuple[User, str]:
        """Resend verification email.

        Returns:
            Tuple of (user, new_verification_token)
        """
        user = await self.get_by_email(email)
        if not user:
            raise NotFoundException("User not found")

        if user.email_verified:
            raise ValidationException("Email is already verified")

        # Generate new verification token
        verification_token = secrets.token_urlsafe(32)
        user.email_verification_token = verification_token
        user.email_verification_sent_at = datetime.utcnow()

        await self.db.commit()
        await self.db.refresh(user)

        logger.info("Verification email resent", user_id=user.id, email=user.email)
        return user, verification_token

    async def request_password_reset(self, email: str) -> Optional[tuple[User, str]]:
        """Request a password reset.

        Returns:
            Tuple of (user, reset_token) or None if user not found
        """
        user = await self.get_by_email(email)
        if not user:
            # Don't reveal if user exists
            return None

        # Generate reset token
        reset_token = secrets.token_urlsafe(32)
        user.password_reset_token = reset_token
        user.password_reset_sent_at = datetime.utcnow()

        await self.db.commit()
        await self.db.refresh(user)

        logger.info("Password reset requested", user_id=user.id, email=user.email)
        return user, reset_token

    async def reset_password(self, token: str, new_password: str) -> User:
        """Reset user's password using reset token."""
        result = await self.db.execute(
            select(User).where(User.password_reset_token == token)
        )
        user = result.scalar_one_or_none()

        if not user:
            raise ValidationException("Invalid or expired reset token")

        # Check if token is expired (1 hour)
        if user.password_reset_sent_at:
            expiry = user.password_reset_sent_at + timedelta(
                hours=settings.PASSWORD_RESET_EXPIRE_HOURS
            )
            if datetime.utcnow() > expiry:
                raise ValidationException("Reset token has expired")

        # Update password
        user.hashed_password = get_password_hash(new_password)
        user.password_reset_token = None
        user.password_reset_sent_at = None

        await self.db.commit()
        await self.db.refresh(user)

        logger.info("Password reset completed", user_id=user.id, email=user.email)
        return user

    # Admin Methods

    async def get_pending_users(self) -> List[User]:
        """Get all users pending approval."""
        result = await self.db.execute(
            select(User).where(
                and_(
                    User.role == "client",
                    User.is_approved == False,
                    User.email_verified == True,
                )
            ).order_by(User.created_at.desc())
        )
        return list(result.scalars().all())

    async def approve_user(self, user_id: int, approved_by: User) -> User:
        """Approve a user's account."""
        user = await self.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")

        if user.is_approved:
            raise ValidationException("User is already approved")

        user.is_approved = True
        user.approved_at = datetime.utcnow()
        user.approved_by_id = approved_by.id

        # Create default features for the user
        for feature_key in DEFAULT_FEATURES:
            feature = UserFeature(
                user_id=user.id,
                feature_key=feature_key,
                enabled=True,
            )
            self.db.add(feature)

        await self.db.commit()
        await self.db.refresh(user)

        logger.info(
            "User approved",
            user_id=user.id,
            email=user.email,
            approved_by=approved_by.id,
        )
        return user

    async def reject_user(self, user_id: int, reason: Optional[str] = None) -> User:
        """Reject a user's application (delete the user)."""
        user = await self.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")

        if user.is_approved:
            raise ValidationException("Cannot reject an already approved user")

        # Store user info for logging before deletion
        user_email = user.email
        logger.info(
            "User rejected",
            user_id=user_id,
            email=user_email,
            reason=reason,
        )

        await self.db.delete(user)
        await self.db.commit()

        return user

    async def deactivate_user(self, user_id: int) -> User:
        """Deactivate a user's account."""
        user = await self.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")

        user.is_active = False
        await self.db.commit()
        await self.db.refresh(user)

        logger.info("User deactivated", user_id=user.id, email=user.email)
        return user

    async def reactivate_user(self, user_id: int) -> User:
        """Reactivate a user's account."""
        user = await self.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")

        user.is_active = True
        await self.db.commit()
        await self.db.refresh(user)

        logger.info("User reactivated", user_id=user.id, email=user.email)
        return user

    async def get_user_features(self, user_id: int) -> Dict[str, bool]:
        """Get user's feature flags."""
        user = await self.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")

        result = await self.db.execute(
            select(UserFeature).where(UserFeature.user_id == user_id)
        )
        features = result.scalars().all()

        return {f.feature_key: f.enabled for f in features}

    async def update_user_features(
        self, user_id: int, features: Dict[str, bool]
    ) -> Dict[str, bool]:
        """Update user's feature flags."""
        user = await self.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")

        for feature_key, enabled in features.items():
            # Check if feature exists
            result = await self.db.execute(
                select(UserFeature).where(
                    and_(
                        UserFeature.user_id == user_id,
                        UserFeature.feature_key == feature_key,
                    )
                )
            )
            feature = result.scalar_one_or_none()

            if feature:
                feature.enabled = enabled
            else:
                # Create new feature
                feature = UserFeature(
                    user_id=user_id,
                    feature_key=feature_key,
                    enabled=enabled,
                )
                self.db.add(feature)

        await self.db.commit()

        logger.info("User features updated", user_id=user_id, features=features)
        return await self.get_user_features(user_id)
