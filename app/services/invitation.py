"""Invitation service for managing user invitations."""

import secrets
from datetime import datetime, timedelta
from typing import List, Optional

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.core.config import get_settings
from app.core.exceptions import NotFoundException, ValidationException
from app.core.security import get_password_hash
from app.models.invitation import Invitation
from app.models.user import User
from app.models.user_feature import DEFAULT_FEATURES, UserFeature
from app.schemas.invitation import InvitationAccept

logger = structlog.get_logger()
settings = get_settings()


class InvitationService:
    """Service for invitation-related operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_id(self, invitation_id: int) -> Optional[Invitation]:
        """Get invitation by ID."""
        result = await self.db.execute(
            select(Invitation)
            .options(joinedload(Invitation.invited_by))
            .where(Invitation.id == invitation_id)
        )
        return result.scalar_one_or_none()

    async def get_by_token(self, token: str) -> Optional[Invitation]:
        """Get invitation by token."""
        result = await self.db.execute(
            select(Invitation)
            .options(joinedload(Invitation.invited_by))
            .where(Invitation.token == token)
        )
        return result.scalar_one_or_none()

    async def get_by_email(self, email: str) -> Optional[Invitation]:
        """Get pending invitation by email."""
        result = await self.db.execute(
            select(Invitation)
            .options(joinedload(Invitation.invited_by))
            .where(
                Invitation.email == email,
                Invitation.used_at.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def get_all(self) -> List[Invitation]:
        """Get all invitations."""
        result = await self.db.execute(
            select(Invitation)
            .options(joinedload(Invitation.invited_by))
            .order_by(Invitation.created_at.desc())
        )
        return list(result.scalars().all())

    async def create(self, email: str, invited_by: User) -> tuple[Invitation, str]:
        """Create a new invitation.

        Returns:
            Tuple of (invitation, token)
        """
        # Check if user already exists
        from app.services.user import UserService
        user_service = UserService(self.db)
        existing_user = await user_service.get_by_email(email)
        if existing_user:
            raise ValidationException("A user with this email already exists")

        # Check if there's already a valid invitation for this email
        existing_invitation = await self.get_by_email(email)
        if existing_invitation and existing_invitation.is_valid:
            raise ValidationException("A valid invitation already exists for this email")

        # Generate token
        token = secrets.token_urlsafe(32)

        # Calculate expiry date
        expires_at = datetime.utcnow() + timedelta(days=settings.INVITATION_EXPIRE_DAYS)

        invitation = Invitation(
            email=email,
            token=token,
            invited_by_id=invited_by.id,
            expires_at=expires_at,
        )

        self.db.add(invitation)
        await self.db.commit()
        await self.db.refresh(invitation)

        logger.info(
            "Invitation created",
            invitation_id=invitation.id,
            email=email,
            invited_by=invited_by.id,
        )
        return invitation, token

    async def create_bulk(
        self, emails: List[str], invited_by: User
    ) -> tuple[List[str], List[dict]]:
        """Create bulk invitations.

        Returns:
            Tuple of (successful_emails, failed_emails_with_reasons)
        """
        successful = []
        failed = []

        for email in emails:
            try:
                invitation, token = await self.create(email, invited_by)
                successful.append(email)
            except ValidationException as e:
                failed.append({"email": email, "reason": str(e.message)})

        return successful, failed

    async def validate_token(self, token: str) -> tuple[bool, Optional[str], Optional[str]]:
        """Validate an invitation token.

        Returns:
            Tuple of (is_valid, email_or_none, error_message_or_none)
        """
        invitation = await self.get_by_token(token)

        if not invitation:
            return False, None, "Invalid invitation token"

        if invitation.is_used:
            return False, None, "This invitation has already been used"

        if invitation.is_expired:
            return False, None, "This invitation has expired"

        return True, invitation.email, None

    async def accept(self, token: str, data: InvitationAccept) -> User:
        """Accept an invitation and create a user account."""
        invitation = await self.get_by_token(token)

        if not invitation:
            raise ValidationException("Invalid invitation token")

        if invitation.is_used:
            raise ValidationException("This invitation has already been used")

        if invitation.is_expired:
            raise ValidationException("This invitation has expired")

        # Check if user already exists (in case they registered separately)
        from app.services.user import UserService
        user_service = UserService(self.db)
        existing_user = await user_service.get_by_email(invitation.email)
        if existing_user:
            raise ValidationException("A user with this email already exists")

        # Generate username from email
        username = invitation.email.split("@")[0]
        base_username = username
        counter = 1
        while await user_service.get_by_username(username):
            username = f"{base_username}{counter}"
            counter += 1

        # Create user (pre-approved and pre-verified since they were invited)
        hashed_password = get_password_hash(data.password)
        user = User(
            email=invitation.email,
            username=username,
            hashed_password=hashed_password,
            first_name=data.first_name,
            last_name=data.last_name,
            company_name=data.company_name,
            phone=data.phone,
            role="client",
            is_active=True,
            is_approved=True,  # Pre-approved since invited
            email_verified=True,  # Pre-verified since invited
            approved_at=datetime.utcnow(),
            approved_by_id=invitation.invited_by_id,
        )

        self.db.add(user)

        # Mark invitation as used
        invitation.used_at = datetime.utcnow()

        await self.db.commit()
        await self.db.refresh(user)

        # Create default features for the user
        for feature_key in DEFAULT_FEATURES:
            feature = UserFeature(
                user_id=user.id,
                feature_key=feature_key,
                enabled=True,
            )
            self.db.add(feature)

        await self.db.commit()

        logger.info(
            "Invitation accepted",
            invitation_id=invitation.id,
            user_id=user.id,
            email=user.email,
        )
        return user

    async def resend(self, invitation_id: int) -> tuple[Invitation, str]:
        """Resend an invitation with a new token.

        Returns:
            Tuple of (invitation, new_token)
        """
        invitation = await self.get_by_id(invitation_id)
        if not invitation:
            raise NotFoundException("Invitation not found")

        if invitation.is_used:
            raise ValidationException("Cannot resend a used invitation")

        # Generate new token and extend expiry
        new_token = secrets.token_urlsafe(32)
        invitation.token = new_token
        invitation.expires_at = datetime.utcnow() + timedelta(
            days=settings.INVITATION_EXPIRE_DAYS
        )

        await self.db.commit()
        await self.db.refresh(invitation)

        logger.info(
            "Invitation resent",
            invitation_id=invitation.id,
            email=invitation.email,
        )
        return invitation, new_token

    async def revoke(self, invitation_id: int) -> bool:
        """Revoke (delete) an invitation."""
        invitation = await self.get_by_id(invitation_id)
        if not invitation:
            raise NotFoundException("Invitation not found")

        if invitation.is_used:
            raise ValidationException("Cannot revoke a used invitation")

        await self.db.delete(invitation)
        await self.db.commit()

        logger.info(
            "Invitation revoked",
            invitation_id=invitation_id,
            email=invitation.email,
        )
        return True
