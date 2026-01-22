"""Admin endpoints for user management."""

from typing import Dict, List

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_admin_user, get_db
from app.core.exceptions import NotFoundException, ValidationException
from app.models.user import User
from app.schemas.invitation import (
    BulkInvitationCreate,
    BulkInvitationResult,
    InvitationCreate,
    InvitationResponse,
)
from app.schemas.user import (
    PendingUserResponse,
    UserApproval,
    UserFeatureUpdate,
    UserResponseExtended,
)
from app.services.email import email_service
from app.services.invitation import InvitationService
from app.services.user import UserService

logger = structlog.get_logger()

router = APIRouter()


# User Approval Endpoints


@router.get("/users/pending", response_model=List[PendingUserResponse])
async def get_pending_users(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Get all users pending approval."""
    user_service = UserService(db)
    return await user_service.get_pending_users()


@router.post("/users/{user_id}/approve", response_model=UserResponseExtended)
async def approve_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Approve a user's account."""
    user_service = UserService(db)

    try:
        user = await user_service.approve_user(user_id, current_user)

        # Send approval email
        await email_service.send_approval_email(user)

        logger.info(
            "User approved",
            user_id=user.id,
            approved_by=current_user.id,
        )
        return user
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


@router.post("/users/{user_id}/reject")
async def reject_user(
    user_id: int,
    approval: UserApproval,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Reject a user's account application."""
    user_service = UserService(db)

    try:
        user = await user_service.get_by_id(user_id)
        if not user:
            raise NotFoundException("User not found")

        # Send rejection email before deleting
        await email_service.send_rejection_email(user, approval.reason)

        await user_service.reject_user(user_id, approval.reason)

        logger.info(
            "User rejected",
            user_id=user_id,
            rejected_by=current_user.id,
            reason=approval.reason,
        )
        return {"message": "User application rejected"}
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


@router.post("/users/{user_id}/deactivate", response_model=UserResponseExtended)
async def deactivate_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Deactivate a user's account."""
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot deactivate your own account",
        )

    user_service = UserService(db)

    try:
        user = await user_service.deactivate_user(user_id)
        logger.info(
            "User deactivated",
            user_id=user.id,
            deactivated_by=current_user.id,
        )
        return user
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


@router.post("/users/{user_id}/reactivate", response_model=UserResponseExtended)
async def reactivate_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Reactivate a user's account."""
    user_service = UserService(db)

    try:
        user = await user_service.reactivate_user(user_id)
        logger.info(
            "User reactivated",
            user_id=user.id,
            reactivated_by=current_user.id,
        )
        return user
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


# User Features Endpoints


@router.get("/users/{user_id}/features", response_model=Dict[str, bool])
async def get_user_features(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Get user's feature flags."""
    user_service = UserService(db)

    try:
        return await user_service.get_user_features(user_id)
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


@router.put("/users/{user_id}/features", response_model=Dict[str, bool])
async def update_user_features(
    user_id: int,
    features_update: UserFeatureUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Update user's feature flags."""
    user_service = UserService(db)

    try:
        features = await user_service.update_user_features(
            user_id, features_update.features
        )
        logger.info(
            "User features updated",
            user_id=user_id,
            updated_by=current_user.id,
            features=features_update.features,
        )
        return features
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


# Invitation Endpoints


@router.get("/invitations", response_model=List[InvitationResponse])
async def get_invitations(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Get all invitations."""
    invitation_service = InvitationService(db)
    invitations = await invitation_service.get_all()

    # Add invited_by_name to each invitation
    result = []
    for inv in invitations:
        inv_dict = {
            "id": inv.id,
            "email": inv.email,
            "invited_by_id": inv.invited_by_id,
            "invited_by_name": f"{inv.invited_by.first_name or ''} {inv.invited_by.last_name or ''}".strip()
            or inv.invited_by.username
            if inv.invited_by
            else None,
            "expires_at": inv.expires_at,
            "used_at": inv.used_at,
            "created_at": inv.created_at,
            "is_expired": inv.is_expired,
            "is_used": inv.is_used,
            "is_valid": inv.is_valid,
        }
        result.append(InvitationResponse(**inv_dict))

    return result


@router.post("/invitations", response_model=InvitationResponse, status_code=status.HTTP_201_CREATED)
async def create_invitation(
    invitation_data: InvitationCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Create a single invitation."""
    invitation_service = InvitationService(db)

    try:
        invitation, token = await invitation_service.create(
            invitation_data.email, current_user
        )

        # Send invitation email
        invited_by_name = (
            f"{current_user.first_name or ''} {current_user.last_name or ''}".strip()
            or current_user.username
        )
        await email_service.send_invitation_email(
            invitation_data.email, token, invited_by_name
        )

        logger.info(
            "Invitation created and sent",
            invitation_id=invitation.id,
            email=invitation_data.email,
            invited_by=current_user.id,
        )

        return InvitationResponse(
            id=invitation.id,
            email=invitation.email,
            invited_by_id=invitation.invited_by_id,
            invited_by_name=invited_by_name,
            expires_at=invitation.expires_at,
            used_at=invitation.used_at,
            created_at=invitation.created_at,
            is_expired=invitation.is_expired,
            is_used=invitation.is_used,
            is_valid=invitation.is_valid,
        )
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


@router.post("/invitations/bulk", response_model=BulkInvitationResult)
async def create_bulk_invitations(
    bulk_data: BulkInvitationCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Create bulk invitations."""
    invitation_service = InvitationService(db)

    successful, failed = await invitation_service.create_bulk(
        bulk_data.emails, current_user
    )

    # Send emails for successful invitations
    invited_by_name = (
        f"{current_user.first_name or ''} {current_user.last_name or ''}".strip()
        or current_user.username
    )

    for email in successful:
        # Get the invitation to get the token
        invitation = await invitation_service.get_by_email(email)
        if invitation:
            await email_service.send_invitation_email(
                email, invitation.token, invited_by_name
            )

    logger.info(
        "Bulk invitations created",
        total=len(bulk_data.emails),
        successful=len(successful),
        failed=len(failed),
        invited_by=current_user.id,
    )

    return BulkInvitationResult(
        successful=successful,
        failed=failed,
        total_sent=len(successful),
        total_failed=len(failed),
    )


@router.post("/invitations/{invitation_id}/resend", response_model=InvitationResponse)
async def resend_invitation(
    invitation_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Resend an invitation email."""
    invitation_service = InvitationService(db)

    try:
        invitation, token = await invitation_service.resend(invitation_id)

        # Send invitation email
        invited_by_name = (
            f"{current_user.first_name or ''} {current_user.last_name or ''}".strip()
            or current_user.username
        )
        await email_service.send_invitation_email(
            invitation.email, token, invited_by_name
        )

        logger.info(
            "Invitation resent",
            invitation_id=invitation.id,
            email=invitation.email,
            resent_by=current_user.id,
        )

        return InvitationResponse(
            id=invitation.id,
            email=invitation.email,
            invited_by_id=invitation.invited_by_id,
            invited_by_name=invited_by_name,
            expires_at=invitation.expires_at,
            used_at=invitation.used_at,
            created_at=invitation.created_at,
            is_expired=invitation.is_expired,
            is_used=invitation.is_used,
            is_valid=invitation.is_valid,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


@router.delete("/invitations/{invitation_id}")
async def revoke_invitation(
    invitation_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
):
    """Revoke an invitation."""
    invitation_service = InvitationService(db)

    try:
        await invitation_service.revoke(invitation_id)

        logger.info(
            "Invitation revoked",
            invitation_id=invitation_id,
            revoked_by=current_user.id,
        )

        return {"message": "Invitation revoked"}
    except NotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)
