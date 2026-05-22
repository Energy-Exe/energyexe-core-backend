"""Endpoints for the first-login Terms of Use / Privacy Policy flow."""

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, get_db
from app.models.user import User
from app.schemas.consent import ConsentAcceptRequest, ConsentStatusResponse
from app.services.consent import ConsentService

router = APIRouter()


@router.get("/me", response_model=ConsentStatusResponse)
async def get_my_consent_status(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ConsentStatusResponse:
    """Return whether the current user must (re-)accept the Terms / Privacy Policy."""
    return await ConsentService(db).get_status(current_user.id)


@router.post("/me/accept", response_model=ConsentStatusResponse)
async def accept_my_consents(
    body: ConsentAcceptRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ConsentStatusResponse:
    """Record the user's acceptance of both legal documents."""
    ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    return await ConsentService(db).record_acceptance(
        current_user.id,
        terms_version=body.terms_version,
        privacy_version=body.privacy_version,
        ip_address=ip,
        user_agent=user_agent,
    )
