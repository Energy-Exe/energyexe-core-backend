"""Authentication endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.audit import audit_action
from app.core.deps import get_db
from app.core.exceptions import AuthenticationException, NotFoundException, ValidationException
from app.core.security import create_access_token
from app.models.audit_log import AuditAction
from app.schemas.invitation import InvitationAccept, InvitationValidation
from app.schemas.user import (
    ClientRegister,
    EmailVerification,
    ForgotPassword,
    ResendVerification,
    ResetPassword,
    Token,
    UserCreate,
    UserLogin,
    UserResponse,
    UserResponseExtended,
)
from app.services.audit_log import AuditLogService
from app.services.email import email_service
from app.services.invitation import InvitationService
from app.services.user import UserService

logger = structlog.get_logger()

router = APIRouter()


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
@audit_action(AuditAction.CREATE, "user", description="User registered")
async def register(
    user_data: UserCreate,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
):
    """Register a new user."""
    user_service = UserService(db)

    try:
        user = await user_service.create(user_data)
        logger.info("User registered successfully", user_id=user.id, username=user.username)
        return user
    except ValidationException as e:
        logger.warning("Registration failed", error=e.message)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


@router.post("/login", response_model=Token)
async def login(
    login_data: UserLogin,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
):
    """Login with JSON data and get access token."""
    user_service = UserService(db)

    user = await user_service.authenticate(login_data.username, login_data.password)

    if not user:
        logger.warning("Login failed", username=login_data.username)
        # Log failed login attempt
        await AuditLogService.log_action(
            db=db,
            action=AuditAction.LOGIN,
            resource_type="user",
            user_email=login_data.username,
            description="Failed login attempt",
            ip_address=request.client.host if request and request.client else None,
            user_agent=request.headers.get("User-Agent") if request else None,
            endpoint=str(request.url.path) if request else None,
            method=request.method if request else None,
            extra_metadata={"success": False, "username": login_data.username},
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(subject=user.username)
    logger.info("User logged in successfully", user_id=user.id, username=user.username)

    # Log successful login
    await AuditLogService.log_action(
        db=db,
        action=AuditAction.LOGIN,
        resource_type="user",
        user_id=user.id,
        user_email=user.email,
        resource_id=str(user.id),
        resource_name=user.username,
        description="Successful login",
        ip_address=request.client.host if request and request.client else None,
        user_agent=request.headers.get("User-Agent") if request else None,
        endpoint=str(request.url.path) if request else None,
        method=request.method if request else None,
        extra_metadata={"success": True, "username": user.username},
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
    }


@router.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
):
    """OAuth2 compatible login endpoint (for Swagger UI)."""
    user_service = UserService(db)

    user = await user_service.authenticate(form_data.username, form_data.password)

    if not user:
        logger.warning("Login failed", username=form_data.username)
        # Log failed login attempt
        await AuditLogService.log_action(
            db=db,
            action=AuditAction.LOGIN,
            resource_type="user",
            user_email=form_data.username,
            description="Failed OAuth2 login attempt",
            ip_address=request.client.host if request and request.client else None,
            user_agent=request.headers.get("User-Agent") if request else None,
            endpoint=str(request.url.path) if request else None,
            method=request.method if request else None,
            extra_metadata={
                "success": False,
                "username": form_data.username,
                "login_type": "oauth2",
            },
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(subject=user.username)
    logger.info("User logged in successfully", user_id=user.id, username=user.username)

    # Log successful login
    await AuditLogService.log_action(
        db=db,
        action=AuditAction.LOGIN,
        resource_type="user",
        user_id=user.id,
        user_email=user.email,
        resource_id=str(user.id),
        resource_name=user.username,
        description="Successful OAuth2 login",
        ip_address=request.client.host if request and request.client else None,
        user_agent=request.headers.get("User-Agent") if request else None,
        endpoint=str(request.url.path) if request else None,
        method=request.method if request else None,
        extra_metadata={"success": True, "username": user.username, "login_type": "oauth2"},
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
    }


# Client Portal Authentication Endpoints


@router.post(
    "/client/register",
    response_model=UserResponseExtended,
    status_code=status.HTTP_201_CREATED,
)
async def register_client(
    client_data: ClientRegister,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
):
    """Register a new client user.

    This creates a new client account that requires:
    1. Email verification
    2. Admin approval before full access
    """
    user_service = UserService(db)

    try:
        user, verification_token = await user_service.register_client(client_data)

        # Extract all needed data BEFORE any async operations to avoid DetachedInstanceError
        user_data = {
            "id": user.id,
            "email": user.email,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "is_active": user.is_active,
            "is_superuser": user.is_superuser,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
            "role": user.role,
            "is_approved": user.is_approved,
            "email_verified": user.email_verified,
            "company_name": user.company_name,
            "phone": user.phone,
            "approved_at": user.approved_at,
            "features": None,
        }
        user_email = user.email

        # Send verification email (can fail silently - user is already registered)
        await email_service.send_verification_email(user, verification_token)

        logger.info(
            "Client registered, verification email sent",
            user_id=user_data["id"],
            email=user_email,
        )
        return user_data
    except ValidationException as e:
        logger.warning("Client registration failed", error=e.message)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


@router.post("/verify-email")
async def verify_email(
    verification: EmailVerification,
    db: AsyncSession = Depends(get_db),
):
    """Verify a user's email address."""
    user_service = UserService(db)

    try:
        user = await user_service.verify_email(verification.token)
        return {
            "message": "Email verified successfully",
            "email": user.email,
            "is_approved": user.is_approved,
        }
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


@router.post("/resend-verification")
async def resend_verification(
    data: ResendVerification,
    db: AsyncSession = Depends(get_db),
):
    """Resend email verification link."""
    user_service = UserService(db)

    try:
        user, verification_token = await user_service.resend_verification(data.email)
        await email_service.send_verification_email(user, verification_token)

        return {"message": "Verification email sent"}
    except NotFoundException:
        # Don't reveal if user exists
        return {"message": "If an account exists, a verification email has been sent"}
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


@router.post("/forgot-password")
async def forgot_password(
    data: ForgotPassword,
    db: AsyncSession = Depends(get_db),
):
    """Request a password reset email."""
    user_service = UserService(db)

    result = await user_service.request_password_reset(data.email)

    if result:
        user, reset_token = result
        await email_service.send_password_reset_email(user, reset_token)

    # Always return success to not reveal if user exists
    return {"message": "If an account exists, a password reset email has been sent"}


@router.post("/reset-password")
async def reset_password(
    data: ResetPassword,
    db: AsyncSession = Depends(get_db),
):
    """Reset password using the reset token."""
    user_service = UserService(db)

    try:
        user = await user_service.reset_password(data.token, data.new_password)

        # Send confirmation email
        await email_service.send_password_changed_email(user)

        return {"message": "Password reset successfully"}
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)


# Invitation Endpoints


@router.get("/invitation/{token}", response_model=InvitationValidation)
async def validate_invitation(
    token: str,
    db: AsyncSession = Depends(get_db),
):
    """Validate an invitation token."""
    invitation_service = InvitationService(db)

    is_valid, email, message = await invitation_service.validate_token(token)

    return InvitationValidation(
        valid=is_valid,
        email=email,
        message=message,
    )


@router.post(
    "/invitation/{token}/accept",
    response_model=UserResponseExtended,
    status_code=status.HTTP_201_CREATED,
)
async def accept_invitation(
    token: str,
    data: InvitationAccept,
    db: AsyncSession = Depends(get_db),
):
    """Accept an invitation and create an account."""
    invitation_service = InvitationService(db)

    try:
        user = await invitation_service.accept(token, data)

        # Extract all needed data to avoid DetachedInstanceError
        user_data = {
            "id": user.id,
            "email": user.email,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "is_active": user.is_active,
            "is_superuser": user.is_superuser,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
            "role": user.role,
            "is_approved": user.is_approved,
            "email_verified": user.email_verified,
            "company_name": user.company_name,
            "phone": user.phone,
            "approved_at": user.approved_at,
            "features": None,
        }

        logger.info(
            "Invitation accepted, user created",
            user_id=user_data["id"],
            email=user_data["email"],
        )
        return user_data
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)
