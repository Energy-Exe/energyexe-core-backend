"""Authentication endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.audit import audit_action
from app.core.deps import get_db
from app.core.exceptions import AuthenticationException, ValidationException
from app.core.security import create_access_token
from app.models.audit_log import AuditAction
from app.schemas.user import Token, UserCreate, UserLogin, UserResponse
from app.services.audit_log import AuditLogService
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
            extra_metadata={"success": False, "username": form_data.username, "login_type": "oauth2"},
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
