"""Authentication endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_db
from app.core.exceptions import AuthenticationException, ValidationException
from app.core.security import create_access_token
from app.schemas.user import Token, UserCreate, UserLogin, UserResponse
from app.services.user import UserService

logger = structlog.get_logger()

router = APIRouter()


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(
    user_data: UserCreate,
    db: AsyncSession = Depends(get_db),
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
):
    """Login with JSON data and get access token."""
    user_service = UserService(db)

    user = await user_service.authenticate(login_data.username, login_data.password)

    if not user:
        logger.warning("Login failed", username=login_data.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(subject=user.username)
    logger.info("User logged in successfully", user_id=user.id, username=user.username)

    return {
        "access_token": access_token,
        "token_type": "bearer",
    }


@router.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
):
    """OAuth2 compatible login endpoint (for Swagger UI)."""
    user_service = UserService(db)

    user = await user_service.authenticate(form_data.username, form_data.password)

    if not user:
        logger.warning("Login failed", username=form_data.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(subject=user.username)
    logger.info("User logged in successfully", user_id=user.id, username=user.username)

    return {
        "access_token": access_token,
        "token_type": "bearer",
    }
