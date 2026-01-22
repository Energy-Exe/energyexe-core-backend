"""User schemas for request/response validation."""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, EmailStr, Field


class UserBase(BaseModel):
    """Base user schema."""

    email: EmailStr
    username: str = Field(..., min_length=3, max_length=100)
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    is_active: bool = True


class UserCreate(UserBase):
    """Schema for creating a user (admin creation)."""

    password: str = Field(..., min_length=8, max_length=100)


class ClientRegister(BaseModel):
    """Schema for client self-registration."""

    email: EmailStr
    password: str = Field(..., min_length=8, max_length=100)
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    company_name: str = Field(..., min_length=1, max_length=255)
    phone: Optional[str] = Field(None, max_length=50)


class UserUpdate(BaseModel):
    """Schema for updating a user."""

    email: Optional[EmailStr] = None
    username: Optional[str] = Field(None, min_length=3, max_length=100)
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    is_active: Optional[bool] = None
    password: Optional[str] = Field(None, min_length=8, max_length=100)
    company_name: Optional[str] = Field(None, max_length=255)
    phone: Optional[str] = Field(None, max_length=50)


class UserResponse(UserBase):
    """Schema for user response."""

    id: int
    is_superuser: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        """Pydantic config."""

        from_attributes = True


class UserResponseExtended(UserResponse):
    """Extended user response with client portal fields."""

    role: str
    is_approved: bool
    email_verified: bool
    company_name: Optional[str] = None
    phone: Optional[str] = None
    approved_at: Optional[datetime] = None
    features: Optional[Dict[str, bool]] = None


class UserLogin(BaseModel):
    """Schema for user login."""

    username: str = Field(..., min_length=3)
    password: str = Field(..., min_length=1)


class Token(BaseModel):
    """Schema for token response."""

    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Schema for token data."""

    username: Optional[str] = None


# Email verification schemas
class EmailVerification(BaseModel):
    """Schema for email verification request."""

    token: str


class ResendVerification(BaseModel):
    """Schema for resending verification email."""

    email: EmailStr


# Password reset schemas
class ForgotPassword(BaseModel):
    """Schema for forgot password request."""

    email: EmailStr


class ResetPassword(BaseModel):
    """Schema for password reset."""

    token: str
    new_password: str = Field(..., min_length=8, max_length=100)


# User approval schemas
class UserApproval(BaseModel):
    """Schema for user approval action."""

    approved: bool
    reason: Optional[str] = None


# User feature schemas
class UserFeatureUpdate(BaseModel):
    """Schema for updating user features."""

    features: Dict[str, bool]


class UserFeatureResponse(BaseModel):
    """Schema for user feature response."""

    feature_key: str
    enabled: bool

    class Config:
        """Pydantic config."""

        from_attributes = True


# Pending users list response
class PendingUserResponse(BaseModel):
    """Schema for pending user in list."""

    id: int
    email: str
    username: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    phone: Optional[str] = None
    email_verified: bool
    created_at: datetime

    class Config:
        """Pydantic config."""

        from_attributes = True
