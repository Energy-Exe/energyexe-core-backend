"""Invitation schemas for request/response validation."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field


class InvitationCreate(BaseModel):
    """Schema for creating a single invitation."""

    email: EmailStr


class BulkInvitationCreate(BaseModel):
    """Schema for creating bulk invitations."""

    emails: List[EmailStr] = Field(..., min_length=1, max_length=100)


class InvitationAccept(BaseModel):
    """Schema for accepting an invitation."""

    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=8, max_length=100)
    company_name: str = Field(..., min_length=1, max_length=255)
    phone: Optional[str] = Field(None, max_length=50)


class InvitationResponse(BaseModel):
    """Schema for invitation response."""

    id: int
    email: str
    invited_by_id: int
    invited_by_name: Optional[str] = None
    expires_at: datetime
    used_at: Optional[datetime] = None
    created_at: datetime
    is_expired: bool
    is_used: bool
    is_valid: bool

    class Config:
        """Pydantic config."""

        from_attributes = True


class InvitationValidation(BaseModel):
    """Schema for invitation validation response."""

    valid: bool
    email: Optional[str] = None
    message: Optional[str] = None


class BulkInvitationResult(BaseModel):
    """Schema for bulk invitation result."""

    successful: List[str]
    failed: List[dict]
    total_sent: int
    total_failed: int
