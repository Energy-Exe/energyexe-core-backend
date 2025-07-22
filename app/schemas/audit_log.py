from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.models.audit_log import AuditAction


class AuditLogBase(BaseModel):
    """Base audit log schema."""
    action: AuditAction
    resource_type: str
    resource_id: Optional[str] = None
    resource_name: Optional[str] = None
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    endpoint: Optional[str] = None
    method: Optional[str] = None
    description: Optional[str] = None
    extra_metadata: Optional[Dict[str, Any]] = None


class AuditLogCreate(AuditLogBase):
    """Schema for creating audit log entries."""
    user_id: Optional[int] = None
    user_email: Optional[str] = None


class AuditLog(AuditLogBase):
    """Complete audit log schema."""
    id: int
    user_id: Optional[int] = None
    user_email: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class AuditLogFilter(BaseModel):
    """Schema for filtering audit logs."""
    user_id: Optional[int] = None
    user_email: Optional[str] = None
    action: Optional[AuditAction] = None
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    ip_address: Optional[str] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    search: Optional[str] = Field(None, description="Search in resource_name, description, or user_email")


class AuditLogSummary(BaseModel):
    """Summary statistics for audit logs."""
    total_actions: int
    actions_by_type: Dict[str, int]
    actions_by_user: List[Dict[str, Any]]
    actions_by_resource: Dict[str, int]
    date_range: Dict[str, Optional[datetime]]