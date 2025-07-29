from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.audit import audit_action
from app.core.database import get_db
from app.core.deps import get_current_superuser, get_current_user
from app.models.audit_log import AuditAction
from app.models.user import User
from app.schemas.audit_log import AuditLog, AuditLogFilter, AuditLogSummary
from app.services.audit_log import AuditLogService

router = APIRouter()


@router.get("/", response_model=List[AuditLog])
@audit_action(AuditAction.ACCESS, "audit_log", description="Listed audit logs")
async def get_audit_logs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    user_id: Optional[int] = Query(None),
    user_email: Optional[str] = Query(None),
    action: Optional[AuditAction] = Query(None),
    resource_type: Optional[str] = Query(None),
    resource_id: Optional[str] = Query(None),
    ip_address: Optional[str] = Query(None),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),  # Only superusers can view audit logs
):
    """Get audit logs with optional filtering. Requires superuser access."""
    filters = AuditLogFilter(
        user_id=user_id,
        user_email=user_email,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        ip_address=ip_address,
        date_from=date_from,
        date_to=date_to,
        search=search,
    )
    return await AuditLogService.get_audit_logs(db, filters=filters, skip=skip, limit=limit)


@router.get("/count", response_model=int)
@audit_action(AuditAction.ACCESS, "audit_log", description="Counted audit logs")
async def count_audit_logs(
    user_id: Optional[int] = Query(None),
    user_email: Optional[str] = Query(None),
    action: Optional[AuditAction] = Query(None),
    resource_type: Optional[str] = Query(None),
    resource_id: Optional[str] = Query(None),
    ip_address: Optional[str] = Query(None),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
):
    """Count audit logs with optional filtering. Requires superuser access."""
    filters = AuditLogFilter(
        user_id=user_id,
        user_email=user_email,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        ip_address=ip_address,
        date_from=date_from,
        date_to=date_to,
        search=search,
    )
    return await AuditLogService.count_audit_logs(db, filters=filters)


@router.get("/summary", response_model=AuditLogSummary)
@audit_action(AuditAction.ACCESS, "audit_log", description="Viewed audit log summary")
async def get_audit_summary(
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
):
    """Get audit log summary statistics. Requires superuser access."""
    filters = AuditLogFilter(date_from=date_from, date_to=date_to)
    return await AuditLogService.get_audit_summary(db, filters=filters)


@router.get("/{log_id}", response_model=AuditLog)
@audit_action(
    AuditAction.ACCESS,
    "audit_log",
    lambda result, *args, **kwargs: str(kwargs.get("log_id", "unknown")),
    description="Viewed audit log",
)
async def get_audit_log(
    log_id: int,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
):
    """Get a specific audit log by ID. Requires superuser access."""
    audit_log = await AuditLogService.get_audit_log(db, log_id)
    if not audit_log:
        raise HTTPException(status_code=404, detail="Audit log not found")
    return audit_log


@router.get("/resource/{resource_type}/{resource_id}", response_model=List[AuditLog])
@audit_action(AuditAction.ACCESS, "audit_log", description="Viewed resource audit history")
async def get_resource_audit_history(
    resource_type: str,
    resource_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
):
    """Get audit history for a specific resource. Requires superuser access."""
    return await AuditLogService.get_resource_audit_history(
        db, resource_type=resource_type, resource_id=resource_id, skip=skip, limit=limit
    )


@router.get("/user/{user_id}/history", response_model=List[AuditLog])
@audit_action(AuditAction.ACCESS, "audit_log", description="Viewed user audit history")
async def get_user_audit_history(
    user_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_user),
):
    """Get audit history for a specific user. Users can only view their own history unless they're superusers."""
    # Users can only view their own audit history unless they're superusers
    if not current_user.is_superuser and current_user.id != user_id:
        raise HTTPException(status_code=403, detail="Not enough permissions")

    return await AuditLogService.get_user_audit_history(db, user_id=user_id, skip=skip, limit=limit)


@router.get("/my/history", response_model=List[AuditLog])
@audit_action(AuditAction.ACCESS, "audit_log", description="Viewed own audit history")
async def get_my_audit_history(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_user),
):
    """Get audit history for the current user."""
    return await AuditLogService.get_user_audit_history(
        db, user_id=current_user.id, skip=skip, limit=limit
    )
