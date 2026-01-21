"""Alerts API endpoints for managing user alerts and notifications."""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.models.alert import AlertTriggerStatus, NotificationStatus
from app.services.alert_service import AlertService
from app.schemas.alert import (
    AlertRuleCreate,
    AlertRuleUpdate,
    AlertRuleResponse,
    AlertRuleListResponse,
    AlertTriggerResponse,
    AlertTriggerListResponse,
    AlertTriggerStatusUpdate,
    NotificationResponse,
    NotificationListResponse,
    NotificationMarkRead,
    NotificationPreferenceUpdate,
    NotificationPreferenceResponse,
    AlertsSummary,
    AlertsOverview,
)

router = APIRouter()


# ============================================================================
# ALERT RULES ENDPOINTS
# ============================================================================

@router.get("/rules", response_model=AlertRuleListResponse)
async def list_alert_rules(
    is_enabled: Optional[bool] = Query(None, description="Filter by enabled status"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List all alert rules for the current user."""
    service = AlertService(db)
    rules = await service.list_alert_rules(current_user.id, is_enabled=is_enabled)
    return {
        "rules": rules,
        "total": len(rules),
    }


@router.post("/rules", response_model=AlertRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_alert_rule(
    data: AlertRuleCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a new alert rule."""
    service = AlertService(db)
    rule = await service.create_alert_rule(current_user.id, data)
    rules = await service.list_alert_rules(current_user.id)
    # Find the newly created rule with all details
    for r in rules:
        if r["id"] == rule.id:
            return r
    return rule


@router.get("/rules/{rule_id}", response_model=AlertRuleResponse)
async def get_alert_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get an alert rule by ID."""
    service = AlertService(db)
    rules = await service.list_alert_rules(current_user.id)
    for r in rules:
        if r["id"] == rule_id:
            return r
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Alert rule not found",
    )


@router.put("/rules/{rule_id}", response_model=AlertRuleResponse)
async def update_alert_rule(
    rule_id: int,
    data: AlertRuleUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update an alert rule."""
    service = AlertService(db)
    rule = await service.update_alert_rule(rule_id, current_user.id, data)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert rule not found",
        )
    rules = await service.list_alert_rules(current_user.id)
    for r in rules:
        if r["id"] == rule.id:
            return r
    return rule


@router.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_alert_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete an alert rule."""
    service = AlertService(db)
    deleted = await service.delete_alert_rule(rule_id, current_user.id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert rule not found",
        )


@router.post("/rules/{rule_id}/toggle", response_model=AlertRuleResponse)
async def toggle_alert_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Toggle an alert rule's enabled status."""
    service = AlertService(db)
    rule = await service.toggle_alert_rule(rule_id, current_user.id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert rule not found",
        )
    rules = await service.list_alert_rules(current_user.id)
    for r in rules:
        if r["id"] == rule.id:
            return r
    return rule


# ============================================================================
# ALERT TRIGGERS ENDPOINTS
# ============================================================================

@router.get("/triggers", response_model=AlertTriggerListResponse)
async def list_alert_triggers(
    status: Optional[str] = Query(None, description="Filter by status (active, acknowledged, resolved)"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List alert triggers for the current user's rules."""
    service = AlertService(db)
    trigger_status = AlertTriggerStatus(status) if status else None
    result = await service.list_triggers(
        current_user.id,
        status=trigger_status,
        limit=limit,
        offset=offset,
    )
    return result


@router.patch("/triggers/{trigger_id}/status", response_model=AlertTriggerResponse)
async def update_trigger_status(
    trigger_id: int,
    data: AlertTriggerStatusUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update an alert trigger's status."""
    service = AlertService(db)
    trigger = await service.update_trigger_status(trigger_id, current_user.id, data)
    if not trigger:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert trigger not found",
        )
    # Re-fetch to get related data
    result = await service.list_triggers(current_user.id, limit=100)
    for t in result["triggers"]:
        if t["id"] == trigger.id:
            return t
    return trigger


@router.post("/triggers/{trigger_id}/acknowledge", response_model=AlertTriggerResponse)
async def acknowledge_trigger(
    trigger_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Acknowledge an alert trigger."""
    service = AlertService(db)
    trigger = await service.acknowledge_trigger(trigger_id, current_user.id)
    if not trigger:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert trigger not found",
        )
    result = await service.list_triggers(current_user.id, limit=100)
    for t in result["triggers"]:
        if t["id"] == trigger.id:
            return t
    return trigger


@router.post("/triggers/{trigger_id}/resolve", response_model=AlertTriggerResponse)
async def resolve_trigger(
    trigger_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Resolve an alert trigger."""
    service = AlertService(db)
    trigger = await service.resolve_trigger(trigger_id, current_user.id)
    if not trigger:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert trigger not found",
        )
    result = await service.list_triggers(current_user.id, limit=100)
    for t in result["triggers"]:
        if t["id"] == trigger.id:
            return t
    return trigger


# ============================================================================
# NOTIFICATIONS ENDPOINTS
# ============================================================================

@router.get("/notifications", response_model=NotificationListResponse)
async def list_notifications(
    status: Optional[str] = Query(None, description="Filter by status (unread, read, archived)"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List notifications for the current user."""
    service = AlertService(db)
    notification_status = NotificationStatus(status) if status else None
    result = await service.list_notifications(
        current_user.id,
        status=notification_status,
        limit=limit,
        offset=offset,
    )
    return result


@router.post("/notifications/mark-read")
async def mark_notifications_read(
    data: NotificationMarkRead,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Mark specific notifications as read."""
    service = AlertService(db)
    count = await service.mark_notifications_read(current_user.id, data.notification_ids)
    return {"marked_read": count}


@router.post("/notifications/mark-all-read")
async def mark_all_notifications_read(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Mark all notifications as read."""
    service = AlertService(db)
    count = await service.mark_all_notifications_read(current_user.id)
    return {"marked_read": count}


@router.delete("/notifications/{notification_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_notification(
    notification_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a notification."""
    service = AlertService(db)
    deleted = await service.delete_notification(notification_id, current_user.id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Notification not found",
        )


@router.get("/notifications/unread-count")
async def get_unread_count(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get count of unread notifications."""
    service = AlertService(db)
    count = await service.get_unread_count(current_user.id)
    return {"unread_count": count}


# ============================================================================
# NOTIFICATION PREFERENCES ENDPOINTS
# ============================================================================

@router.get("/preferences", response_model=NotificationPreferenceResponse)
async def get_notification_preferences(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get notification preferences for the current user."""
    service = AlertService(db)
    prefs = await service.get_notification_preferences(current_user.id)
    return prefs


@router.put("/preferences", response_model=NotificationPreferenceResponse)
async def update_notification_preferences(
    data: NotificationPreferenceUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update notification preferences for the current user."""
    service = AlertService(db)
    prefs = await service.update_notification_preferences(current_user.id, data)
    return prefs


# ============================================================================
# SUMMARY & OVERVIEW ENDPOINTS
# ============================================================================

@router.get("/summary", response_model=AlertsSummary)
async def get_alerts_summary(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get summary of alerts for dashboard."""
    service = AlertService(db)
    summary = await service.get_alerts_summary(current_user.id)
    return summary


@router.get("/overview", response_model=AlertsOverview)
async def get_alerts_overview(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get quick overview of alerts status."""
    service = AlertService(db)
    overview = await service.get_alerts_overview(current_user.id)
    return overview
