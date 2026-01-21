"""Alert service for managing user alerts and notifications."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete, and_, update, or_
from sqlalchemy.orm import joinedload

from app.models.alert import (
    AlertRule,
    AlertTrigger,
    Notification,
    NotificationPreference,
    AlertMetric,
    AlertCondition,
    AlertScope,
    AlertSeverity,
    AlertTriggerStatus,
    NotificationChannel,
    NotificationStatus,
)
from app.models.windfarm import Windfarm
from app.models.portfolio import Portfolio
from app.schemas.alert import (
    AlertRuleCreate,
    AlertRuleUpdate,
    AlertTriggerStatusUpdate,
    NotificationPreferenceUpdate,
)


class AlertService:
    """Service for managing user alerts and notifications."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ========================================================================
    # ALERT RULE CRUD
    # ========================================================================

    async def create_alert_rule(self, user_id: int, data: AlertRuleCreate) -> AlertRule:
        """Create a new alert rule for a user."""
        rule = AlertRule(
            user_id=user_id,
            name=data.name,
            description=data.description,
            metric=AlertMetric(data.metric.value),
            condition=AlertCondition(data.condition.value),
            threshold_value=data.threshold_value,
            threshold_value_upper=data.threshold_value_upper,
            scope=AlertScope(data.scope.value),
            windfarm_id=data.windfarm_id,
            portfolio_id=data.portfolio_id,
            severity=AlertSeverity(data.severity.value),
            channels=[c.value for c in data.channels],
            sustained_minutes=data.sustained_minutes,
            is_enabled=data.is_enabled,
        )
        self.db.add(rule)
        await self.db.commit()
        await self.db.refresh(rule)
        return rule

    async def get_alert_rule(self, rule_id: int, user_id: int) -> Optional[AlertRule]:
        """Get an alert rule by ID for a specific user."""
        result = await self.db.execute(
            select(AlertRule)
            .options(joinedload(AlertRule.windfarm), joinedload(AlertRule.portfolio))
            .where(and_(AlertRule.id == rule_id, AlertRule.user_id == user_id))
        )
        return result.unique().scalar_one_or_none()

    async def list_alert_rules(
        self, user_id: int, is_enabled: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """List all alert rules for a user."""
        query = (
            select(
                AlertRule,
                func.count(AlertTrigger.id).filter(
                    AlertTrigger.status == AlertTriggerStatus.ACTIVE
                ).label("trigger_count"),
            )
            .outerjoin(AlertTrigger, AlertRule.id == AlertTrigger.rule_id)
            .options(joinedload(AlertRule.windfarm), joinedload(AlertRule.portfolio))
            .where(AlertRule.user_id == user_id)
            .group_by(AlertRule.id)
            .order_by(AlertRule.created_at.desc())
        )

        if is_enabled is not None:
            query = query.where(AlertRule.is_enabled == is_enabled)

        result = await self.db.execute(query)
        rules = []
        for row in result.unique().all():
            rule = row[0]
            trigger_count = row[1]
            rules.append(self._rule_to_dict(rule, trigger_count))

        return rules

    async def update_alert_rule(
        self, rule_id: int, user_id: int, data: AlertRuleUpdate
    ) -> Optional[AlertRule]:
        """Update an alert rule."""
        rule = await self.get_alert_rule(rule_id, user_id)
        if not rule:
            return None

        if data.name is not None:
            rule.name = data.name
        if data.description is not None:
            rule.description = data.description
        if data.metric is not None:
            rule.metric = AlertMetric(data.metric.value)
        if data.condition is not None:
            rule.condition = AlertCondition(data.condition.value)
        if data.threshold_value is not None:
            rule.threshold_value = data.threshold_value
        if data.threshold_value_upper is not None:
            rule.threshold_value_upper = data.threshold_value_upper
        if data.scope is not None:
            rule.scope = AlertScope(data.scope.value)
        if data.windfarm_id is not None:
            rule.windfarm_id = data.windfarm_id
        if data.portfolio_id is not None:
            rule.portfolio_id = data.portfolio_id
        if data.severity is not None:
            rule.severity = AlertSeverity(data.severity.value)
        if data.channels is not None:
            rule.channels = [c.value for c in data.channels]
        if data.sustained_minutes is not None:
            rule.sustained_minutes = data.sustained_minutes
        if data.is_enabled is not None:
            rule.is_enabled = data.is_enabled

        await self.db.commit()
        await self.db.refresh(rule)
        return rule

    async def delete_alert_rule(self, rule_id: int, user_id: int) -> bool:
        """Delete an alert rule."""
        rule = await self.get_alert_rule(rule_id, user_id)
        if not rule:
            return False

        await self.db.delete(rule)
        await self.db.commit()
        return True

    async def toggle_alert_rule(self, rule_id: int, user_id: int) -> Optional[AlertRule]:
        """Toggle an alert rule's enabled status."""
        rule = await self.get_alert_rule(rule_id, user_id)
        if not rule:
            return None

        rule.is_enabled = not rule.is_enabled
        await self.db.commit()
        await self.db.refresh(rule)
        return rule

    # ========================================================================
    # ALERT TRIGGERS
    # ========================================================================

    async def create_trigger(
        self,
        rule_id: int,
        windfarm_id: int,
        triggered_value: float,
        threshold_value: float,
        message: str,
    ) -> AlertTrigger:
        """Create an alert trigger when a rule condition is met."""
        trigger = AlertTrigger(
            rule_id=rule_id,
            windfarm_id=windfarm_id,
            triggered_value=triggered_value,
            threshold_value=threshold_value,
            message=message,
            status=AlertTriggerStatus.ACTIVE,
        )
        self.db.add(trigger)

        # Update rule's last triggered timestamp
        await self.db.execute(
            update(AlertRule)
            .where(AlertRule.id == rule_id)
            .values(last_triggered_at=datetime.utcnow())
        )

        await self.db.commit()
        await self.db.refresh(trigger)
        return trigger

    async def list_triggers(
        self,
        user_id: int,
        status: Optional[AlertTriggerStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List alert triggers for a user's rules."""
        base_query = (
            select(AlertTrigger)
            .join(AlertRule, AlertTrigger.rule_id == AlertRule.id)
            .options(
                joinedload(AlertTrigger.rule),
                joinedload(AlertTrigger.windfarm),
            )
            .where(AlertRule.user_id == user_id)
        )

        if status is not None:
            base_query = base_query.where(AlertTrigger.status == status)

        # Get total count
        count_result = await self.db.execute(
            select(func.count())
            .select_from(AlertTrigger)
            .join(AlertRule, AlertTrigger.rule_id == AlertRule.id)
            .where(AlertRule.user_id == user_id)
        )
        total = count_result.scalar() or 0

        # Get active and acknowledged counts
        status_counts = await self.db.execute(
            select(AlertTrigger.status, func.count())
            .join(AlertRule, AlertTrigger.rule_id == AlertRule.id)
            .where(AlertRule.user_id == user_id)
            .group_by(AlertTrigger.status)
        )
        counts = {row[0]: row[1] for row in status_counts.all()}

        # Get triggers
        result = await self.db.execute(
            base_query.order_by(AlertTrigger.triggered_at.desc())
            .limit(limit)
            .offset(offset)
        )

        triggers = []
        for trigger in result.unique().scalars().all():
            triggers.append(self._trigger_to_dict(trigger))

        return {
            "triggers": triggers,
            "total": total,
            "active_count": counts.get(AlertTriggerStatus.ACTIVE, 0),
            "acknowledged_count": counts.get(AlertTriggerStatus.ACKNOWLEDGED, 0),
        }

    async def update_trigger_status(
        self, trigger_id: int, user_id: int, data: AlertTriggerStatusUpdate
    ) -> Optional[AlertTrigger]:
        """Update an alert trigger's status."""
        result = await self.db.execute(
            select(AlertTrigger)
            .join(AlertRule, AlertTrigger.rule_id == AlertRule.id)
            .where(and_(AlertTrigger.id == trigger_id, AlertRule.user_id == user_id))
        )
        trigger = result.scalar_one_or_none()
        if not trigger:
            return None

        trigger.status = AlertTriggerStatus(data.status.value)
        if data.status.value == "acknowledged":
            trigger.acknowledged_at = datetime.utcnow()
        elif data.status.value == "resolved":
            trigger.resolved_at = datetime.utcnow()

        await self.db.commit()
        await self.db.refresh(trigger)
        return trigger

    async def acknowledge_trigger(self, trigger_id: int, user_id: int) -> Optional[AlertTrigger]:
        """Acknowledge an alert trigger."""
        return await self.update_trigger_status(
            trigger_id,
            user_id,
            AlertTriggerStatusUpdate(status=AlertTriggerStatus.ACKNOWLEDGED),
        )

    async def resolve_trigger(self, trigger_id: int, user_id: int) -> Optional[AlertTrigger]:
        """Resolve an alert trigger."""
        return await self.update_trigger_status(
            trigger_id,
            user_id,
            AlertTriggerStatusUpdate(status=AlertTriggerStatus.RESOLVED),
        )

    # ========================================================================
    # NOTIFICATIONS
    # ========================================================================

    async def create_notification(
        self,
        user_id: int,
        title: str,
        message: str,
        severity: AlertSeverity = AlertSeverity.MEDIUM,
        notification_type: str = "alert",
        trigger_id: Optional[int] = None,
        entity_type: Optional[str] = None,
        entity_id: Optional[int] = None,
        channel: NotificationChannel = NotificationChannel.IN_APP,
    ) -> Notification:
        """Create a notification for a user."""
        notification = Notification(
            user_id=user_id,
            trigger_id=trigger_id,
            title=title,
            message=message,
            severity=severity,
            notification_type=notification_type,
            entity_type=entity_type,
            entity_id=entity_id,
            channel=channel,
            status=NotificationStatus.UNREAD,
        )
        self.db.add(notification)
        await self.db.commit()
        await self.db.refresh(notification)
        return notification

    async def list_notifications(
        self,
        user_id: int,
        status: Optional[NotificationStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List notifications for a user."""
        base_query = select(Notification).where(Notification.user_id == user_id)

        if status is not None:
            base_query = base_query.where(Notification.status == status)

        # Get total count
        count_result = await self.db.execute(
            select(func.count()).select_from(Notification).where(Notification.user_id == user_id)
        )
        total = count_result.scalar() or 0

        # Get unread count
        unread_result = await self.db.execute(
            select(func.count())
            .select_from(Notification)
            .where(
                and_(
                    Notification.user_id == user_id,
                    Notification.status == NotificationStatus.UNREAD,
                )
            )
        )
        unread_count = unread_result.scalar() or 0

        # Get notifications
        result = await self.db.execute(
            base_query.order_by(Notification.created_at.desc()).limit(limit).offset(offset)
        )

        notifications = []
        for notification in result.scalars().all():
            notifications.append(self._notification_to_dict(notification))

        return {
            "notifications": notifications,
            "total": total,
            "unread_count": unread_count,
        }

    async def mark_notifications_read(self, user_id: int, notification_ids: List[int]) -> int:
        """Mark specific notifications as read."""
        result = await self.db.execute(
            update(Notification)
            .where(
                and_(
                    Notification.id.in_(notification_ids),
                    Notification.user_id == user_id,
                    Notification.status == NotificationStatus.UNREAD,
                )
            )
            .values(status=NotificationStatus.READ, read_at=datetime.utcnow())
        )
        await self.db.commit()
        return result.rowcount

    async def mark_all_notifications_read(self, user_id: int) -> int:
        """Mark all notifications as read for a user."""
        result = await self.db.execute(
            update(Notification)
            .where(
                and_(
                    Notification.user_id == user_id,
                    Notification.status == NotificationStatus.UNREAD,
                )
            )
            .values(status=NotificationStatus.READ, read_at=datetime.utcnow())
        )
        await self.db.commit()
        return result.rowcount

    async def delete_notification(self, notification_id: int, user_id: int) -> bool:
        """Delete a notification."""
        result = await self.db.execute(
            delete(Notification).where(
                and_(Notification.id == notification_id, Notification.user_id == user_id)
            )
        )
        await self.db.commit()
        return result.rowcount > 0

    async def get_unread_count(self, user_id: int) -> int:
        """Get count of unread notifications for a user."""
        result = await self.db.execute(
            select(func.count())
            .select_from(Notification)
            .where(
                and_(
                    Notification.user_id == user_id,
                    Notification.status == NotificationStatus.UNREAD,
                )
            )
        )
        return result.scalar() or 0

    # ========================================================================
    # NOTIFICATION PREFERENCES
    # ========================================================================

    async def get_notification_preferences(self, user_id: int) -> NotificationPreference:
        """Get notification preferences for a user, creating defaults if needed."""
        result = await self.db.execute(
            select(NotificationPreference).where(NotificationPreference.user_id == user_id)
        )
        prefs = result.scalar_one_or_none()

        if not prefs:
            # Create default preferences
            prefs = NotificationPreference(user_id=user_id)
            self.db.add(prefs)
            await self.db.commit()
            await self.db.refresh(prefs)

        return prefs

    async def update_notification_preferences(
        self, user_id: int, data: NotificationPreferenceUpdate
    ) -> NotificationPreference:
        """Update notification preferences for a user."""
        prefs = await self.get_notification_preferences(user_id)

        if data.email_enabled is not None:
            prefs.email_enabled = data.email_enabled
        if data.email_digest_enabled is not None:
            prefs.email_digest_enabled = data.email_digest_enabled
        if data.in_app_enabled is not None:
            prefs.in_app_enabled = data.in_app_enabled
        if data.digest_frequency_hours is not None:
            prefs.digest_frequency_hours = data.digest_frequency_hours
        if data.quiet_hours_enabled is not None:
            prefs.quiet_hours_enabled = data.quiet_hours_enabled
        if data.quiet_hours_start is not None:
            prefs.quiet_hours_start = data.quiet_hours_start
        if data.quiet_hours_end is not None:
            prefs.quiet_hours_end = data.quiet_hours_end
        if data.min_severity is not None:
            prefs.min_severity = AlertSeverity(data.min_severity.value)

        await self.db.commit()
        await self.db.refresh(prefs)
        return prefs

    # ========================================================================
    # SUMMARY & OVERVIEW
    # ========================================================================

    async def get_alerts_summary(self, user_id: int) -> Dict[str, Any]:
        """Get summary of alerts for dashboard."""
        # Get rule counts
        rule_counts = await self.db.execute(
            select(
                func.count().label("total"),
                func.count().filter(AlertRule.is_enabled == True).label("active"),
            )
            .select_from(AlertRule)
            .where(AlertRule.user_id == user_id)
        )
        row = rule_counts.one()
        total_rules = row[0]
        active_rules = row[1]

        # Get trigger counts
        trigger_counts = await self.db.execute(
            select(AlertTrigger.status, func.count())
            .join(AlertRule, AlertTrigger.rule_id == AlertRule.id)
            .where(AlertRule.user_id == user_id)
            .group_by(AlertTrigger.status)
        )
        t_counts = {row[0]: row[1] for row in trigger_counts.all()}

        # Get unread notification count
        unread = await self.get_unread_count(user_id)

        # Get recent triggers
        recent = await self.list_triggers(user_id, limit=5)

        return {
            "total_rules": total_rules,
            "active_rules": active_rules,
            "active_triggers": t_counts.get(AlertTriggerStatus.ACTIVE, 0),
            "acknowledged_triggers": t_counts.get(AlertTriggerStatus.ACKNOWLEDGED, 0),
            "unread_notifications": unread,
            "recent_triggers": recent["triggers"],
        }

    async def get_alerts_overview(self, user_id: int) -> Dict[str, Any]:
        """Get quick overview of alerts status."""
        # Get active trigger counts by severity
        severity_counts = await self.db.execute(
            select(AlertRule.severity, func.count())
            .join(AlertTrigger, AlertRule.id == AlertTrigger.rule_id)
            .where(
                and_(
                    AlertRule.user_id == user_id,
                    AlertTrigger.status == AlertTriggerStatus.ACTIVE,
                )
            )
            .group_by(AlertRule.severity)
        )
        s_counts = {row[0]: row[1] for row in severity_counts.all()}

        active_count = sum(s_counts.values())
        unread = await self.get_unread_count(user_id)

        return {
            "has_active_alerts": active_count > 0,
            "active_count": active_count,
            "critical_count": s_counts.get(AlertSeverity.CRITICAL, 0),
            "high_count": s_counts.get(AlertSeverity.HIGH, 0),
            "unread_notifications": unread,
        }

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def _rule_to_dict(self, rule: AlertRule, trigger_count: int = 0) -> Dict[str, Any]:
        """Convert rule model to dict."""
        return {
            "id": rule.id,
            "user_id": rule.user_id,
            "name": rule.name,
            "description": rule.description,
            "metric": rule.metric.value,
            "condition": rule.condition.value,
            "threshold_value": rule.threshold_value,
            "threshold_value_upper": rule.threshold_value_upper,
            "scope": rule.scope.value,
            "windfarm_id": rule.windfarm_id,
            "portfolio_id": rule.portfolio_id,
            "severity": rule.severity.value,
            "channels": rule.channels,
            "sustained_minutes": rule.sustained_minutes,
            "is_enabled": rule.is_enabled,
            "created_at": rule.created_at,
            "updated_at": rule.updated_at,
            "last_triggered_at": rule.last_triggered_at,
            "trigger_count": trigger_count,
            "windfarm": (
                {"id": rule.windfarm.id, "name": rule.windfarm.name}
                if rule.windfarm
                else None
            ),
            "portfolio": (
                {"id": rule.portfolio.id, "name": rule.portfolio.name}
                if rule.portfolio
                else None
            ),
        }

    def _trigger_to_dict(self, trigger: AlertTrigger) -> Dict[str, Any]:
        """Convert trigger model to dict."""
        return {
            "id": trigger.id,
            "rule_id": trigger.rule_id,
            "windfarm_id": trigger.windfarm_id,
            "triggered_value": trigger.triggered_value,
            "threshold_value": trigger.threshold_value,
            "message": trigger.message,
            "status": trigger.status.value,
            "triggered_at": trigger.triggered_at,
            "acknowledged_at": trigger.acknowledged_at,
            "resolved_at": trigger.resolved_at,
            "rule_name": trigger.rule.name if trigger.rule else "",
            "windfarm_name": trigger.windfarm.name if trigger.windfarm else "",
            "severity": trigger.rule.severity.value if trigger.rule else "medium",
        }

    def _notification_to_dict(self, notification: Notification) -> Dict[str, Any]:
        """Convert notification model to dict."""
        return {
            "id": notification.id,
            "user_id": notification.user_id,
            "trigger_id": notification.trigger_id,
            "title": notification.title,
            "message": notification.message,
            "severity": notification.severity.value,
            "notification_type": notification.notification_type,
            "entity_type": notification.entity_type,
            "entity_id": notification.entity_id,
            "channel": notification.channel.value,
            "status": notification.status.value,
            "created_at": notification.created_at,
            "read_at": notification.read_at,
        }
