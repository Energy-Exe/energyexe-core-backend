"""Alert schemas for API serialization."""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum


# ============================================================================
# ENUMS
# ============================================================================

class AlertMetricEnum(str, Enum):
    """Metric types that can trigger alerts."""
    CAPACITY_FACTOR = "capacity_factor"
    GENERATION = "generation"
    PRICE = "price"
    CAPTURE_RATE = "capture_rate"
    WIND_SPEED = "wind_speed"
    DATA_QUALITY = "data_quality"


class AlertConditionEnum(str, Enum):
    """Condition types for alert triggers."""
    ABOVE = "above"
    BELOW = "below"
    CHANGE_BY_PERCENT = "change_by_percent"
    OUTSIDE_RANGE = "outside_range"


class AlertScopeEnum(str, Enum):
    """Scope of alert monitoring."""
    SPECIFIC_WINDFARM = "specific_windfarm"
    PORTFOLIO = "portfolio"
    ALL_WINDFARMS = "all_windfarms"


class AlertSeverityEnum(str, Enum):
    """Severity level of alert."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class NotificationChannelEnum(str, Enum):
    """Notification delivery channels."""
    IN_APP = "in_app"
    EMAIL = "email"
    EMAIL_DIGEST = "email_digest"


class AlertTriggerStatusEnum(str, Enum):
    """Status of an alert trigger."""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class NotificationStatusEnum(str, Enum):
    """Status of a notification."""
    UNREAD = "unread"
    READ = "read"
    ARCHIVED = "archived"


# ============================================================================
# ALERT RULE SCHEMAS
# ============================================================================

class AlertRuleBase(BaseModel):
    """Base alert rule schema."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    metric: AlertMetricEnum
    condition: AlertConditionEnum
    threshold_value: float
    threshold_value_upper: Optional[float] = None
    scope: AlertScopeEnum = AlertScopeEnum.ALL_WINDFARMS
    windfarm_id: Optional[int] = None
    portfolio_id: Optional[int] = None
    severity: AlertSeverityEnum = AlertSeverityEnum.MEDIUM
    channels: List[NotificationChannelEnum] = [NotificationChannelEnum.IN_APP]
    sustained_minutes: int = Field(default=0, ge=0)
    is_enabled: bool = True


class AlertRuleCreate(AlertRuleBase):
    """Schema for creating an alert rule."""
    pass


class AlertRuleUpdate(BaseModel):
    """Schema for updating an alert rule."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    metric: Optional[AlertMetricEnum] = None
    condition: Optional[AlertConditionEnum] = None
    threshold_value: Optional[float] = None
    threshold_value_upper: Optional[float] = None
    scope: Optional[AlertScopeEnum] = None
    windfarm_id: Optional[int] = None
    portfolio_id: Optional[int] = None
    severity: Optional[AlertSeverityEnum] = None
    channels: Optional[List[NotificationChannelEnum]] = None
    sustained_minutes: Optional[int] = Field(None, ge=0)
    is_enabled: Optional[bool] = None


class WindfarmBrief(BaseModel):
    """Brief windfarm info for alert responses."""
    id: int
    name: str

    class Config:
        from_attributes = True


class PortfolioBrief(BaseModel):
    """Brief portfolio info for alert responses."""
    id: int
    name: str

    class Config:
        from_attributes = True


class AlertRuleResponse(BaseModel):
    """Schema for alert rule response."""
    id: int
    user_id: int
    name: str
    description: Optional[str] = None
    metric: AlertMetricEnum
    condition: AlertConditionEnum
    threshold_value: float
    threshold_value_upper: Optional[float] = None
    scope: AlertScopeEnum
    windfarm_id: Optional[int] = None
    portfolio_id: Optional[int] = None
    severity: AlertSeverityEnum
    channels: List[str]
    sustained_minutes: int
    is_enabled: bool
    created_at: datetime
    updated_at: datetime
    last_triggered_at: Optional[datetime] = None
    trigger_count: int = 0
    windfarm: Optional[WindfarmBrief] = None
    portfolio: Optional[PortfolioBrief] = None

    class Config:
        from_attributes = True


class AlertRuleListResponse(BaseModel):
    """Schema for listing alert rules."""
    rules: List[AlertRuleResponse]
    total: int


# ============================================================================
# ALERT TRIGGER SCHEMAS
# ============================================================================

class AlertTriggerResponse(BaseModel):
    """Schema for alert trigger response."""
    id: int
    rule_id: int
    windfarm_id: int
    triggered_value: float
    threshold_value: float
    message: str
    status: AlertTriggerStatusEnum
    triggered_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    rule_name: str = ""
    windfarm_name: str = ""
    severity: AlertSeverityEnum = AlertSeverityEnum.MEDIUM

    class Config:
        from_attributes = True


class AlertTriggerListResponse(BaseModel):
    """Schema for listing alert triggers."""
    triggers: List[AlertTriggerResponse]
    total: int
    active_count: int
    acknowledged_count: int


class AlertTriggerStatusUpdate(BaseModel):
    """Schema for updating alert trigger status."""
    status: AlertTriggerStatusEnum


# ============================================================================
# NOTIFICATION SCHEMAS
# ============================================================================

class NotificationResponse(BaseModel):
    """Schema for notification response."""
    id: int
    user_id: int
    trigger_id: Optional[int] = None
    title: str
    message: str
    severity: AlertSeverityEnum
    notification_type: str
    entity_type: Optional[str] = None
    entity_id: Optional[int] = None
    channel: NotificationChannelEnum
    status: NotificationStatusEnum
    created_at: datetime
    read_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class NotificationListResponse(BaseModel):
    """Schema for listing notifications."""
    notifications: List[NotificationResponse]
    total: int
    unread_count: int


class NotificationMarkRead(BaseModel):
    """Schema for marking notifications as read."""
    notification_ids: List[int]


class NotificationMarkAllRead(BaseModel):
    """Schema for marking all notifications as read."""
    pass


# ============================================================================
# NOTIFICATION PREFERENCE SCHEMAS
# ============================================================================

class NotificationPreferenceBase(BaseModel):
    """Base notification preference schema."""
    email_enabled: bool = True
    email_digest_enabled: bool = True
    in_app_enabled: bool = True
    digest_frequency_hours: int = Field(default=24, ge=1, le=168)
    quiet_hours_enabled: bool = False
    quiet_hours_start: Optional[int] = Field(None, ge=0, le=23)
    quiet_hours_end: Optional[int] = Field(None, ge=0, le=23)
    min_severity: AlertSeverityEnum = AlertSeverityEnum.LOW


class NotificationPreferenceUpdate(BaseModel):
    """Schema for updating notification preferences."""
    email_enabled: Optional[bool] = None
    email_digest_enabled: Optional[bool] = None
    in_app_enabled: Optional[bool] = None
    digest_frequency_hours: Optional[int] = Field(None, ge=1, le=168)
    quiet_hours_enabled: Optional[bool] = None
    quiet_hours_start: Optional[int] = Field(None, ge=0, le=23)
    quiet_hours_end: Optional[int] = Field(None, ge=0, le=23)
    min_severity: Optional[AlertSeverityEnum] = None


class NotificationPreferenceResponse(NotificationPreferenceBase):
    """Schema for notification preference response."""
    id: int
    user_id: int
    last_digest_sent_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# ALERT SUMMARY SCHEMAS
# ============================================================================

class AlertsSummary(BaseModel):
    """Summary of alerts for dashboard."""
    total_rules: int
    active_rules: int
    active_triggers: int
    acknowledged_triggers: int
    unread_notifications: int
    recent_triggers: List[AlertTriggerResponse]


class AlertsOverview(BaseModel):
    """Overview of alerts for quick status."""
    has_active_alerts: bool
    active_count: int
    critical_count: int
    high_count: int
    unread_notifications: int
