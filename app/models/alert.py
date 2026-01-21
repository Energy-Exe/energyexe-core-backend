"""Alert models for user-configurable monitoring and notifications."""

from datetime import datetime
from typing import Optional, List

from sqlalchemy import Boolean, DateTime, Enum, Float, ForeignKey, Integer, String, Text, UniqueConstraint, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
import enum

from app.core.database import Base


class AlertMetric(str, enum.Enum):
    """Metric types that can trigger alerts."""
    CAPACITY_FACTOR = "capacity_factor"
    GENERATION = "generation"
    PRICE = "price"
    CAPTURE_RATE = "capture_rate"
    WIND_SPEED = "wind_speed"
    DATA_QUALITY = "data_quality"


class AlertCondition(str, enum.Enum):
    """Condition types for alert triggers."""
    ABOVE = "above"
    BELOW = "below"
    CHANGE_BY_PERCENT = "change_by_percent"
    OUTSIDE_RANGE = "outside_range"


class AlertScope(str, enum.Enum):
    """Scope of alert monitoring."""
    SPECIFIC_WINDFARM = "specific_windfarm"
    PORTFOLIO = "portfolio"
    ALL_WINDFARMS = "all_windfarms"


class AlertSeverity(str, enum.Enum):
    """Severity level of alert."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class NotificationChannel(str, enum.Enum):
    """Notification delivery channels."""
    IN_APP = "in_app"
    EMAIL = "email"
    EMAIL_DIGEST = "email_digest"


class AlertRule(Base):
    """Alert rule model for defining monitoring conditions."""

    __tablename__ = "alert_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # What to monitor
    metric: Mapped[AlertMetric] = mapped_column(Enum(AlertMetric), nullable=False)
    condition: Mapped[AlertCondition] = mapped_column(Enum(AlertCondition), nullable=False)
    threshold_value: Mapped[float] = mapped_column(Float, nullable=False)
    threshold_value_upper: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # For range conditions

    # Scope
    scope: Mapped[AlertScope] = mapped_column(Enum(AlertScope), default=AlertScope.ALL_WINDFARMS, nullable=False)
    windfarm_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=True, index=True
    )
    portfolio_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=True, index=True
    )

    # Severity and notification
    severity: Mapped[AlertSeverity] = mapped_column(
        Enum(AlertSeverity), default=AlertSeverity.MEDIUM, nullable=False
    )
    channels: Mapped[dict] = mapped_column(JSON, default=lambda: ["in_app"], nullable=False)

    # Duration threshold (alerts only if condition sustained for X minutes)
    sustained_minutes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Status
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    last_triggered_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    user = relationship("User", back_populates="alert_rules")
    windfarm = relationship("Windfarm")
    portfolio = relationship("Portfolio")
    triggers = relationship("AlertTrigger", back_populates="rule", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<AlertRule(id={self.id}, name='{self.name}', metric={self.metric}, user_id={self.user_id})>"


class AlertTriggerStatus(str, enum.Enum):
    """Status of an alert trigger."""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class AlertTrigger(Base):
    """Alert trigger records when an alert rule condition is met."""

    __tablename__ = "alert_triggers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    rule_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("alert_rules.id", ondelete="CASCADE"), nullable=False, index=True
    )
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Trigger details
    triggered_value: Mapped[float] = mapped_column(Float, nullable=False)
    threshold_value: Mapped[float] = mapped_column(Float, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    # Status
    status: Mapped[AlertTriggerStatus] = mapped_column(
        Enum(AlertTriggerStatus), default=AlertTriggerStatus.ACTIVE, nullable=False
    )

    # Timestamps
    triggered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    rule = relationship("AlertRule", back_populates="triggers")
    windfarm = relationship("Windfarm")
    notifications = relationship("Notification", back_populates="trigger", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<AlertTrigger(id={self.id}, rule_id={self.rule_id}, status={self.status})>"


class NotificationStatus(str, enum.Enum):
    """Status of a notification."""
    UNREAD = "unread"
    READ = "read"
    ARCHIVED = "archived"


class Notification(Base):
    """Notification model for user notifications."""

    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    # Optional link to alert trigger
    trigger_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("alert_triggers.id", ondelete="SET NULL"), nullable=True, index=True
    )

    # Notification content
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[AlertSeverity] = mapped_column(
        Enum(AlertSeverity), default=AlertSeverity.MEDIUM, nullable=False
    )

    # Notification type (for categorization)
    notification_type: Mapped[str] = mapped_column(String(50), default="alert", nullable=False)

    # Link to relevant entity
    entity_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # e.g., "windfarm", "portfolio"
    entity_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Channel used
    channel: Mapped[NotificationChannel] = mapped_column(
        Enum(NotificationChannel), default=NotificationChannel.IN_APP, nullable=False
    )

    # Status
    status: Mapped[NotificationStatus] = mapped_column(
        Enum(NotificationStatus), default=NotificationStatus.UNREAD, nullable=False
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    read_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    user = relationship("User", back_populates="notifications")
    trigger = relationship("AlertTrigger", back_populates="notifications")

    def __repr__(self) -> str:
        return f"<Notification(id={self.id}, user_id={self.user_id}, status={self.status})>"


class NotificationPreference(Base):
    """User notification preferences."""

    __tablename__ = "notification_preferences"
    __table_args__ = (
        UniqueConstraint("user_id", name="uq_user_notification_preferences"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    # Channel preferences
    email_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    email_digest_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    in_app_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Digest frequency (hours between digest emails)
    digest_frequency_hours: Mapped[int] = mapped_column(Integer, default=24, nullable=False)
    last_digest_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Quiet hours (UTC)
    quiet_hours_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    quiet_hours_start: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Hour 0-23
    quiet_hours_end: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Hour 0-23

    # Severity filter (only receive notifications >= this severity)
    min_severity: Mapped[AlertSeverity] = mapped_column(
        Enum(AlertSeverity), default=AlertSeverity.LOW, nullable=False
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Relationships
    user = relationship("User", back_populates="notification_preferences")

    def __repr__(self) -> str:
        return f"<NotificationPreference(user_id={self.user_id})>"
