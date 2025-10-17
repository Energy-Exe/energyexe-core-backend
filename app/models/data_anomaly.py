"""Data anomaly detection model."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class DataAnomaly(Base):
    """Data anomaly model for tracking data quality issues."""

    __tablename__ = "data_anomalies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # Anomaly classification
    anomaly_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="medium")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)

    # Relationships
    windfarm_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("windfarms.id"), nullable=True, index=True
    )
    generation_unit_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("generation_units.id"), nullable=True
    )

    # Time period affected
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    # Additional details
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    anomaly_metadata: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Resolution tracking
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("users.id"), nullable=True)
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Audit fields
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Relationships
    windfarm = relationship("Windfarm", foreign_keys=[windfarm_id])
    generation_unit = relationship("GenerationUnit", foreign_keys=[generation_unit_id])
    resolver = relationship("User", foreign_keys=[resolved_by])

    __table_args__ = (
        Index('idx_anomaly_windfarm_status', 'windfarm_id', 'status'),
        Index('idx_anomaly_period', 'period_start', 'period_end'),
        Index('idx_anomaly_type_status', 'anomaly_type', 'status'),
    )

    def __repr__(self) -> str:
        """String representation of DataAnomaly."""
        return (
            f"<DataAnomaly(id={self.id}, type='{self.anomaly_type}', "
            f"status='{self.status}', windfarm_id={self.windfarm_id})>"
        )


# Anomaly type constants
class AnomalyType:
    """Constants for anomaly types."""
    CAPACITY_FACTOR_OVER_LIMIT = "capacity_factor_over_limit"
    NEGATIVE_GENERATION = "negative_generation"
    MISSING_DATA = "missing_data"
    DATA_SPIKE = "data_spike"
    DATA_GAP = "data_gap"
    INVALID_CAPACITY = "invalid_capacity"


# Anomaly status constants
class AnomalyStatus:
    """Constants for anomaly statuses."""
    PENDING = "pending"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"
    IGNORED = "ignored"
    FALSE_POSITIVE = "false_positive"


# Severity constants
class AnomalySeverity:
    """Constants for anomaly severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
