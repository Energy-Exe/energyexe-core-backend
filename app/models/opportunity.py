"""Opportunity model for automated wind farm asset analysis findings."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class SchemaCode(str, Enum):
    """The 6 opportunity detection schemas."""
    OPS_01 = "OPS_01"  # Volatile disruption periods
    OPS_02 = "OPS_02"  # Performance seasonality
    OPS_03 = "OPS_03"  # Misaligned contracting strategies
    MKT_01 = "MKT_01"  # Low capture rates — contracting
    MKT_02 = "MKT_02"  # Low capture rates — storage
    MKT_03 = "MKT_03"  # High cannibalisation rates


class Severity(str, Enum):
    """Opportunity severity tiers."""
    CONFIRMED = "CONFIRMED"
    INDICATIVE = "INDICATIVE"
    WATCH = "WATCH"


class Branch(str, Enum):
    """Root cause branch."""
    A = "A"
    B = "B"
    C = "C"


class OpportunityStatus(str, Enum):
    """Opportunity lifecycle status."""
    ACTIVE = "ACTIVE"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"
    SUPERSEDED = "SUPERSEDED"


class Opportunity(Base):
    """Detected opportunity for a wind farm asset."""

    __tablename__ = "opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    schema_code: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(15), nullable=False)
    branch: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    status: Mapped[str] = mapped_column(
        String(15), default=OpportunityStatus.ACTIVE, nullable=False, index=True
    )

    # Computed data and gaps
    data_slots: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    missing_slots: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    # Cross-schema dependency
    triggered_by_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("opportunities.id", ondelete="SET NULL"), nullable=True
    )

    # Detection context
    detection_period_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    detection_period_end: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    detection_run_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("import_job_executions.id", ondelete="SET NULL"), nullable=True
    )
    suppression_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    windfarm = relationship("Windfarm")
    triggered_by = relationship("Opportunity", remote_side="Opportunity.id")
    detection_run = relationship("ImportJobExecution")

    __table_args__ = (
        Index(
            "ix_opportunities_active_unique",
            "windfarm_id",
            "schema_code",
            unique=True,
            postgresql_where=(status == OpportunityStatus.ACTIVE),
        ),
        Index("ix_opportunities_windfarm_schema", "windfarm_id", "schema_code"),
    )

    def __repr__(self) -> str:
        return f"<Opportunity(id={self.id}, windfarm_id={self.windfarm_id}, schema={self.schema_code}, severity={self.severity})>"
