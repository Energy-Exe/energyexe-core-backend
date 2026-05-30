"""Opportunity model for automated wind farm asset analysis findings."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class SchemaCode(str, Enum):
    """The 19 opportunity detection schemas across 4 domains.

    Domains: Operational (OPS), Market (MKT), Financial (FIN), Data Quality (DQ).
    Originally 6 (OPS_01..03, MKT_01..03); expanded to the full spec set in the
    6 -> 18 initiative (the "18" is the spec's branding shorthand; the actual
    member set across OPS_01..08, MKT_01..07, FIN_01..03, DQ_01 is 19).
    """

    # Operational
    OPS_01 = "OPS_01"  # Volatile disruption periods
    OPS_02 = "OPS_02"  # Performance seasonality
    OPS_03 = "OPS_03"  # Misaligned contracting strategies
    OPS_04 = "OPS_04"  # Turbine degradation
    OPS_05 = "OPS_05"  # Grid curtailment
    OPS_06 = "OPS_06"  # Persistent underperformance
    OPS_07 = "OPS_07"  # Fleet age risk
    OPS_08 = "OPS_08"  # Structural constraint
    # Market
    MKT_01 = "MKT_01"  # Low capture rates — contracting
    MKT_02 = "MKT_02"  # Low capture rates — storage
    MKT_03 = "MKT_03"  # High cannibalisation rates
    MKT_04 = "MKT_04"  # PPA expiry
    MKT_05 = "MKT_05"  # PPA underpricing (INACTIVE — no PPA prices)
    MKT_06 = "MKT_06"  # Negative-price hours
    MKT_07 = "MKT_07"  # Forecast deviation (INACTIVE — no forecast data)
    # Financial
    FIN_01 = "FIN_01"  # P50 attainment
    FIN_02 = "FIN_02"  # Onshore OPEX overrun
    FIN_03 = "FIN_03"  # Offshore OPEX overrun
    # Data Quality
    DQ_01 = "DQ_01"  # Data gaps gate


class Severity(str, Enum):
    """Opportunity severity tiers."""

    CONFIRMED = "CONFIRMED"
    INDICATIVE = "INDICATIVE"
    WATCH = "WATCH"
    SUPPRESSED = "SUPPRESSED"  # Gated off by DQ-01 data-gap detection


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
    INACTIVE = "INACTIVE"  # Schema is data-blocked (e.g. MKT-05, MKT-07)


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
