"""Hourly performance anomaly flags with loss quantification."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class PerformanceAnomaly(Base):
    """One row per flagged hour (underperformance or overperformance only)."""

    __tablename__ = "performance_anomalies"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    anomaly_type: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # underperformance, overperformance

    # Actual vs expected
    actual_p_pu: Mapped[Optional[float]] = mapped_column(Numeric(6, 5), nullable=True)
    expected_p_pu: Mapped[Optional[float]] = mapped_column(Numeric(6, 5), nullable=True)  # q50 from capability
    wind_speed: Mapped[Optional[float]] = mapped_column(Numeric(5, 2), nullable=True)
    wind_bin: Mapped[Optional[float]] = mapped_column(Numeric(4, 1), nullable=True)

    # Loss quantification
    lost_mwh: Mapped[Optional[float]] = mapped_column(Numeric(10, 3), nullable=True)
    lost_eur: Mapped[Optional[float]] = mapped_column(Numeric(12, 2), nullable=True)
    market_price: Mapped[Optional[float]] = mapped_column(Numeric(12, 4), nullable=True)

    # Consecutive run grouping
    run_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Module 3b — IsolationForest secondary flag (spec item 5.2). Informational
    # only; does NOT contribute to lost_mwh / lost_eur. NULL = not evaluated.
    flag_isolation_forest: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), nullable=False
    )

    windfarm = relationship("Windfarm")

    __table_args__ = (
        UniqueConstraint("windfarm_id", "hour", name="uq_perf_anomaly_wf_hour"),
        Index("ix_perf_anomaly_wf_type", "windfarm_id", "anomaly_type"),
        Index("ix_perf_anomaly_wf_hour", "windfarm_id", "hour"),
        Index("ix_perf_anomaly_run", "windfarm_id", "run_id"),
    )

    def __repr__(self) -> str:
        return f"<PerformanceAnomaly(wf={self.windfarm_id}, hour={self.hour}, type={self.anomaly_type})>"
