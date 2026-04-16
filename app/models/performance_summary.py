"""Monthly/yearly aggregated performance metrics — ODI, normalisation, commercial."""

from datetime import datetime, timezone, date
from typing import Optional

from sqlalchemy import (
    Date,
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


class PerformanceSummary(Base):
    """One row per windfarm per period (month or year)."""

    __tablename__ = "performance_summaries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    period_type: Mapped[str] = mapped_column(String(10), nullable=False)  # month, year
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # NULL for yearly

    # ── Module 3: ODI metrics ──
    total_hours: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    underperf_hours: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    overperf_hours: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    odi_pct_underperf: Mapped[Optional[float]] = mapped_column(Numeric(6, 3), nullable=True)
    lost_mwh: Mapped[Optional[float]] = mapped_column(Numeric(12, 3), nullable=True)
    expected_mwh: Mapped[Optional[float]] = mapped_column(Numeric(12, 3), nullable=True)
    odi_pct_loss_mwh: Mapped[Optional[float]] = mapped_column(Numeric(6, 3), nullable=True)
    lost_eur: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)
    expected_revenue_eur: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)
    odi_pct_loss_eur: Mapped[Optional[float]] = mapped_column(Numeric(6, 3), nullable=True)
    long_run_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    max_run_hours: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # ── Module 4: Wind normalisation ──
    norm_ratio_p50: Mapped[Optional[float]] = mapped_column(Numeric(8, 5), nullable=True)
    norm_index_p50: Mapped[Optional[float]] = mapped_column(Numeric(8, 3), nullable=True)
    norm_ratio_p10: Mapped[Optional[float]] = mapped_column(Numeric(8, 5), nullable=True)
    norm_index_p10: Mapped[Optional[float]] = mapped_column(Numeric(8, 3), nullable=True)

    # ── Module 6: Commercial ──
    constraint_proxy_mwh: Mapped[Optional[float]] = mapped_column(Numeric(12, 3), nullable=True)
    lost_value_eur: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)

    # ── Metadata ──
    pipeline_run_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("import_job_executions.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    windfarm = relationship("Windfarm")
    pipeline_run = relationship("ImportJobExecution")

    __table_args__ = (
        UniqueConstraint("windfarm_id", "period_type", "year", "month", name="uq_perf_summary_wf_period"),
        Index("ix_perf_summary_wf_year", "windfarm_id", "year"),
    )

    def __repr__(self) -> str:
        period = f"{self.year}-{self.month:02d}" if self.month else str(self.year)
        return f"<PerformanceSummary(wf={self.windfarm_id}, period={period})>"
