"""Degradation analysis OLS regression results."""

from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import Date, DateTime, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class DegradationResult(Base):
    """OLS regression result for performance degradation trend per windfarm."""

    __tablename__ = "degradation_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    reference_curve: Mapped[str] = mapped_column(
        String(5), nullable=False
    )  # q50 (P50) or q90 (P10)

    analysis_start: Mapped[date] = mapped_column(Date, nullable=False)
    analysis_end: Mapped[date] = mapped_column(Date, nullable=False)
    data_points: Mapped[int] = mapped_column(Integer, nullable=False)

    # OLS regression results
    slope_pu_per_year: Mapped[Optional[float]] = mapped_column(Numeric(12, 8), nullable=True)
    slope_pct_per_year: Mapped[Optional[float]] = mapped_column(Numeric(10, 3), nullable=True)
    intercept: Mapped[Optional[float]] = mapped_column(Numeric(14, 6), nullable=True)
    r_squared: Mapped[Optional[float]] = mapped_column(Numeric(6, 5), nullable=True)
    p_value: Mapped[Optional[float]] = mapped_column(Numeric(8, 6), nullable=True)
    ci_lower_95: Mapped[Optional[float]] = mapped_column(Numeric(12, 8), nullable=True)
    ci_upper_95: Mapped[Optional[float]] = mapped_column(Numeric(12, 8), nullable=True)
    ci_lower_95_pct: Mapped[Optional[float]] = mapped_column(Numeric(10, 3), nullable=True)
    ci_upper_95_pct: Mapped[Optional[float]] = mapped_column(Numeric(10, 3), nullable=True)
    baseline_cap_pu: Mapped[Optional[float]] = mapped_column(Numeric(6, 5), nullable=True)
    n_constraint_hours_excluded: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Metadata
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
        UniqueConstraint(
            "windfarm_id", "reference_curve", "pipeline_run_id", name="uq_degradation_wf_ref_run"
        ),
        Index("ix_degradation_wf", "windfarm_id"),
    )

    def __repr__(self) -> str:
        return f"<DegradationResult(wf={self.windfarm_id}, ref={self.reference_curve}, slope={self.slope_pct_per_year}%/yr)>"
