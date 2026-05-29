"""Constraint loss summary — Module 3f.

One row per confirmed structural-constraint period on a windfarm, recording
the infrastructure-driven energy/revenue loss for that period priced against
the pooled ``overall_clean`` Q50 capability curve.

The reference pipeline produces this as ``constraint_loss_summary.csv`` — it is
"the metric that replaces the under-reported ODI figure for cable-fault-type
events". The backend equivalent lives here (issue #82). Constrained hours are
excluded from the normal Module 3 ODI accounting (they are masked upstream), so
this table is where their loss is attributed.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class ConstraintLossSummary(Base):
    """Infrastructure-driven loss for one confirmed constraint period."""

    __tablename__ = "constraint_loss_summaries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    windfarm_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("windfarms.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration_hours: Mapped[int] = mapped_column(Integer, nullable=False)

    # Energy + revenue, priced against overall_clean Q50.
    actual_mwh: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)
    expected_mwh: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)
    lost_mwh: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)
    lost_eur: Mapped[Optional[float]] = mapped_column(Numeric(16, 2), nullable=True)

    # Carried from the Module 1b flag for context (~0.5 = single cable failure).
    mean_q90_ratio: Mapped[Optional[float]] = mapped_column(Numeric(6, 3), nullable=True)

    reference_curve: Mapped[str] = mapped_column(
        String(40), nullable=False, default="overall_clean_q50"
    )

    pipeline_run_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("import_job_executions.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    windfarm = relationship("Windfarm")
    pipeline_run = relationship("ImportJobExecution")

    __table_args__ = (
        UniqueConstraint(
            "windfarm_id",
            "period_start",
            "period_end",
            name="uq_cls_windfarm_period",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ConstraintLossSummary(wf={self.windfarm_id}, "
            f"{self.period_start.date()}→{self.period_end.date()}, "
            f"lost_mwh={self.lost_mwh})>"
        )
