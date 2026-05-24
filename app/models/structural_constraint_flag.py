"""Structural-constraint flag — Module 1b.

One row per detected constrained-output run on a windfarm. Generated
automatically by the detector and reviewed manually by analysts before
downstream modules (2/3/5) treat the flagged hours as constrained.

See `app/services/structural_constraint_detection_service.py` for the
detection logic and `tests/reference/p-1-validation-notes.md` for the
B1.5 amendment that adds the Q50-ratio detector path.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class StructuralConstraintFlag(Base):
    """One detected constraint period on a windfarm awaiting analyst review."""

    __tablename__ = "structural_constraint_flags"

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

    # Diagnostics
    wind_bins_affected: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mean_q90_ratio: Mapped[Optional[float]] = mapped_column(Numeric(6, 3), nullable=True)
    mean_q50_ratio: Mapped[Optional[float]] = mapped_column(Numeric(6, 3), nullable=True)

    # 'q90_ratio' | 'q50_ratio' | 'both' — which detector path fired (B1.5).
    flag_trigger: Mapped[str] = mapped_column(String(20), nullable=False, default="q90_ratio")
    flag_source: Mapped[str] = mapped_column(
        String(40), nullable=False, default="auto_constraint_detector"
    )

    # Review workflow
    review_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending_review")
    analyst_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reviewed_by: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

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
            name="uq_scf_windfarm_period",
        ),
        Index("ix_scf_status", "review_status"),
        Index("ix_scf_windfarm", "windfarm_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<StructuralConstraintFlag(wf={self.windfarm_id}, "
            f"{self.period_start.date()}→{self.period_end.date()}, "
            f"{self.duration_hours}h, trigger={self.flag_trigger}, "
            f"status={self.review_status})>"
        )
