"""Models for data backfill tracking."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class BackfillJobStatus(str, Enum):
    """Status of a backfill job."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_COMPLETED = "partially_completed"


class BackfillTaskStatus(str, Enum):
    """Status of a backfill task."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class BackfillJob(Base):
    """Backfill job model for tracking data backfill requests."""

    __tablename__ = "backfill_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    
    # Windfarm and date range
    windfarm_id: Mapped[int] = mapped_column(Integer, ForeignKey("windfarms.id"), nullable=False)
    start_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    
    # Status tracking
    status: Mapped[str] = mapped_column(
        String(50), default=BackfillJobStatus.PENDING, nullable=False
    )
    total_tasks: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    completed_tasks: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_tasks: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    
    # Metadata
    created_by_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    job_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None), nullable=False
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    windfarm = relationship("Windfarm", back_populates="backfill_jobs")
    created_by = relationship("User", back_populates="backfill_jobs")
    tasks = relationship("BackfillTask", back_populates="job", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        try:
            return f"<BackfillJob(id={self.id}, windfarm_id={self.windfarm_id}, status={self.status})>"
        except Exception:
            # Handle detached instance case
            return f"<BackfillJob(detached)>"


class BackfillTask(Base):
    """Individual task within a backfill job."""

    __tablename__ = "backfill_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    
    # Job reference
    job_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("backfill_jobs.id", ondelete="CASCADE"), nullable=False
    )
    
    # Task details
    generation_unit_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("generation_units.id"), nullable=False
    )
    source: Mapped[str] = mapped_column(String(50), nullable=False)  # entsoe, elexon, eia, taipower
    start_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    
    # Status tracking
    status: Mapped[str] = mapped_column(
        String(50), default=BackfillTaskStatus.PENDING, nullable=False
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    
    # Results
    records_fetched: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    task_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    job = relationship("BackfillJob", back_populates="tasks")
    generation_unit = relationship("GenerationUnit", back_populates="backfill_tasks")

    def __repr__(self) -> str:
        try:
            return f"<BackfillTask(id={self.id}, job_id={self.job_id}, status={self.status})>"
        except Exception:
            # Handle detached instance case
            return f"<BackfillTask(detached)>"