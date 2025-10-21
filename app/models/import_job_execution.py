"""Models for scheduled data import job tracking."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    JSON,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class ImportJobStatus(str, Enum):
    """Status of an import job execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


class ImportJobType(str, Enum):
    """Type of import job."""
    SCHEDULED = "scheduled"  # Triggered by cron
    MANUAL = "manual"  # Triggered by user via UI


class ImportJobExecution(Base):
    """Track execution of data import jobs."""

    __tablename__ = "import_job_executions"

    # Identity
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    job_type: Mapped[str] = mapped_column(
        String(20), default=ImportJobType.SCHEDULED, nullable=False, index=True
    )

    # Period being imported (data date range, not execution time)
    import_start_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    import_end_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Execution tracking
    status: Mapped[str] = mapped_column(
        String(20), default=ImportJobStatus.PENDING, nullable=False, index=True
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Results
    records_imported: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    api_calls_made: Mapped[int] = mapped_column(Integer, default=0, nullable=True)

    # Error handling
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)

    # Metadata (source-specific details, command used, etc.)
    job_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # User tracking (null for scheduled jobs)
    created_by_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=True
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    # Relationships
    created_by = relationship("User", foreign_keys=[created_by_id])

    # Indexes for common queries
    __table_args__ = (
        Index("ix_import_jobs_source_status", "source", "status"),
        Index("ix_import_jobs_latest", "job_name", "started_at"),
        Index("ix_import_jobs_recent", "created_at"),
    )

    def mark_running(self):
        """Mark job as running."""
        self.status = ImportJobStatus.RUNNING
        self.started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    def mark_success(self, records_imported: int = 0, records_updated: int = 0, api_calls: int = 0):
        """Mark job as successful."""
        self.status = ImportJobStatus.SUCCESS
        self.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.records_imported = records_imported
        self.records_updated = records_updated
        self.api_calls_made = api_calls

        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()

    def mark_failed(self, error_message: str):
        """Mark job as failed."""
        self.status = ImportJobStatus.FAILED
        self.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.error_message = error_message

        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()

    def can_retry(self) -> bool:
        """Check if job can be retried."""
        return self.status == ImportJobStatus.FAILED and self.retry_count < self.max_retries

    def __repr__(self) -> str:
        return f"<ImportJobExecution(id={self.id}, job_name={self.job_name}, status={self.status})>"
