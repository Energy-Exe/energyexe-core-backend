"""Models for weather data import job tracking."""

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


class WeatherImportStatus(str, Enum):
    """Status of a weather import job execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WeatherImportJob(Base):
    """Track execution of weather data import jobs."""

    __tablename__ = "weather_import_jobs"

    # Identity
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(50), default="ERA5", nullable=False)

    # Date range being imported (data dates, not execution time)
    import_start_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    import_end_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Execution tracking
    status: Mapped[str] = mapped_column(
        String(20), default=WeatherImportStatus.PENDING, nullable=False, index=True
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Results
    records_imported: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    files_downloaded: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    files_deleted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    api_calls_made: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Progress tracking (JSON metadata)
    job_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    # Example metadata structure:
    # {
    #   'total_dates': 10,
    #   'dates_completed': 3,
    #   'current_date': '2025-01-15',
    #   'current_phase': 'downloading' | 'processing' | 'storing',
    #   'records_processed': 114552,
    #   'last_update': '2025-01-15T10:30:00Z'
    # }

    # Error handling
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)

    # User tracking
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
        Index("ix_weather_jobs_status", "status"),
        Index("ix_weather_jobs_latest", "created_at"),
        Index("ix_weather_jobs_date_range", "import_start_date", "import_end_date"),
    )

    def mark_running(self):
        """Mark job as running."""
        self.status = WeatherImportStatus.RUNNING
        self.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        if self.job_metadata is None:
            self.job_metadata = {}
        self.job_metadata["last_update"] = datetime.now(timezone.utc).isoformat()

    def mark_success(
        self,
        records_imported: int = 0,
        files_downloaded: int = 0,
        files_deleted: int = 0,
        api_calls: int = 0,
    ):
        """Mark job as successful."""
        self.status = WeatherImportStatus.SUCCESS
        self.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.records_imported = records_imported
        self.files_downloaded = files_downloaded
        self.files_deleted = files_deleted
        self.api_calls_made = api_calls

        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()

        if self.job_metadata is None:
            self.job_metadata = {}
        self.job_metadata["last_update"] = datetime.now(timezone.utc).isoformat()
        self.job_metadata["current_phase"] = "completed"

    def mark_failed(self, error_message: str):
        """Mark job as failed."""
        self.status = WeatherImportStatus.FAILED
        self.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.error_message = error_message

        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()

        if self.job_metadata is None:
            self.job_metadata = {}
        self.job_metadata["last_update"] = datetime.now(timezone.utc).isoformat()
        self.job_metadata["current_phase"] = "failed"

    def mark_cancelled(self):
        """Mark job as cancelled."""
        self.status = WeatherImportStatus.CANCELLED
        self.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()

        if self.job_metadata is None:
            self.job_metadata = {}
        self.job_metadata["last_update"] = datetime.now(timezone.utc).isoformat()
        self.job_metadata["current_phase"] = "cancelled"

    def update_progress(
        self,
        dates_completed: Optional[int] = None,
        current_date: Optional[str] = None,
        current_phase: Optional[str] = None,
        records_processed: Optional[int] = None,
    ):
        """Update job progress metadata."""
        if self.job_metadata is None:
            self.job_metadata = {}

        if dates_completed is not None:
            self.job_metadata["dates_completed"] = dates_completed
        if current_date is not None:
            self.job_metadata["current_date"] = current_date
        if current_phase is not None:
            self.job_metadata["current_phase"] = current_phase
        if records_processed is not None:
            self.job_metadata["records_processed"] = records_processed

        self.job_metadata["last_update"] = datetime.now(timezone.utc).isoformat()

    def get_progress_percentage(self) -> float:
        """Calculate progress percentage based on dates completed."""
        if not self.job_metadata or "total_dates" not in self.job_metadata:
            return 0.0

        total = self.job_metadata.get("total_dates", 0)
        completed = self.job_metadata.get("dates_completed", 0)

        if total == 0:
            return 0.0

        return (completed / total) * 100

    def can_retry(self) -> bool:
        """Check if job can be retried."""
        return self.status == WeatherImportStatus.FAILED and self.retry_count < self.max_retries

    def __repr__(self) -> str:
        return f"<WeatherImportJob(id={self.id}, job_name={self.job_name}, status={self.status})>"
