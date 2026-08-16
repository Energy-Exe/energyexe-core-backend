"""Report and ReportSection models — the generated-reports store (EPR-81/82).

A *report* is one generation run of a report type (opportunity, periodic_digest,
board_pack) against a windfarm or portfolio for a date window. Its content lives
in *report_sections*, one row per section, so Pass-1 sections generating
concurrently can each commit their own status without racing on a shared JSONB
column. The join of a report + its sections is the single render source for
both the in-platform view and the PDF.

Versioning is retain-on-export: regenerating creates version N+1 and supersedes
version N; superseded versions that were never exported (pdf_downloaded_at IS
NULL and not locked) are pruned, while any version whose PDF left the platform
is kept byte-for-byte forever.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class ReportType(str, Enum):
    """Registered report types (must match REPORT_TYPE_REGISTRY keys)."""

    OPPORTUNITY = "opportunity"
    PERIODIC_DIGEST = "periodic_digest"
    BOARD_PACK = "board_pack"


class ReportScope(str, Enum):
    WINDFARM = "windfarm"
    PORTFOLIO = "portfolio"


class ReportStatus(str, Enum):
    PENDING = "PENDING"  # created, generation task not started yet
    GENERATING = "GENERATING"  # orchestrator running
    COMPLETE = "COMPLETE"  # all runnable sections generated
    PARTIAL = "PARTIAL"  # finished, but at least one section failed
    FAILED = "FAILED"  # orchestrator-level failure (nothing usable)
    SUPERSEDED = "SUPERSEDED"  # replaced by a newer version (retained: exported/locked)


class SectionStatus(str, Enum):
    UNGENERATED = "UNGENERATED"  # nothing produced yet (AI sections start here)
    GENERATING = "GENERATING"
    GENERATED = "GENERATED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"  # not runnable for this report (e.g. missing inputs)
    STALE = "STALE"  # generated, but an upstream Pass-1 section changed since


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    report_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    scope_type: Mapped[str] = mapped_column(String(16), nullable=False)
    windfarm_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=True, index=True
    )
    portfolio_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=True, index=True
    )
    period_start: Mapped[datetime] = mapped_column(Date, nullable=False)
    period_end: Mapped[datetime] = mapped_column(Date, nullable=False)

    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=ReportStatus.PENDING, index=True
    )
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    params: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # PDF artifact (rendered at generation time; key: reports/{id}/v{n}/{slug}.pdf)
    pdf_s3_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    pdf_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    # Retention flag: set on first download — an exported version is never pruned.
    pdf_downloaded_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    locked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    superseded_by_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("reports.id", ondelete="SET NULL"), nullable=True
    )
    requested_by_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=False, index=True
    )

    total_cost_usd: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    total_input_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_output_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    generation_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    generation_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=_utcnow, onupdate=_utcnow, nullable=False
    )

    windfarm = relationship("Windfarm")
    portfolio = relationship("Portfolio")
    requested_by = relationship("User")
    superseded_by = relationship("Report", remote_side="Report.id")
    sections = relationship(
        "ReportSection",
        back_populates="report",
        cascade="all, delete-orphan",
        order_by="ReportSection.display_order",
    )

    __table_args__ = (
        # One in-flight generation per logical target: a concurrent duplicate
        # POST hits this index and surfaces as a 409 instead of a second run.
        Index(
            "uq_reports_inflight",
            "report_type",
            "windfarm_id",
            "portfolio_id",
            unique=True,
            postgresql_where=(status.in_(("PENDING", "GENERATING"))),
        ),
        # Library listing: latest live reports first.
        Index(
            "ix_reports_library",
            "report_type",
            "windfarm_id",
            "created_at",
            postgresql_where=(deleted_at.is_(None)),
        ),
    )

    @property
    def is_frozen(self) -> bool:
        """Retain-on-export: once a version's PDF left the platform (or it was
        explicitly locked), its content and stored artifact never change."""
        return self.locked or self.pdf_downloaded_at is not None

    def __repr__(self) -> str:
        return (
            f"<Report(id={self.id}, type={self.report_type}, wf={self.windfarm_id}, "
            f"v{self.version}, {self.status})>"
        )


class ReportSection(Base):
    __tablename__ = "report_sections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    report_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("reports.id", ondelete="CASCADE"), nullable=False, index=True
    )
    section_key: Mapped[str] = mapped_column(String(64), nullable=False)
    pass_number: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1)
    display_order: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    layout: Mapped[str] = mapped_column(String(16), nullable=False, default="full")

    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=SectionStatus.UNGENERATED
    )

    # Deterministic data slice (Pass-1 builder output), incl. metric cards and
    # directional indicators — computed server-side so UI and PDF agree.
    data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    # Structured LLM output (Phase 2) and its flat rendered text.
    narrative_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    narrative_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    llm_model: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    prompt_version: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    input_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=_utcnow, onupdate=_utcnow, nullable=False
    )

    report = relationship("Report", back_populates="sections")

    __table_args__ = (UniqueConstraint("report_id", "section_key", name="uq_report_section_key"),)

    def __repr__(self) -> str:
        return f"<ReportSection(report={self.report_id}, key={self.section_key}, {self.status})>"
