"""Report commentary model for storing LLM-generated narrative sections."""

from datetime import datetime
from decimal import Decimal
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, Numeric, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.core.database import Base


class ReportCommentary(Base):
    """Store LLM-generated narrative commentary for report sections."""

    __tablename__ = "report_commentary"

    id = Column(Integer, primary_key=True, index=True)
    windfarm_id = Column(Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True)

    # Section identification
    section_type = Column(
        String(50),
        nullable=False,
        index=True,
        comment="Type of section: wind_resource, power_generation, peer_comparison, market_context, etc."
    )

    # Data context used for generation
    data_snapshot = Column(
        JSONB,
        nullable=False,
        comment="The data provided to LLM for context"
    )
    date_range_start = Column(DateTime, nullable=False, index=True)
    date_range_end = Column(DateTime, nullable=False, index=True)

    # Generated content
    commentary_text = Column(
        Text,
        nullable=False,
        comment="The generated narrative commentary"
    )

    # LLM metadata
    llm_provider = Column(String(20), nullable=False, comment="claude, gpt4, gpt4o, etc.")
    llm_model = Column(String(100), nullable=False, comment="Specific model version")
    prompt_template_version = Column(String(20), default="v1", comment="Version of prompt template used")

    # Usage tracking
    token_count_input = Column(Integer, nullable=False, default=0)
    token_count_output = Column(Integer, nullable=False, default=0)
    generation_cost_usd = Column(Numeric(10, 6), nullable=False, default=0, comment="Cost in USD")
    generation_duration_seconds = Column(Numeric(8, 2), nullable=False, default=0)

    # Status and versioning
    status = Column(
        String(20),
        nullable=False,
        default="published",
        comment="draft, approved, published"
    )
    version = Column(Integer, nullable=False, default=1)
    is_current = Column(Boolean, nullable=False, default=True, index=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    windfarm = relationship("Windfarm", back_populates="report_commentaries")

    # Indexes for common queries
    __table_args__ = (
        Index(
            'ix_report_commentary_lookup',
            'windfarm_id',
            'section_type',
            'date_range_start',
            'date_range_end',
            'is_current'
        ),
        Index('ix_report_commentary_created', 'created_at'),
    )

    def __repr__(self):
        return f"<ReportCommentary(id={self.id}, windfarm_id={self.windfarm_id}, section={self.section_type})>"
