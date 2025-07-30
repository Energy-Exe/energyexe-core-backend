"""Power generation data model for TimescaleDB."""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class PowerGenerationData(Base):
    """Time-series table for power generation data."""

    __tablename__ = "power_generation_data"

    # Composite primary key for hypertable
    time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    area_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    production_type: Mapped[str] = mapped_column(String(20), primary_key=True)

    # Data fields
    generation_unit_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    generation_unit_source: Mapped[str] = mapped_column(String(20), default="ENTSOE")
    value_mw: Mapped[float] = mapped_column(Float, nullable=False)
    data_quality_score: Mapped[float] = mapped_column(Float, default=1.0)

    # Foreign keys
    fetch_history_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("entsoe_fetch_history.id"), nullable=True
    )

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    fetch_history = relationship("EntsoeFetchHistory", backref="generation_data")
