"""SQLAlchemy model for ENTSOE generation data."""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    DECIMAL,
    TIMESTAMP,
    BigInteger,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ENTSOEGenerationData(Base):
    """Model for storing ENTSOE generation data points."""

    __tablename__ = "entsoe_generation_data"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    area_code: Mapped[str] = mapped_column(String(50), nullable=False)
    production_type: Mapped[str] = mapped_column(String(50), nullable=False)
    value: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=False)
    unit: Mapped[str] = mapped_column(String(10), default="MW", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False
    )
    fetch_id: Mapped[Optional[UUID]] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)

    __table_args__ = (
        # Unique constraint to prevent duplicate data points
        UniqueConstraint("timestamp", "area_code", "production_type", name="uq_entsoe_data_point"),
        # Indexes for query performance
        Index("idx_entsoe_timestamp_area_production", "timestamp", "area_code", "production_type"),
        Index("idx_entsoe_fetch_id", "fetch_id"),
        Index("idx_entsoe_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<ENTSOEGenerationData("
            f"timestamp={self.timestamp}, "
            f"area={self.area_code}, "
            f"type={self.production_type}, "
            f"value={self.value}{self.unit})>"
        )