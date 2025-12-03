"""Price data models for ENTSOE and other power price sources."""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List
from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID, JSONB, ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class PriceDataRaw(Base):
    """Raw price data from all sources (ENTSOE, etc.)."""

    __tablename__ = "price_data_raw"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Source identification
    source: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(20), nullable=False, default="api")

    # Price type (day_ahead, intraday)
    price_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    # Temporal fields
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    period_type: Mapped[Optional[str]] = mapped_column(String(20))

    # Bidzone identifier (e.g., DE_LU, FR, DK_1)
    identifier: Mapped[str] = mapped_column(Text, nullable=False, index=True)

    # Extracted key fields for indexing
    value_extracted: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4))
    unit: Mapped[Optional[str]] = mapped_column(String(20), default="EUR/MWh")
    currency: Mapped[Optional[str]] = mapped_column(String(3), default="EUR")

    # Raw data storage (full JSON from source)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            'source', 'identifier', 'period_start', 'price_type',
            name='uq_price_raw_source_identifier_period_type'
        ),
        Index('idx_price_raw_period', 'period_start', 'period_end'),
        Index('idx_price_raw_identifier', 'identifier'),
        Index('idx_price_raw_source_period', 'source', 'period_start'),
    )

    def __repr__(self) -> str:
        return f"<PriceDataRaw(id={self.id}, source={self.source}, identifier={self.identifier}, period_start={self.period_start})>"


class PriceData(Base):
    """Processed hourly price data mapped to windfarms."""

    __tablename__ = "price_data"

    id: Mapped[str] = mapped_column(
        PostgresUUID(as_uuid=True), primary_key=True, default=uuid4
    )

    # Fixed hourly period
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    # Relations
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id"), nullable=False
    )
    bidzone_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("bidzones.id")
    )

    # Price values
    day_ahead_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4))
    intraday_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4))

    # Currency
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="EUR")

    # Source tracking
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="ENTSOE")
    raw_data_ids: Mapped[Optional[List[int]]] = mapped_column(ARRAY(BigInteger))

    # Quality
    quality_flag: Mapped[Optional[str]] = mapped_column(String(20))
    quality_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Relationships
    windfarm = relationship("Windfarm", back_populates="price_data")
    bidzone = relationship("Bidzone", back_populates="price_data")

    __table_args__ = (
        UniqueConstraint('hour', 'windfarm_id', 'source', name='uq_price_hour_windfarm_source'),
        Index('idx_price_windfarm_hour', 'windfarm_id', 'hour'),
        Index('idx_price_bidzone_hour', 'bidzone_id', 'hour'),
        Index('idx_price_hour_range', 'hour'),
    )

    def __repr__(self) -> str:
        return f"<PriceData(hour={self.hour}, windfarm_id={self.windfarm_id}, day_ahead={self.day_ahead_price})>"
