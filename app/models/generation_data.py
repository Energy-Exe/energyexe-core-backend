"""Unified generation data models."""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List
from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
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


class GenerationDataRaw(Base):
    """Raw generation data from all sources."""
    
    __tablename__ = "generation_data_raw"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    
    # Source identification
    source: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(20), nullable=False, default="api")
    
    # Temporal fields
    period_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    period_type: Mapped[Optional[str]] = mapped_column(String(20))
    
    # Raw data storage
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    
    # Extracted key fields for indexing
    identifier: Mapped[Optional[str]] = mapped_column(Text)
    value_extracted: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    unit: Mapped[Optional[str]] = mapped_column(String(10))
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    
    __table_args__ = (
        Index('idx_raw_period', 'period_start', 'period_end'),
        Index('idx_raw_identifier', 'identifier'),
    )
    
    def __repr__(self) -> str:
        return f"<GenerationDataRaw(id={self.id}, source={self.source}, period_start={self.period_start})>"


class GenerationData(Base):
    """Processed hourly generation data."""
    
    __tablename__ = "generation_data"
    
    id: Mapped[str] = mapped_column(
        PostgresUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    
    # Fixed hourly period
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    
    # Relations
    generation_unit_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("generation_units.id")
    )
    windfarm_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("windfarms.id")
    )
    turbine_unit_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("turbine_units.id")
    )

    # Values
    generation_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    capacity_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    capacity_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))

    # Raw values from source
    raw_capacity_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    raw_capacity_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))
    
    # Source tracking
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    source_resolution: Mapped[Optional[str]] = mapped_column(String(20))
    raw_data_ids: Mapped[Optional[List[int]]] = mapped_column(ARRAY(BigInteger))
    
    # Quality
    quality_flag: Mapped[Optional[str]] = mapped_column(String(20))
    quality_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    completeness: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    
    # Relationships
    generation_unit = relationship("GenerationUnit", back_populates="generation_data")
    windfarm = relationship("Windfarm", back_populates="generation_data")
    turbine_unit = relationship("TurbineUnit", back_populates="generation_data")
    
    __table_args__ = (
        UniqueConstraint('hour', 'generation_unit_id', 'source', name='uq_generation_hour_unit_source'),
        Index('idx_gen_unit_hour', 'generation_unit_id', 'hour'),
        Index('idx_gen_windfarm_hour', 'windfarm_id', 'hour'),
    )
    
    def __repr__(self) -> str:
        return f"<GenerationData(hour={self.hour}, generation_mwh={self.generation_mwh}, source={self.source})>"


class GenerationUnitMapping(Base):
    """Mapping between source identifiers and generation units."""
    
    __tablename__ = "generation_unit_mapping"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Mapping fields
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    source_identifier: Mapped[str] = mapped_column(Text, nullable=False)
    generation_unit_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("generation_units.id")
    )
    windfarm_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("windfarms.id")
    )
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    
    # Relationships
    generation_unit = relationship("GenerationUnit")
    windfarm = relationship("Windfarm")
    
    __table_args__ = (
        UniqueConstraint('source', 'source_identifier', name='uq_source_identifier'),
        Index('idx_mapping_source_id', 'source', 'source_identifier'),
    )
    
    def __repr__(self) -> str:
        return f"<GenerationUnitMapping(source={self.source}, identifier={self.source_identifier})>"