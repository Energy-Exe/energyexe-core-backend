"""SQLAlchemy model for Elexon generation data storage."""

from decimal import Decimal
from datetime import datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.core.database import Base


class ElexonGenerationData(Base):
    """Model for storing historical Elexon generation data."""

    __tablename__ = "elexon_generation_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    timestamp = Column(DateTime, nullable=False, index=True)
    bm_unit = Column(String(50), nullable=False, index=True)
    settlement_period = Column(Integer, index=True)
    value = Column(Numeric(10, 2))
    unit = Column(String(10))
    
    # Foreign keys
    generation_unit_id = Column(Integer, ForeignKey("generation_units.id"))
    created_by_id = Column(Integer, ForeignKey("users.id"))
    updated_by_id = Column(Integer, ForeignKey("users.id"))
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    generation_unit = relationship("GenerationUnit", back_populates="elexon_data")
    created_by = relationship("User", foreign_keys=[created_by_id])
    updated_by = relationship("User", foreign_keys=[updated_by_id])
    
    # Composite index for efficient time-series queries and unique constraint
    __table_args__ = (
        Index('idx_elexon_data_timestamp_bm_unit', 'timestamp', 'bm_unit'),
        Index('idx_elexon_data_unique', 'timestamp', 'bm_unit', 'settlement_period', unique=True),
    )
    
    def __repr__(self):
        return f"<ElexonGenerationData(id={self.id}, timestamp={self.timestamp}, bm_unit={self.bm_unit}, value={self.value})>"