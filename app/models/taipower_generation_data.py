"""Model for storing Taipower generation snapshots."""

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.core.database import Base


class TaipowerGenerationData(Base):
    """Model for storing Taipower generation snapshots."""
    
    __tablename__ = "taipower_generation_data"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Timestamp of the snapshot
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # Generation unit information
    generation_type = Column(String(50), index=True)  # Wind, Solar, etc.
    unit_name = Column(String(100), nullable=False, index=True)
    
    # Capacity and generation
    installed_capacity_mw = Column(Numeric(10, 2))
    net_generation_mw = Column(Numeric(10, 2))
    capacity_utilization_percent = Column(Numeric(5, 2))
    
    # Additional information
    notes = Column(Text)
    
    # Relationships
    generation_unit_id = Column(Integer, ForeignKey("generation_units.id"))
    generation_unit = relationship("GenerationUnit", back_populates="taipower_data")
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by_id = Column(Integer, ForeignKey("users.id"))
    updated_by_id = Column(Integer, ForeignKey("users.id"))
    
    created_by = relationship("User", foreign_keys=[created_by_id])
    updated_by = relationship("User", foreign_keys=[updated_by_id])
    
    # Composite unique constraint
    __table_args__ = (
        Index('idx_taipower_unique', 'timestamp', 'unit_name', unique=True),
        Index('idx_taipower_gen_type', 'generation_type'),
        Index('idx_taipower_gen_unit', 'generation_unit_id'),
    )