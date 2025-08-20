"""Model for storing EIA monthly generation data."""

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.core.database import Base


class EIAGenerationData(Base):
    """Model for storing EIA monthly generation data."""
    
    __tablename__ = "eia_generation_data"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Time dimension - monthly periods
    period = Column(String(7), nullable=False, index=True)  # YYYY-MM format
    year = Column(Integer, nullable=False, index=True)
    month = Column(Integer, nullable=False, index=True)
    
    # Plant identification
    plant_code = Column(String(50), nullable=False, index=True)
    plant_name = Column(String(255))
    state = Column(String(2))  # US state code
    
    # Generation data
    generation = Column(Numeric(12, 2), nullable=False)  # MWh monthly total
    fuel_type = Column(String(50), default="WND")
    unit = Column(String(10), default="MWh")
    
    # Relationships
    generation_unit_id = Column(Integer, ForeignKey("generation_units.id"))
    generation_unit = relationship("GenerationUnit", back_populates="eia_data")
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by_id = Column(Integer, ForeignKey("users.id"))
    updated_by_id = Column(Integer, ForeignKey("users.id"))
    
    created_by = relationship("User", foreign_keys=[created_by_id])
    updated_by = relationship("User", foreign_keys=[updated_by_id])
    
    # Composite unique constraint
    __table_args__ = (
        Index('idx_eia_unique', 'period', 'plant_code', 'fuel_type', unique=True),
        Index('idx_eia_year_month', 'year', 'month'),
        Index('idx_eia_gen_unit', 'generation_unit_id'),
    )