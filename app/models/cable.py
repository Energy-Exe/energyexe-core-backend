from sqlalchemy import Column, Integer, String, Text, DateTime, Float, ForeignKey, DECIMAL
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core.database import Base


class Cable(Base):
    __tablename__ = "cables"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    
    # Type
    type = Column(String(100), nullable=True)  # "export" | "array" | "land" | "interconnector"
    
    # Owner relationship
    owner_id = Column(Integer, ForeignKey("owners.id"), nullable=True)
    
    # Connection points (polymorphic relationships)
    from_type = Column(String(50), nullable=False)  # "turbine" | "substation" | "windfarm"
    from_id = Column(Integer, nullable=False)
    to_type = Column(String(50), nullable=False)  # "turbine" | "substation" | "windfarm"
    to_id = Column(Integer, nullable=False)
    
    # Technical specifications
    current_type = Column(String(10), nullable=True)  # "ac" | "dc"
    voltage_kv = Column(Integer, nullable=True)
    length_km = Column(DECIMAL(8, 2), nullable=True)
    
    # Location
    landing_point_lat = Column(Float, nullable=True)
    landing_point_lng = Column(Float, nullable=True)
    route_wkt = Column(Text, nullable=True)
    
    # Additional info
    notes = Column(Text, nullable=True)
    
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    owner = relationship("Owner", back_populates="cables")