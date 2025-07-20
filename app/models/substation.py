from sqlalchemy import Column, Integer, String, Text, DateTime, Date, Float, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core.database import Base


class Substation(Base):
    __tablename__ = "substations"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    
    # Owner relationship
    owner_id = Column(Integer, ForeignKey("owners.id"), nullable=True)
    
    # Type
    substation_type = Column(String(100), nullable=True)  # "substation" | "converter"
    
    # Location (required)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    
    # Technical specifications
    current_type = Column(String(2), nullable=True)  # "ac" | "dc"
    array_cable_voltage_kv = Column(Integer, nullable=True)
    export_cable_voltage_kv = Column(Integer, nullable=True)
    transformer_capacity_mva = Column(Integer, nullable=True)
    
    # Dates
    commissioning_date = Column(Date, nullable=True)
    operational_date = Column(Date, nullable=True)
    
    # Additional info
    notes = Column(String(300), nullable=True)
    
    # Address
    address = Column(Text, nullable=True)
    postal_code = Column(String(20), nullable=True)
    
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    owner = relationship("Owner", back_populates="substations")