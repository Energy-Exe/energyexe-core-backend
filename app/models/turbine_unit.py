from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, DECIMAL
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core.database import Base


class TurbineUnit(Base):
    __tablename__ = "turbine_units"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)  # Format: {WINDFARM_ID}_{SERIAL}
    
    # Required relationships
    windfarm_id = Column(Integer, ForeignKey("windfarms.id"), nullable=False)
    turbine_model_id = Column(Integer, ForeignKey("turbine_models.id"), nullable=False)
    
    # Location (required)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    
    # Status and technical info
    status = Column(String(100), nullable=True)  # "operational" | "installing" | "decommissioned"
    hub_height_m = Column(DECIMAL(6, 2), nullable=True)
    
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    windfarm = relationship("Windfarm", back_populates="turbine_units")
    turbine_model = relationship("TurbineModel", back_populates="turbine_units")