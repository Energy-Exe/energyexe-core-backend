from sqlalchemy import DECIMAL, Column, DateTime, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class TurbineModel(Base):
    __tablename__ = "turbine_models"

    id = Column(Integer, primary_key=True, index=True)
    model = Column(String(50), unique=True, nullable=False, index=True)
    supplier = Column(String(50), nullable=False)
    original_supplier = Column(String(50), nullable=False)

    rated_power_kw = Column(Integer, nullable=True)
    rotor_diameter_m = Column(DECIMAL(6, 2), nullable=True)

    cut_in_wind_speed_ms = Column(DECIMAL(4, 2), nullable=True)
    cut_out_wind_speed_ms = Column(DECIMAL(4, 2), nullable=True)
    rated_wind_speed_ms = Column(DECIMAL(4, 2), nullable=True)
    blade_length_m = Column(DECIMAL(6, 2), nullable=True)

    generator_type = Column(String(100), nullable=True)  # "direct_drive" | "geared" | "hybrid"

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    turbine_units = relationship("TurbineUnit", back_populates="turbine_model")
