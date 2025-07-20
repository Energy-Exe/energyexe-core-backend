from datetime import datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import relationship

from app.core.database import Base


class State(Base):
    __tablename__ = "states"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    country_id = Column(Integer, ForeignKey("countries.id"), nullable=False)
    lat = Column(Float, nullable=True)  # Latitude of state centroid
    lng = Column(Float, nullable=True)  # Longitude of state centroid
    polygon_wkt = Column(Text, nullable=True)  # State boundary as WKT polygon string
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    country = relationship("Country", back_populates="states")
    windfarms = relationship("Windfarm", back_populates="state")