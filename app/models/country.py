from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.models.bidzone_country import bidzone_countries


class Country(Base):
    __tablename__ = "countries"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(3), unique=True, nullable=False, index=True)  # ISO 3166-1 alpha-3
    name = Column(String(255), nullable=False)
    lat = Column(Float, nullable=True)  # Latitude of country centroid
    lng = Column(Float, nullable=True)  # Longitude of country centroid
    polygon_wkt = Column(Text, nullable=True)  # Country boundary as WKT polygon string
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    states = relationship("State", back_populates="country")
    windfarms = relationship("Windfarm", back_populates="country")
    bidzones = relationship("Bidzone", secondary=bidzone_countries, back_populates="countries")
    control_areas = relationship("ControlArea", back_populates="country")
    market_balance_areas = relationship("MarketBalanceArea", back_populates="country")
