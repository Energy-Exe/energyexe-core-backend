from sqlalchemy import Column, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base
from app.models.bidzone_country import bidzone_countries


class Bidzone(Base):
    __tablename__ = "bidzones"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    bidzone_type = Column(
        String(50), nullable=True
    )  # "national", "regional", "interconnector", "virtual"
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
    polygon_wkt = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    windfarms = relationship("Windfarm", back_populates="bidzone")
    countries = relationship("Country", secondary=bidzone_countries, back_populates="bidzones")
