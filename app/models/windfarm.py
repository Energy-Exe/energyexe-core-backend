from sqlalchemy import (
    DECIMAL,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Windfarm(Base):
    __tablename__ = "windfarms"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)

    # Required geography relationships
    country_id = Column(Integer, ForeignKey("countries.id"), nullable=False)
    state_id = Column(Integer, ForeignKey("states.id"), nullable=True)

    # Optional geography relationships
    region_id = Column(Integer, ForeignKey("regions.id"), nullable=True)
    bidzone_id = Column(Integer, ForeignKey("bidzones.id"), nullable=True)
    market_balance_area_id = Column(Integer, ForeignKey("market_balance_areas.id"), nullable=True)
    control_area_id = Column(Integer, ForeignKey("control_areas.id"), nullable=True)

    # Capacity and technical info
    nameplate_capacity_mw = Column(Float, nullable=True)

    # Relationships to energy entities
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True)

    # Dates
    commercial_operational_date = Column(Date, nullable=True)
    first_power_date = Column(Date, nullable=True)

    # Location
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
    polygon_wkt = Column(Text, nullable=True)

    # Technical characteristics
    foundation_type = Column(String(100), nullable=True)  # "fixed" | "floating"
    location_type = Column(String(100), nullable=True)  # "onshore" | "offshore"
    status = Column(
        String(100), nullable=True
    )  # "operational" | "decommissioned" | "under_installation" | "expanded"

    # Additional info
    notes = Column(String(300), nullable=True)
    alternate_name = Column(String(255), nullable=True)

    # Environmental and regulatory
    environmental_assessment_status = Column(String(100), nullable=True)
    permits_obtained = Column(Boolean, default=False, nullable=False)
    grid_connection_status = Column(String(100), nullable=True)

    # Financial
    total_investment_amount = Column(DECIMAL(15, 2), nullable=True)
    investment_currency = Column(String(3), nullable=True)

    # Address
    address = Column(Text, nullable=True)
    postal_code = Column(String(20), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    country = relationship("Country", back_populates="windfarms")
    state = relationship("State", back_populates="windfarms")
    region = relationship("Region", back_populates="windfarms")
    bidzone = relationship("Bidzone", back_populates="windfarms")
    market_balance_area = relationship("MarketBalanceArea", back_populates="windfarms")
    control_area = relationship("ControlArea", back_populates="windfarms")
    project = relationship("Project", back_populates="windfarms")
    windfarm_owners = relationship(
        "WindfarmOwner", back_populates="windfarm", cascade="all, delete-orphan"
    )
    turbine_units = relationship(
        "TurbineUnit", back_populates="windfarm", cascade="all, delete-orphan"
    )
    generation_units = relationship("GenerationUnit", back_populates="windfarm")
    generation_data = relationship("GenerationData", back_populates="windfarm")
