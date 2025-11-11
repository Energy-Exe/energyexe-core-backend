"""Weather data models for ERA5 Copernicus integration."""

from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PostgresUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class WeatherDataRaw(Base):
    """Raw weather data from ERA5 Copernicus (grid point level)."""

    __tablename__ = "weather_data_raw"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Source identification
    source: Mapped[str] = mapped_column(String(20), default="ERA5", nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(20), default="api", nullable=False)

    # Temporal fields (hourly UTC)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    # Spatial fields (ERA5 grid point - actual grid point returned by API)
    latitude: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    longitude: Mapped[float] = mapped_column(Numeric(7, 4), nullable=False)

    # Raw data storage (ALL ERA5 parameters in JSONB for flexibility)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    __table_args__ = (
        # Prevent duplicate fetches for same grid point + time
        UniqueConstraint('source', 'latitude', 'longitude', 'timestamp',
                        name='uq_weather_raw_grid_time'),
        # Efficient queries by time and location
        Index('idx_weather_raw_timestamp', 'timestamp'),
        Index('idx_weather_raw_location', 'latitude', 'longitude'),
    )

    def __repr__(self) -> str:
        return f"<WeatherDataRaw(id={self.id}, lat={self.latitude}, lon={self.longitude}, time={self.timestamp})>"


class WeatherData(Base):
    """Processed hourly weather data for windfarms."""

    __tablename__ = "weather_data"

    id: Mapped[str] = mapped_column(
        PostgresUUID(as_uuid=True), primary_key=True, default=uuid4
    )

    # Temporal (fixed hourly period)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    # Windfarm relationship
    windfarm_id: Mapped[int] = mapped_column(Integer, ForeignKey("windfarms.id"), nullable=False)

    # Calculated wind metrics (at 100m height)
    wind_speed_100m: Mapped[float] = mapped_column(Numeric(8, 3), nullable=False)
    wind_direction_deg: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)

    # Temperature
    temperature_2m_k: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False)
    temperature_2m_c: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)

    # Source tracking
    source: Mapped[str] = mapped_column(String(20), default="ERA5", nullable=False)
    raw_data_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("weather_data_raw.id"), nullable=True
    )

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Relationships
    windfarm = relationship("Windfarm", back_populates="weather_data")

    __table_args__ = (
        # One weather record per windfarm per hour
        UniqueConstraint('hour', 'windfarm_id', 'source',
                        name='uq_weather_hour_windfarm_source'),
        # Fast queries by windfarm + time range
        Index('idx_weather_windfarm_hour', 'windfarm_id', 'hour'),
    )

    def __repr__(self) -> str:
        return f"<WeatherData(windfarm_id={self.windfarm_id}, hour={self.hour}, wind_speed={self.wind_speed_100m})>"
