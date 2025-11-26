from sqlalchemy import (
    DECIMAL,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class PPA(Base):
    """Power Purchase Agreement model for tracking PPA contracts with windfarms."""

    __tablename__ = "ppas"
    __table_args__ = (
        UniqueConstraint(
            "windfarm_id",
            "ppa_buyer",
            "ppa_start_date",
            "ppa_end_date",
            name="uq_ppa_windfarm_buyer_dates",
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    windfarm_id = Column(Integer, ForeignKey("windfarms.id"), nullable=False, index=True)

    # PPA details
    ppa_buyer = Column(String(255), nullable=False)
    ppa_size_mw = Column(DECIMAL(10, 2), nullable=True)
    ppa_duration_years = Column(Integer, nullable=True)
    ppa_start_date = Column(Date, nullable=True)
    ppa_end_date = Column(Date, nullable=True)
    ppa_notes = Column(String(200), nullable=True)

    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    windfarm = relationship("Windfarm", back_populates="ppas")
