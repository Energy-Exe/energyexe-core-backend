from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Owner(Base):
    __tablename__ = "owners"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    type = Column(
        String(100), nullable=True
    )  # energy | institutional_investor | community_investors | municipality | private_individual | supply_chain_oem | other | unknown
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    windfarm_owners = relationship("WindfarmOwner", back_populates="owner")
    substation_owners = relationship("SubstationOwner", back_populates="owner")
    cables = relationship("Cable", back_populates="owner")
