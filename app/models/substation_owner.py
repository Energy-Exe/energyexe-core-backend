from sqlalchemy import DECIMAL, Column, DateTime, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class SubstationOwner(Base):
    __tablename__ = "substation_owners"

    id = Column(Integer, primary_key=True, index=True)
    substation_id = Column(Integer, ForeignKey("substations.id"), nullable=False)
    owner_id = Column(Integer, ForeignKey("owners.id"), nullable=False)
    ownership_percentage = Column(DECIMAL(5, 2), nullable=False)  # 0.00 to 100.00
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    substation = relationship("Substation", back_populates="substation_owners")
    owner = relationship("Owner", back_populates="substation_owners")
