from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class WindfarmFinancialEntity(Base):
    __tablename__ = "windfarm_financial_entities"
    __table_args__ = (
        UniqueConstraint("windfarm_id", "financial_entity_id", name="uq_windfarm_financial_entity"),
    )

    id = Column(Integer, primary_key=True, index=True)
    windfarm_id = Column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    financial_entity_id = Column(
        Integer, ForeignKey("financial_entities.id", ondelete="CASCADE"), nullable=False, index=True
    )
    relationship_type = Column(String(50), nullable=True)  # primary_asset / consolidated
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    windfarm = relationship("Windfarm", back_populates="windfarm_financial_entities")
    financial_entity = relationship("FinancialEntity", back_populates="windfarm_financial_entities")
