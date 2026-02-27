from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class FinancialEntity(Base):
    __tablename__ = "financial_entities"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(100), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    entity_type = Column(String(50), nullable=False, default="spv")  # spv / holdco / fund / joint_venture / other
    registration_number = Column(String(100), nullable=True)
    country_of_incorporation = Column(String(100), nullable=True)
    parent_entity_id = Column(Integer, ForeignKey("financial_entities.id"), nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    windfarm_financial_entities = relationship(
        "WindfarmFinancialEntity", back_populates="financial_entity", cascade="all, delete-orphan"
    )
    financial_data = relationship(
        "FinancialData", back_populates="financial_entity", cascade="all, delete-orphan"
    )
    parent_entity = relationship(
        "FinancialEntity", remote_side=[id], backref="child_entities"
    )
