"""Generation unit model."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class GenerationUnit(Base):
    """Generation unit model."""

    __tablename__ = "generation_units"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    source: Mapped[str] = mapped_column(String(50), nullable=False)
    fuel_type: Mapped[str] = mapped_column(String(100), nullable=False)
    technology_type: Mapped[str] = mapped_column(String(100), nullable=True)

    capacity_mw: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=True)

    windfarm_id: Mapped[int] = mapped_column(Integer, nullable=True)

    notes: Mapped[str] = mapped_column(Text, nullable=True)

    # Audit fields
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    def __repr__(self) -> str:
        """String representation of GenerationUnit."""
        return f"<GenerationUnit(id={self.id}, code='{self.code}', name='{self.name}')>"
