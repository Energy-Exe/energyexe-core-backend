"""Portfolio models for user-managed wind farm collections."""

from datetime import datetime
from typing import Optional, List

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
import enum

from app.core.database import Base


class PortfolioType(str, enum.Enum):
    """Type of portfolio."""
    WATCHLIST = "watchlist"
    OWNED = "owned"
    COMPETITOR = "competitor"
    CUSTOM = "custom"


class Portfolio(Base):
    """Portfolio model for grouping wind farms."""

    __tablename__ = "portfolios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    portfolio_type: Mapped[PortfolioType] = mapped_column(
        Enum(PortfolioType), default=PortfolioType.CUSTOM, nullable=False
    )
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Relationships
    user = relationship("User", back_populates="portfolios")
    items = relationship("PortfolioItem", back_populates="portfolio", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Portfolio(id={self.id}, name='{self.name}', user_id={self.user_id})>"


class PortfolioItem(Base):
    """Portfolio item linking portfolios to wind farms."""

    __tablename__ = "portfolio_items"
    __table_args__ = (
        UniqueConstraint("portfolio_id", "windfarm_id", name="uq_portfolio_windfarm"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    portfolio_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=False, index=True
    )
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    portfolio = relationship("Portfolio", back_populates="items")
    windfarm = relationship("Windfarm")

    def __repr__(self) -> str:
        return f"<PortfolioItem(portfolio_id={self.portfolio_id}, windfarm_id={self.windfarm_id})>"


class UserFavorite(Base):
    """User favorites for quick access to wind farms."""

    __tablename__ = "user_favorites"
    __table_args__ = (
        UniqueConstraint("user_id", "windfarm_id", name="uq_user_windfarm_favorite"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    user = relationship("User", back_populates="favorites")
    windfarm = relationship("Windfarm")

    def __repr__(self) -> str:
        return f"<UserFavorite(user_id={self.user_id}, windfarm_id={self.windfarm_id})>"
