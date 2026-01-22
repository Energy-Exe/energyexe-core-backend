"""UserFeature model for feature flags per user."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


# Default feature keys that can be assigned to users
DEFAULT_FEATURES = [
    "can_view_dashboard",
    "can_view_reports",
    "can_export_data",
    "can_view_analytics",
]


class UserFeature(Base):
    """UserFeature model for managing feature flags per user."""

    __tablename__ = "user_features"
    __table_args__ = (
        UniqueConstraint("user_id", "feature_key", name="uq_user_feature"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    feature_key: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, onupdate=datetime.utcnow, nullable=True
    )

    # Relationships
    user = relationship("User", back_populates="features")

    def __repr__(self) -> str:
        """String representation of UserFeature."""
        return f"<UserFeature(user_id={self.user_id}, feature='{self.feature_key}', enabled={self.enabled})>"
