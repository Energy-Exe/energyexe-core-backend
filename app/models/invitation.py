"""Invitation model for user invitations."""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class Invitation(Base):
    """Invitation model for inviting users to the platform."""

    __tablename__ = "invitations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    email: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    token: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    invited_by_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    used_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    # Relationships
    invited_by = relationship(
        "User", back_populates="invitations_sent", foreign_keys=[invited_by_id]
    )

    @property
    def is_expired(self) -> bool:
        """Check if the invitation has expired."""
        return datetime.utcnow() > self.expires_at

    @property
    def is_used(self) -> bool:
        """Check if the invitation has been used."""
        return self.used_at is not None

    @property
    def is_valid(self) -> bool:
        """Check if the invitation is still valid (not expired and not used)."""
        return not self.is_expired and not self.is_used

    def __repr__(self) -> str:
        """String representation of Invitation."""
        return f"<Invitation(id={self.id}, email='{self.email}', expired={self.is_expired})>"
