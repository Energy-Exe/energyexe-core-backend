"""UserConsent model — append-only log of legal document acceptances."""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class UserConsent(Base):
    """Append-only record of a user accepting a versioned legal document."""

    __tablename__ = "user_consents"
    __table_args__ = (
        Index("ix_user_consents_user_doc", "user_id", "document_type"),
        Index("ix_user_consents_accepted_at", "accepted_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    document_type: Mapped[str] = mapped_column(String(16), nullable=False)
    document_version: Mapped[str] = mapped_column(String(32), nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    user = relationship("User", back_populates="consents")

    def __repr__(self) -> str:
        return (
            f"<UserConsent(user_id={self.user_id}, "
            f"document='{self.document_type}', version='{self.document_version}')>"
        )
