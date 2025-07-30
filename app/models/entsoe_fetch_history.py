from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class EntsoeFetchHistory(Base):
    __tablename__ = "entsoe_fetch_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    request_type: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # 'real_time' or 'historical_batch'
    start_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    area_code: Mapped[str] = mapped_column(
        String(100), nullable=False
    )  # Can be comma-separated for multiple
    production_type: Mapped[str] = mapped_column(
        String(100), nullable=False
    )  # Can be comma-separated
    status: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # 'pending', 'success', 'failed', 'partial'
    records_fetched: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    response_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    requested_by_user_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    requested_by_user = relationship("User", backref="entsoe_fetch_requests")
