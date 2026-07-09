"""Platform update announcements managed from the admin panel and shown on the client dashboard."""

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from app.core.database import Base


class PlatformUpdate(Base):
    __tablename__ = "platform_updates"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    body_md = Column(Text, nullable=False)
    category = Column(String(50), nullable=True)
    published_at = Column(Date, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True, server_default="true")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
