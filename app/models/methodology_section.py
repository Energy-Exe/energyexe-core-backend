"""Editable methodology sections shown on the client portal and fed to the brain-agent."""

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from app.core.database import Base


class MethodologySection(Base):
    __tablename__ = "methodology_sections"

    id = Column(Integer, primary_key=True, index=True)
    section_key = Column(String(100), unique=True, nullable=False, index=True)
    title = Column(String(255), nullable=False)
    description = Column(String(500), nullable=True)
    content_md = Column(Text, nullable=False)
    sort_order = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
