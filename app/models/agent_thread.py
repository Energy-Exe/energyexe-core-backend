"""Agent chat thread persistence model."""

from sqlalchemy import Column, DateTime, Integer, Numeric, String, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from app.core.database import Base


class AgentThread(Base):
    __tablename__ = "agent_threads"

    id = Column(String(36), primary_key=True)  # UUID = session_id
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String(255), nullable=True)
    model = Column(String(50), nullable=True)
    messages = Column(JSONB, nullable=False, default=list)
    message_count = Column(Integer, default=0)
    total_cost_usd = Column(Numeric(10, 4), nullable=True)
    total_turns = Column(Integer, default=0)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
