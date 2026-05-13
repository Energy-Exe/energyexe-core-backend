"""Per-route question template suggestions shown by the client portal brain-agent."""

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from app.core.database import Base


class AgentQuestionTemplate(Base):
    __tablename__ = "agent_question_templates"

    id = Column(Integer, primary_key=True, index=True)
    route_path = Column(String(255), unique=True, nullable=False, index=True)
    label = Column(String(255), nullable=False)
    # JSONB array of {"template": str, "fallback": str | None}
    questions = Column(JSONB, nullable=False, default=list)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
