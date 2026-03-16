"""Schemas for Brain Agent endpoints."""

from typing import Optional

from pydantic import BaseModel, Field


class AgentChatRequest(BaseModel):
    """Request body for brain agent chat."""

    prompt: str = Field(..., description="The user's message/question")
    session_id: Optional[str] = Field(
        default=None,
        description="Session ID for multi-turn conversation. Auto-generated if omitted.",
    )


class AgentInterruptRequest(BaseModel):
    """Request body for interrupting a running agent."""

    session_id: str = Field(..., description="Session ID to interrupt")
