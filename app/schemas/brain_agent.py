"""Schemas for Brain Agent endpoints."""

from typing import Literal, Optional

from pydantic import BaseModel, Field

ALLOWED_BRAIN_MODELS = ("claude-sonnet-4-6", "claude-opus-4-6")

BrainModelType = Literal["claude-sonnet-4-6", "claude-opus-4-6"]

DEFAULT_BRAIN_MODEL = "claude-sonnet-4-6"


class AgentChatRequest(BaseModel):
    """Request body for brain agent chat."""

    prompt: str = Field(..., description="The user's message/question")
    session_id: Optional[str] = Field(
        default=None,
        description="Session ID for multi-turn conversation. Auto-generated if omitted.",
    )
    model: Optional[BrainModelType] = Field(
        default=None,
        description="Claude model to use. Defaults to claude-sonnet-4-6.",
    )


class AgentInterruptRequest(BaseModel):
    """Request body for interrupting a running agent."""

    session_id: str = Field(..., description="Session ID to interrupt")
