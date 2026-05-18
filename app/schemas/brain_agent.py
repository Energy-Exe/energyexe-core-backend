"""Schemas for Brain Agent endpoints."""

from datetime import datetime
from decimal import Decimal
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field

ALLOWED_BRAIN_MODELS = ("claude-sonnet-4-6", "claude-opus-4-6")

BrainModelType = Literal["claude-sonnet-4-6", "claude-opus-4-6"]

DEFAULT_BRAIN_MODEL = "claude-sonnet-4-6"

AgentSourceType = Literal["admin", "client"]


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
    conversation_history: Optional[List[Any]] = Field(
        default=None,
        description="Prior messages for context when resuming a conversation with a new session.",
    )
    source: Optional[AgentSourceType] = Field(
        default=None,
        description=(
            "Caller surface: 'admin' for the internal admin UI, 'client' for the client portal. "
            "Clients are always forced to 'client' regardless of this value. Admins default to 'admin'."
        ),
    )


class AgentInterruptRequest(BaseModel):
    """Request body for interrupting a running agent."""

    session_id: str = Field(..., description="Session ID to interrupt")


# --- Thread persistence schemas ---


class ThreadUpsertRequest(BaseModel):
    """Request body for creating or updating an agent thread."""

    title: Optional[str] = Field(default=None, max_length=255, description="Thread title")
    model: Optional[str] = Field(default=None, max_length=50, description="Model used for this thread")
    messages: List[Any] = Field(default_factory=list, description="Full message history (JSON array)")
    message_count: int = Field(default=0, ge=0, description="Number of messages")
    total_cost_usd: Optional[Decimal] = Field(default=None, description="Cumulative cost in USD")
    total_turns: int = Field(default=0, ge=0, description="Number of agent turns")


class ThreadTitleUpdate(BaseModel):
    """Request body for renaming a thread."""

    title: str = Field(..., max_length=255, description="New thread title")


class ThreadListItem(BaseModel):
    """Lightweight thread summary (no messages)."""

    id: str
    title: Optional[str]
    model: Optional[str]
    message_count: int
    total_cost_usd: Optional[Decimal]
    total_turns: int
    is_streaming: bool = False
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ThreadDetail(ThreadListItem):
    """Full thread including messages."""

    messages: List[Any]

    model_config = {"from_attributes": True}
