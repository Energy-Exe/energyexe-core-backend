"""Schemas for Brain Agent endpoints."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field, field_serializer

# Current pair: Sonnet 5 (default, "balanced") and Opus 5 ("most capable").
# The older model strings stay accepted-but-legacy so a stale frontend bundle, a
# persisted browser session, or a thread saved under an old model string doesn't
# 422 during a deploy rollout.
ALLOWED_BRAIN_MODELS = (
    "claude-sonnet-5",
    "claude-opus-5",
    # legacy
    "claude-sonnet-4-6",
    "claude-opus-4-8",
    "claude-opus-4-6",
)

BrainModelType = Literal[
    "claude-sonnet-5",
    "claude-opus-5",
    "claude-sonnet-4-6",
    "claude-opus-4-8",
    "claude-opus-4-6",
]

DEFAULT_BRAIN_MODEL = "claude-sonnet-5"

AgentSourceType = Literal["admin", "client"]


class AgentChatRequest(BaseModel):
    """Request body for brain agent chat."""

    prompt: str = Field(..., description="The user's message/question")
    session_id: Optional[str] = Field(
        default=None,
        description="Session ID for multi-turn conversation. Auto-generated if omitted.",
    )
    message_id: Optional[str] = Field(
        default=None,
        description=(
            "Client-generated idempotency id for this send. Re-POSTs of the same "
            "logical message reuse it so the backend can reject duplicates while "
            "the run is live (EPR-98)."
        ),
    )
    model: Optional[BrainModelType] = Field(
        default=None,
        description="Claude model to use. Defaults to claude-sonnet-5.",
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

    @field_serializer("created_at", "updated_at")
    def _serialize_utc(self, dt: datetime) -> str:
        # Column is a naive DateTime storing UTC; without an explicit offset
        # browsers parse the ISO string as local time (threads showed "6h ago"
        # the moment they were created for a UTC+6 user).
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    model_config = {"from_attributes": True}


class ThreadDetail(ThreadListItem):
    """Full thread including messages."""

    messages: List[Any]

    model_config = {"from_attributes": True}
