"""Schemas for Brain chat."""

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class ChatMessageItem(BaseModel):
    """A single message in the conversation history."""

    role: Literal["user", "assistant"] = Field(..., description="Message role")
    content: str = Field(..., description="Message text content")


class ChatStreamRequest(BaseModel):
    """Request body for streaming chat."""

    messages: List[ChatMessageItem] = Field(..., description="Full conversation history")
    context: Optional[dict] = Field(
        default=None,
        description="Optional context: {windfarm_id, page_route, portfolio_id}",
    )
    model: Optional[str] = Field(default=None, description="Override default model")
