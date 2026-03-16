"""Brain (AI Chat Agent) API endpoints."""

import json
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, get_db
from app.models.user import User
from app.schemas.chat import ChatStreamRequest
from app.services.brain_chat_service import BrainChatService

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.post("/chat/stream")
async def chat_stream(
    request: ChatStreamRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StreamingResponse:
    """Stream a Brain chat response via Server-Sent Events.

    The frontend sends the full conversation history with each request.
    The backend is stateless per request — no conversation persistence.
    """
    service = BrainChatService(db)

    # Convert Pydantic messages to dicts
    messages = [{"role": m.role, "content": m.content} for m in request.messages]

    async def event_generator():
        try:
            async for event in service.stream_response(
                messages=messages,
                context=request.context,
                user=current_user,
                model_override=request.model,
            ):
                yield f"event: {event.event_type}\ndata: {json.dumps(event.data, default=str)}\n\n"
        except Exception as e:
            logger.error("brain_stream_error", error=str(e))
            error_data = json.dumps({"message": "Internal server error", "code": "internal_error"})
            yield f"event: error\ndata: {error_data}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/suggested-questions")
async def get_suggested_questions(
    windfarm_id: Optional[int] = Query(None),
    page_route: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
) -> List[str]:
    """Return context-aware suggested questions."""
    if windfarm_id:
        return [
            "What is the capacity factor for this windfarm over the last year?",
            "Show me the generation trend by month",
            "How does this windfarm compare to peers?",
            "What is the capture rate for this windfarm?",
            "Are there any data quality issues?",
        ]

    if page_route and "price" in page_route:
        return [
            "Which windfarm has the best capture rate?",
            "Show pricing trends for the UK market",
            "How many negative price hours were there last month?",
        ]

    if page_route and "comparison" in page_route:
        return [
            "Compare the top 3 offshore windfarms by capacity factor",
            "Which country has the highest average capacity factor?",
            "Show me all Norwegian windfarms",
        ]

    return [
        "Which windfarm has the highest capacity factor this year?",
        "Compare the performance of offshore vs onshore farms",
        "Show me all windfarms in Norway",
        "What are the pricing trends for the UK market?",
        "Which farms have data quality issues?",
    ]


@router.get("/models")
async def list_models(
    current_user: User = Depends(get_current_user),
) -> List[dict]:
    """Return available LLM models based on configured API keys."""
    return BrainChatService.get_available_models()
