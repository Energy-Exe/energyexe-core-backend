"""Brain Agent (Claude Agent SDK) API endpoints."""

import json
from typing import List

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, get_db
from app.models.user import User
from app.schemas.brain_agent import AgentChatRequest, AgentInterruptRequest
from app.services.brain_agent_service import BrainAgentService

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.post("/chat")
async def agent_chat(
    request: AgentChatRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StreamingResponse:
    """Stream a Brain Agent response via Server-Sent Events.

    The agent has access to built-in tools (Read, Bash, WebSearch, etc.)
    plus custom energy data MCP tools. Maintains session across messages.
    """
    service = BrainAgentService(db)

    user_name = None
    if current_user.first_name:
        user_name = current_user.first_name
        if current_user.last_name:
            user_name += f" {current_user.last_name}"

    async def event_generator():
        try:
            async for event in service.chat(
                user_id=current_user.id,
                session_id=request.session_id,
                prompt=request.prompt,
                user_name=user_name,
            ):
                yield f"event: {event.event_type}\ndata: {json.dumps(event.data, default=str)}\n\n"
        except Exception as e:
            logger.error("brain_agent_stream_error", error=str(e))
            error_data = json.dumps(
                {"message": "Internal server error", "code": "internal_error"}
            )
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


@router.post("/interrupt")
async def agent_interrupt(
    request: AgentInterruptRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    """Interrupt a running agent task."""
    service = BrainAgentService(db)
    success = await service.interrupt(request.session_id, current_user.id)
    return {"success": success, "session_id": request.session_id}


@router.get("/sessions")
async def list_sessions(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[dict]:
    """List active agent sessions for the current user."""
    service = BrainAgentService(db)
    return service.list_sessions(current_user.id)


@router.delete("/sessions/{session_id}")
async def end_session(
    session_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    """End an agent session and clean up resources."""
    service = BrainAgentService(db)
    success = await service.end_session(session_id, current_user.id)
    return {"success": success, "session_id": session_id}
