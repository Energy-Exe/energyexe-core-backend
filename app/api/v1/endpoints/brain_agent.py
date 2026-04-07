"""Brain Agent (Claude Agent SDK) API endpoints."""

import asyncio
import json
import mimetypes
import time
from pathlib import Path
from typing import List

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, get_db
from app.models.agent_thread import AgentThread
from app.models.user import User
from app.schemas.brain_agent import (
    AgentChatRequest,
    AgentInterruptRequest,
    ThreadDetail,
    ThreadListItem,
    ThreadTitleUpdate,
    ThreadUpsertRequest,
)
from app.services.brain_agent_service import BrainAgentService

logger = structlog.get_logger(__name__)

router = APIRouter()

# SSE heartbeat interval in seconds — keeps connections alive through proxies.
# Shorter interval (5s) ensures Railway/proxy doesn't buffer or drop the connection.
HEARTBEAT_INTERVAL = 5


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
        last_event_time = time.monotonic()
        try:
            async for event in _with_heartbeat(
                service.chat(
                    user_id=current_user.id,
                    session_id=request.session_id,
                    prompt=request.prompt,
                    user_name=user_name,
                    model=request.model,
                    conversation_history=request.conversation_history,
                ),
                interval=HEARTBEAT_INTERVAL,
            ):
                if event is None:
                    # Heartbeat — real SSE event (not a comment) so proxies don't drop it
                    yield f"event: heartbeat\ndata: {{}}\n\n"
                else:
                    yield f"event: {event.event_type}\ndata: {json.dumps(event.data, default=str)}\n\n"
        except Exception as e:
            import traceback
            logger.error("brain_agent_stream_error", error=str(e), traceback=traceback.format_exc())
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


async def _with_heartbeat(aiter, interval: float):
    """Wrap an async generator to yield None (heartbeat) during inactivity.

    Uses asyncio.wait() instead of asyncio.wait_for() to avoid cancelling
    the generator's coroutine on timeout (which would kill the SDK subprocess).
    """
    ait = aiter.__aiter__()
    pending_next: asyncio.Task | None = None
    try:
        while True:
            if pending_next is None:
                pending_next = asyncio.ensure_future(ait.__anext__())

            done, _ = await asyncio.wait({pending_next}, timeout=interval)
            if done:
                try:
                    event = pending_next.result()
                    yield event
                except StopAsyncIteration:
                    break
                pending_next = None
            else:
                # Timeout — yield heartbeat but DON'T cancel the pending task
                yield None
    finally:
        if pending_next is not None and not pending_next.done():
            pending_next.cancel()
            try:
                await pending_next
            except (asyncio.CancelledError, StopAsyncIteration):
                pass


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


ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif"}


@router.get("/sessions/{session_id}/files/{filename}")
async def get_session_file(
    session_id: str,
    filename: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> FileResponse:
    """Serve an image file from an agent session's sandbox directory."""
    # Validate session ownership
    service = BrainAgentService(db)
    sessions = service.list_sessions(current_user.id)
    if not any(s["session_id"] == session_id for s in sessions):
        raise HTTPException(status_code=404, detail="Session not found")

    # Security: only allow image files, prevent path traversal
    safe_filename = Path(filename).name
    ext = Path(safe_filename).suffix.lower()
    if ext not in ALLOWED_IMAGE_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only image files are allowed")

    file_path = Path(f"/tmp/brain-agent/{current_user.id}/{session_id}") / safe_filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    media_type = mimetypes.guess_type(safe_filename)[0] or "application/octet-stream"
    return FileResponse(file_path, media_type=media_type)


# ---------------------------------------------------------------------------
# Thread persistence endpoints
# ---------------------------------------------------------------------------


@router.get("/threads", response_model=List[ThreadListItem])
async def list_threads(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[ThreadListItem]:
    """List the current user's chat threads (lightweight, no messages)."""
    result = await db.execute(
        select(AgentThread)
        .where(AgentThread.user_id == current_user.id)
        .order_by(AgentThread.updated_at.desc())
        .limit(50)
    )
    threads = result.scalars().all()
    return [ThreadListItem.model_validate(t) for t in threads]


@router.get("/threads/{thread_id}", response_model=ThreadDetail)
async def get_thread(
    thread_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ThreadDetail:
    """Get a full thread including messages. Validates user ownership."""
    result = await db.execute(
        select(AgentThread).where(
            AgentThread.id == thread_id,
            AgentThread.user_id == current_user.id,
        )
    )
    thread = result.scalar_one_or_none()
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    return ThreadDetail.model_validate(thread)


@router.put("/threads/{thread_id}", response_model=ThreadDetail)
async def upsert_thread(
    thread_id: str,
    body: ThreadUpsertRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ThreadDetail:
    """Create or update a chat thread."""
    result = await db.execute(
        select(AgentThread).where(AgentThread.id == thread_id)
    )
    thread = result.scalar_one_or_none()

    if thread:
        # Validate ownership before updating
        if thread.user_id != current_user.id:
            raise HTTPException(status_code=404, detail="Thread not found")
        thread.title = body.title
        thread.model = body.model
        thread.messages = body.messages
        thread.message_count = body.message_count
        thread.total_cost_usd = body.total_cost_usd
        thread.total_turns = body.total_turns
    else:
        thread = AgentThread(
            id=thread_id,
            user_id=current_user.id,
            title=body.title,
            model=body.model,
            messages=body.messages,
            message_count=body.message_count,
            total_cost_usd=body.total_cost_usd,
            total_turns=body.total_turns,
        )
        db.add(thread)

    await db.commit()
    await db.refresh(thread)
    return ThreadDetail.model_validate(thread)


@router.delete("/threads/{thread_id}")
async def delete_thread(
    thread_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    """Delete a chat thread. Validates user ownership."""
    result = await db.execute(
        select(AgentThread).where(
            AgentThread.id == thread_id,
            AgentThread.user_id == current_user.id,
        )
    )
    thread = result.scalar_one_or_none()
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    await db.delete(thread)
    await db.commit()
    return {"success": True, "thread_id": thread_id}


@router.patch("/threads/{thread_id}/title", response_model=ThreadDetail)
async def rename_thread(
    thread_id: str,
    body: ThreadTitleUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ThreadDetail:
    """Rename a chat thread. Validates user ownership."""
    result = await db.execute(
        select(AgentThread).where(
            AgentThread.id == thread_id,
            AgentThread.user_id == current_user.id,
        )
    )
    thread = result.scalar_one_or_none()
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    thread.title = body.title
    await db.commit()
    await db.refresh(thread)
    return ThreadDetail.model_validate(thread)
