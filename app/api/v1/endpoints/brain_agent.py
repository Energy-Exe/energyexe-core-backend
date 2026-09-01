"""Brain Agent (Claude Agent SDK) API endpoints."""

import asyncio
import json
import mimetypes
import time
import uuid
from pathlib import Path
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, Response, StreamingResponse
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
from app.services.brain_agent_service import SANDBOX_SEED_FILES, BrainAgentService
from app.services.brain_agent_uploads import (
    MAX_SESSION_UPLOAD_BYTES,
    MAX_SESSION_UPLOADS,
    MAX_UPLOAD_BYTES,
    UPLOAD_MANIFEST,
    UploadRejected,
    existing_upload_bytes,
    read_upload_manifest,
    record_upload,
    sanitize_upload_name,
    work_dir_for,
)

logger = structlog.get_logger(__name__)

router = APIRouter()

# SSE heartbeat interval in seconds — keeps connections alive through proxies.
# Shorter interval (5s) keeps the ALB (idle_timeout 300s) and browser read
# timeouts fed even when the agent is quiet between events.
HEARTBEAT_INTERVAL = 5

# Per-user turn rate limits (turns per 60s window), by effective source. Agent
# turns are expensive and long-running, so these are deliberately low. Enforced
# via Valkey; fails open when Valkey is unavailable (see check_rate_limit).
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_PER_MINUTE = {"admin": 20, "client": 10}


@router.post("/chat")
async def agent_chat(
    request: AgentChatRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    x_agent_source: Optional[str] = Header(default=None, alias="X-Agent-Source"),
) -> StreamingResponse:
    """Stream a Brain Agent response via Server-Sent Events.

    The agent has access to built-in tools (Read, Bash, WebSearch, etc.)
    plus custom energy data MCP tools. Maintains session across messages.
    """
    service = BrainAgentService(db)

    # Resolve effective source from body, header, and role.
    # Clients are always forced to "client" regardless of what they send.
    # Admins default to "admin" but may opt into "client" for testing.
    # Brain agent is currently open to every authenticated user — no feature gate.
    requested_source = request.source or x_agent_source
    if current_user.role == "client":
        effective_source = "client"
    else:
        effective_source = requested_source if requested_source in ("admin", "client") else "admin"

    # Per-user rate limit. Checked before opening the SSE stream so we can
    # return a real 429 with Retry-After (an error event mid-stream can't carry
    # the status code). Fails open if Valkey is down.
    from app.core.redis import check_rate_limit

    limit = RATE_LIMIT_PER_MINUTE.get(effective_source, RATE_LIMIT_PER_MINUTE["client"])
    allowed, retry_after = await check_rate_limit(
        key=f"brain-agent:{current_user.id}",
        limit=limit,
        window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    )
    if not allowed:
        logger.info(
            "brain_agent_rate_limited",
            user_id=current_user.id,
            source=effective_source,
            retry_after=retry_after,
        )
        raise HTTPException(
            status_code=429,
            detail="You're sending messages too quickly. Please wait a moment and try again.",
            headers={"Retry-After": str(retry_after)},
        )

    # EPR-98: reject a duplicate of the run already live on this session with a
    # real 409 before the SSE stream opens. Clients treat any non-OK response as
    # terminal (no retry), so this is what breaks a re-POST storm — without it a
    # retried send writes the same user turn into the live SDK session again.
    if request.session_id and BrainAgentService.check_duplicate_send(
        request.session_id, request.message_id, request.prompt
    ):
        logger.info(
            "brain_agent_duplicate_send_rejected",
            user_id=current_user.id,
            session_id=request.session_id,
            has_message_id=bool(request.message_id),
        )
        raise HTTPException(
            status_code=409,
            detail=(
                "The agent is still working on your previous message. "
                "The response will appear in this conversation when it finishes."
            ),
        )

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
                    user_first_name=current_user.first_name,
                    user_company_name=current_user.company_name,
                    model=request.model,
                    conversation_history=request.conversation_history,
                    source=effective_source,
                    message_id=request.message_id,
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


def _validated_session_id(session_id: str) -> str:
    """Reject anything that isn't a UUID.

    session_id is interpolated into a filesystem path, and the upload route can
    be called before a session exists, so there's no ownership record to lean on
    yet — the format check is what stops `../../` from escaping the sandbox root.
    Both portals mint the id with crypto.randomUUID(), so this is free.
    """
    try:
        uuid.UUID(session_id)
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid session id.")
    return session_id


@router.post("/sessions/{session_id}/files", status_code=201)
async def upload_session_file(
    session_id: str,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    """Attach a file to a chat so the agent can read it.

    The file is written into the session's sandbox — the agent's cwd — so its
    existing Read/Bash tools reach it with no extra tooling. Uploading is allowed
    before the first message: the sandbox is keyed by the session id the frontend
    already generated, and chat() reuses whatever id it is given.
    """
    _validated_session_id(session_id)

    try:
        safe_name = sanitize_upload_name(file.filename, SANDBOX_SEED_FILES)
    except UploadRejected as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    work_dir = work_dir_for(current_user.id, session_id)
    work_dir.mkdir(parents=True, exist_ok=True)

    existing = read_upload_manifest(work_dir)
    if safe_name not in existing and len(existing) >= MAX_SESSION_UPLOADS:
        raise HTTPException(
            status_code=400,
            detail=f"This chat already has {MAX_SESSION_UPLOADS} attachments. "
            "Start a new chat to attach more.",
        )

    already_used = existing_upload_bytes(work_dir)
    if safe_name in existing:
        # Replacing an existing attachment — its current bytes free up.
        current_path = work_dir / safe_name
        if current_path.is_file():
            already_used -= current_path.stat().st_size

    target = work_dir / safe_name
    written = 0
    try:
        with target.open("wb") as out:
            while chunk := await file.read(1024 * 1024):
                written += len(chunk)
                # Enforce while streaming — reading the whole upload into memory
                # first is how a big file takes the API process down with it.
                if written > MAX_UPLOAD_BYTES:
                    raise UploadRejected(
                        f"'{safe_name}' is larger than "
                        f"{MAX_UPLOAD_BYTES // (1024 * 1024)} MB."
                    )
                if already_used + written > MAX_SESSION_UPLOAD_BYTES:
                    raise UploadRejected(
                        "Attachments for this chat would exceed "
                        f"{MAX_SESSION_UPLOAD_BYTES // (1024 * 1024)} MB in total."
                    )
                out.write(chunk)
    except UploadRejected as exc:
        target.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        target.unlink(missing_ok=True)
        logger.error(
            "brain_agent_upload_write_failed",
            session_id=session_id,
            filename=safe_name,
            error=str(exc),
        )
        raise HTTPException(status_code=500, detail="Could not save the attachment.")
    finally:
        await file.close()

    if written == 0:
        target.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"'{safe_name}' is empty.")

    record_upload(work_dir, safe_name)

    service = BrainAgentService(db)
    service.register_upload(current_user.id, session_id, safe_name)

    # Mirror to S3 so the attachment survives a task restart (the sandbox lives on
    # ephemeral disk). Flat keys, same shape as agent outputs, so the existing
    # file-serving route works unchanged.
    await service._upload_file_to_s3(current_user.id, session_id, safe_name, target)
    await service._upload_file_to_s3(
        current_user.id, session_id, UPLOAD_MANIFEST, work_dir / UPLOAD_MANIFEST
    )

    logger.info(
        "brain_agent_upload_received",
        session_id=session_id,
        user_id=current_user.id,
        filename=safe_name,
        bytes=written,
    )
    return {"session_id": session_id, "filename": safe_name, "size": written}


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


@router.get("/files/{user_id}/{thread_id}/{filename}")
async def get_agent_file(
    user_id: int,
    thread_id: str,
    filename: str,
    current_user: User = Depends(get_current_user),
):
    """Serve a brain agent file (image, CSV, etc.) — local first, then S3."""
    if current_user.id != user_id:
        raise HTTPException(status_code=404, detail="File not found")

    safe_filename = Path(filename).name

    # Try local filesystem first (active session, fast path)
    local_path = Path(f"/tmp/brain-agent/{user_id}/{thread_id}") / safe_filename
    if local_path.exists() and local_path.is_file():
        media_type = mimetypes.guess_type(safe_filename)[0] or "application/octet-stream"
        return FileResponse(local_path, media_type=media_type)

    # Fallback to S3
    from app.services.s3_service import download_file

    s3_key = f"brain-agent/{user_id}/{thread_id}/{safe_filename}"
    file_bytes = await download_file(s3_key)
    if file_bytes is None:
        raise HTTPException(status_code=404, detail="File not found")

    media_type = mimetypes.guess_type(safe_filename)[0] or "application/octet-stream"
    return Response(
        content=file_bytes,
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=86400"},
    )


# ---------------------------------------------------------------------------
# Thread persistence endpoints
# ---------------------------------------------------------------------------


@router.get("/threads", response_model=List[ThreadListItem])
async def list_threads(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[ThreadListItem]:
    """List the current user's chat threads (lightweight, no messages)."""
    result = await db.execute(
        select(AgentThread)
        .where(AgentThread.user_id == current_user.id)
        .order_by(AgentThread.updated_at.desc())
        .limit(limit)
        .offset(offset)
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
        # Never-decrease: the server accumulates per-run cost deltas onto this
        # column, while the frontends PUT the SDK's process-cumulative figure
        # (which resets on session recreation) — letting that assignment win
        # would clobber the accumulated truth. Frontends should stop sending
        # it once both twins read thread_total_cost_usd from the result event.
        if body.total_cost_usd is not None:
            thread.total_cost_usd = max(thread.total_cost_usd or 0, body.total_cost_usd)
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

    # Clean up S3 files (best-effort, don't fail the delete)
    try:
        from app.services.s3_service import delete_by_prefix

        await delete_by_prefix(f"brain-agent/{current_user.id}/{thread_id}/")
    except Exception as e:
        logger.warning("brain_agent_s3_cleanup_failed", thread_id=thread_id, error=str(e))

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
