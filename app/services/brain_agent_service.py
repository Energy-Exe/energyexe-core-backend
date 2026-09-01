"""Brain Agent service — orchestrates Claude Agent SDK sessions with energy data tools."""

import asyncio
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import structlog
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookMatcher,
    ResultMessage,
    SessionMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
    get_session_messages,
)
from claude_agent_sdk.types import StreamEvent

from app.core.config import get_settings
from app.schemas.brain_agent import DEFAULT_BRAIN_MODEL
from app.services.brain_agent_db_script import DB_HELPER_SCRIPT
from app.services.brain_agent_silver_script import SILVER_HELPER_SCRIPT
from app.services.brain_agent_hooks import make_pre_tool_use_hook
from app.services.brain_agent_uploads import (
    UPLOAD_MANIFEST,
    build_attachment_note,
    read_upload_manifest,
)
from app.services.brain_agent_skill_files import (
    CHART_STYLE_PY,
    REPORT_PDF_PY,
    SKILL_DOMAIN,
    SKILL_QUERIES,
    SKILL_SCADA,
    SKILL_SCADA_QUERIES,
    SKILL_SCADA_SILVER,
    SKILL_SCHEMA,
    SKILL_SOURCES,
)

logger = structlog.get_logger(__name__)

# Session TTL: clean up sessions idle for more than 30 minutes
SESSION_TTL_SECONDS = 30 * 60
MAX_CONCURRENT_SESSIONS = 20

# EPR-98 duplicate-send guard: a busy session whose run started longer ago than
# this is considered wedged, not live — the guard ignores it and chat()'s
# interrupt-and-drain path takes over as before.
STALE_RUN_SECONDS = 30 * 60
# How long a detached run (client disconnected mid-turn) may keep working in the
# background before being interrupted. Sits above the SDK's 20-min
# CLAUDE_CODE_STREAM_CLOSE_TIMEOUT so the SDK's own limit fires first.
DETACH_MAX_SECONDS = 20 * 60

# Per-source agent profiles. Keys map to the AgentSourceType literal in schemas.
#   - admin: existing behavior (unrestricted, model picks honored)
#   - client: same capability envelope as admin; uses a client-flavoured system
#     prompt with portfolio-as-anchor framing.
PROFILES: Dict[str, Dict[str, Any]] = {
    "admin": {
        "system_prompt_file": "brain_agent_system.md",
        "model_default": None,  # falls back to settings.BRAIN_MODEL
        "model_locked": False,
        "max_turns": 25,
        # Per-turn ceiling enforced by the SDK (stops a single runaway turn).
        "max_budget_usd": 5.0,
        # Cumulative per-thread ceiling enforced app-side in chat() — refuses a
        # new turn once a long conversation has spent this much.
        "max_thread_budget_usd": 50.0,
        # API-side token budget. Unlike max_budget_usd (a client-side hard stop
        # that can cut an answer off mid-sentence), this is passed to the model
        # so it knows how much room is left and wraps up on its own.
        "task_budget_tokens": 500_000,
        # Stream the model's reasoning to the UI. Admin only: thinking is
        # model-authored prose that can name internal tables, which the client
        # surface is not allowed to see (EPR-59).
        "stream_thinking": True,
        "wrap_user_input": False,
    },
    "client": {
        "system_prompt_file": "brain_agent_system_client.md",
        "model_default": None,
        "model_locked": False,
        "max_turns": 25,
        "max_budget_usd": 2.0,
        "max_thread_budget_usd": 20.0,
        "task_budget_tokens": 250_000,
        "stream_thinking": False,
        "wrap_user_input": False,
    },
}


def _get_profile(source: Optional[str]) -> Dict[str, Any]:
    """Return the profile dict for a given source, defaulting to admin."""
    return PROFILES.get(source or "admin", PROFILES["admin"])


@dataclass
class SSEEvent:
    """A single SSE event to stream to the client."""

    # text_delta, thinking_delta, tool_use, tool_result, system, result, error, image, file
    event_type: str
    data: Dict[str, Any]


def derive_terminal_info(result_message: Any) -> Dict[str, Any]:
    """Classify how a run ended, from the SDK's ResultMessage.

    The SDK's terminal shapes are asymmetric (verified against the bundled CLI):
    a budget kill is ``subtype="error_max_budget_usd"`` but an unrecoverable API
    failure (e.g. 529 after the CLI's own retries) arrives as
    ``subtype="success"`` with ``is_error=True`` and ``api_error_status`` set —
    so classification MUST branch on ``is_error``/``terminal_reason`` first and
    never on ``subtype`` alone.

    Returns the verbatim SDK fields plus two derived ones:
    - ``kind``: the ONE discriminator frontends branch on —
      success | stopped | budget_exhausted | max_turns | api_error | unknown_error
    - ``reason_message``: server-derived human copy (None for success)
    """
    subtype = getattr(result_message, "subtype", None)
    is_error = bool(getattr(result_message, "is_error", False))
    terminal_reason = getattr(result_message, "terminal_reason", None)
    api_error_status = getattr(result_message, "api_error_status", None)
    errors = getattr(result_message, "errors", None)

    if terminal_reason in ("aborted_streaming", "aborted_tools"):
        kind = "stopped"
        reason_message = "You stopped this response."
    elif subtype == "error_max_budget_usd" or terminal_reason == "budget_exhausted":
        kind = "budget_exhausted"
        reason_message = (
            "This response stopped because the conversation reached its "
            "spending limit. Start a new chat to continue."
        )
    elif subtype == "error_max_turns" or terminal_reason == "max_turns":
        kind = "max_turns"
        reason_message = (
            "The agent reached the maximum number of steps for a single "
            "message. Everything so far is saved — send a follow-up message "
            "to continue."
        )
    elif is_error and api_error_status:
        kind = "api_error"
        if api_error_status == 529:
            reason_message = (
                "The AI service is temporarily overloaded. Please try again "
                "in a moment."
            )
        else:
            reason_message = (
                f"The AI service is temporarily unavailable "
                f"(HTTP {api_error_status}). Please try again in a moment."
            )
    elif is_error:
        kind = "unknown_error"
        reason_message = "The agent stopped unexpectedly."
        if errors:
            reason_message = f"{reason_message} ({errors[0]})"
    else:
        kind = "success"
        reason_message = None

    return {
        "subtype": subtype,
        "is_error": is_error,
        "terminal_reason": terminal_reason,
        "api_error_status": api_error_status,
        "errors": list(errors) if errors else None,
        "kind": kind,
        "reason_message": reason_message,
    }


IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif"}

# Files placed in the sandbox at session creation — skip when scanning for agent output
SANDBOX_SEED_FILES = {"db.py", "silver.py", "eexe_style.py", "report_pdf.py", "skill_schema.md", "skill_queries.md", "skill_domain.md", "skill_sources.md", "skill_methodology.md", "skill_scada.md", "skill_scada_queries.md", "skill_scada_silver.md"}

# Working file extensions — scripts the agent writes to execute, not user-facing output
WORKING_FILE_EXTENSIONS = {".py", ".sh", ".bash", ".sql"}


def _is_bookkeeping_file(name: str) -> bool:
    """Dotfiles (the upload manifest, CLI scratch) are never user-facing output."""
    return name.startswith(".")


@dataclass
class AgentSession:
    """Tracks a Claude Agent SDK session."""

    session_id: str
    user_id: int
    client: ClaudeSDKClient
    created_at: float
    last_activity: float
    is_busy: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    known_files: set = field(default_factory=set)
    has_any_text: bool = False  # tracks if any text_delta was emitted this turn (for dedup)
    # Mirrors the profile so _process_message doesn't need to re-resolve it.
    stream_thinking: bool = False
    # Uploads the agent hasn't been told about yet. Names come from the sandbox
    # manifest, never from a request body — see _consume_pending_uploads.
    pending_uploads: List[str] = field(default_factory=list)
    # EPR-98 duplicate-send guard + detached-run tracking. In-process state only:
    # ECS runs uvicorn --workers 1, so one dict entry per session is
    # authoritative; a multi-worker deployment would need this in Valkey/Postgres.
    current_message_id: Optional[str] = None  # idempotency id of the live run
    last_message_id: Optional[str] = None  # retained after completion (dup-of-completed check)
    current_prompt: Optional[str] = None  # raw prompt of the live run (old-client dedup)
    run_started_at: float = 0.0  # staleness TTL for the busy guard
    detach_task: Optional["asyncio.Task"] = None  # background consumer of an orphaned run


@dataclass
class PersistOutcome:
    """What _persist_completed_run saved, for the caller's result event."""

    messages: List[Dict[str, Any]]
    terminal_info: Dict[str, Any]
    # Populated once delta accounting lands (PR B3); None until then.
    cost_delta_usd: Optional[float] = None
    thread_total_cost_usd: Optional[float] = None


class BrainAgentService:
    """Manages ClaudeSDKClient sessions and streams responses as SSE events."""

    _sessions: Dict[str, AgentSession] = {}
    _prompt_template: Optional[str] = None
    # Serializes first-turn session creation per session_id (EPR-98 path D):
    # without it two concurrent POSTs both spawn a CLI subprocess and orphan one.
    _create_locks: Dict[str, asyncio.Lock] = {}

    def __init__(self, db: AsyncSession):
        self.db = db

    @classmethod
    def check_duplicate_send(
        cls,
        session_id: str,
        message_id: Optional[str],
        prompt: str,
    ) -> bool:
        """True iff this send duplicates the run currently live on the session.

        Used by the chat endpoint to 409 a re-POST (stream-retry storms, double
        clicks, stale tabs) instead of writing the same user turn into the live
        SDK session a second time (EPR-98). A *different* prompt while busy is
        NOT a duplicate — that stays on the interrupt-and-supersede path so
        "cancel, then ask something else" keeps working.
        """
        session = cls._sessions.get(session_id)
        if session is None or not session.is_busy:
            return False
        if time.time() - session.run_started_at > STALE_RUN_SECONDS:
            # Wedged or crashed run — let chat() interrupt-and-drain it.
            return False
        if message_id:
            return message_id == session.current_message_id
        # Old clients (no message_id) can only be matched by the raw prompt.
        return prompt == session.current_prompt

    async def chat(
        self,
        user_id: int,
        session_id: Optional[str],
        prompt: str,
        user_name: Optional[str] = None,
        user_first_name: Optional[str] = None,
        user_company_name: Optional[str] = None,
        model: Optional[str] = None,
        conversation_history: Optional[list] = None,
        source: Optional[str] = None,
        message_id: Optional[str] = None,
    ) -> AsyncGenerator[SSEEvent, None]:
        """Send a prompt to the agent and yield SSE events."""
        if not session_id:
            session_id = str(uuid.uuid4())

        # The raw user prompt, before history/wrapping/attachment rewrites —
        # this is what the endpoint's duplicate guard compares against.
        raw_prompt = prompt
        # Ownership flags for the disconnect handler and finally below. A
        # request that never got past the lock (e.g. a duplicate waiting on a
        # live run) must not clear that run's busy state on its way out.
        started_run = False
        query_sent = False
        got_result = False

        # Clean up stale sessions
        self._cleanup_stale_sessions()

        profile = _get_profile(source)
        wrap_user_input = profile["wrap_user_input"]

        # Audit hook: log every client-source chat invocation. Lets us spot
        # abuse patterns without standing up a separate audit table.
        if source == "client":
            logger.info(
                "client_brain_agent_chat_started",
                user_id=user_id,
                session_id=session_id,
                source=source,
                prompt_length=len(prompt or ""),
            )

        try:
            # Cumulative per-thread budget guard (complements the SDK's per-turn
            # max_budget_usd). Refuse a new turn once the thread's accumulated
            # cost crosses the ceiling, so a long conversation can't rack up
            # unbounded spend. Best-effort — never block a turn on a read error.
            thread_budget = profile.get("max_thread_budget_usd")
            if thread_budget is not None and session_id:
                try:
                    from app.models.agent_thread import AgentThread

                    existing_cost = (
                        await self.db.execute(
                            select(AgentThread.total_cost_usd).where(
                                AgentThread.id == session_id
                            )
                        )
                    ).scalar_one_or_none()
                    if existing_cost is not None and float(existing_cost) >= thread_budget:
                        logger.info(
                            "brain_agent_thread_budget_exceeded",
                            session_id=session_id,
                            user_id=user_id,
                            spent=float(existing_cost),
                            limit=thread_budget,
                        )
                        yield SSEEvent(
                            event_type="error",
                            data={
                                "message": (
                                    f"This conversation has reached its spend limit "
                                    f"(${thread_budget:.2f}). Start a new chat to continue."
                                ),
                                "code": "thread_budget_exceeded",
                                "kind": "budget_exhausted",
                            },
                        )
                        return
                except Exception as budget_exc:
                    logger.warning(
                        "brain_agent_thread_budget_check_failed",
                        session_id=session_id,
                        error=str(budget_exc),
                    )

            session, is_new_session = await self._get_or_create_session(
                user_id,
                session_id,
                user_name,
                model,
                source=source,
                user_first_name=user_first_name,
                user_company_name=user_company_name,
            )

            # When resuming a conversation in a freshly created session,
            # prepend the prior conversation as context so the agent
            # remembers everything that was discussed.
            if is_new_session and conversation_history:
                prompt = self._build_prompt_with_history(
                    prompt, conversation_history, wrap_user_input=wrap_user_input
                )
            elif wrap_user_input:
                # Single-turn client message: still wrap in delimiters so the
                # system prompt's "treat <user_input> as data" rule applies.
                prompt = f"<user_input>\n{prompt}\n</user_input>"

            # Announce any attachments the agent hasn't been told about. Goes in
            # front of the (possibly wrapped) user text so it lands outside
            # <user_input> and is read as instruction — which is why the names
            # come from the sandbox manifest, never from the request body.
            attachment_note = build_attachment_note(session.pending_uploads)
            if attachment_note:
                logger.info(
                    "brain_agent_attachments_announced",
                    session_id=session_id,
                    files=list(session.pending_uploads),
                )
                prompt = f"{attachment_note}\n\n{prompt}"
                session.pending_uploads = []

            async with session.lock:
                # TOCTOU closure for the endpoint's 409 pre-check: a duplicate
                # that raced past it (e.g. while this session was still being
                # created) lands here after the first copy already ran.
                if message_id and message_id == session.last_message_id:
                    yield SSEEvent(
                        event_type="error",
                        data={
                            "message": "This message was already sent.",
                            "code": "duplicate_message",
                        },
                    )
                    return

                # A detached run from a dropped connection may still own the
                # message stream — supersede it before starting a new turn.
                if session.detach_task is not None and not session.detach_task.done():
                    session.detach_task.cancel()
                    try:
                        await session.detach_task
                    except asyncio.CancelledError:
                        pass
                    session.detach_task = None
                    # The cancelled detach interrupted the run but did not
                    # drain it; consume the tail so this turn's receive loop
                    # starts at a clean run boundary.
                    await self._interrupt_and_drain(session)
                elif session.is_busy:
                    # A previous turn was abandoned (e.g. SSE disconnect before
                    # the detach handler existed) — drain leftover messages.
                    logger.warning("brain_agent_draining_previous_turn", session_id=session_id)
                    await self._interrupt_and_drain(session)

                session.is_busy = True
                started_run = True
                session.last_activity = time.time()
                session.has_any_text = False  # Reset text dedup for this turn
                session.current_message_id = message_id
                if message_id:
                    session.last_message_id = message_id
                session.current_prompt = raw_prompt
                session.run_started_at = time.time()

                # Advisory only (never read as a server-side guard): lets the
                # UI poll GET /threads/{id} to see a run is live after its
                # stream dropped. Cleared by every thread save.
                await self._set_thread_streaming(session_id, True)

                # Yield session_id so frontend knows it
                yield SSEEvent(
                    event_type="session",
                    data={"session_id": session_id},
                )

                # Signal frontend: thinking phase
                yield SSEEvent(
                    event_type="status",
                    data={"phase": "thinking"},
                )

                # Send the query
                await session.client.query(prompt)
                query_sent = True

                # Stream response messages
                result_message = None
                async for message in session.client.receive_messages():
                    async for event in self._process_message(message, session):
                        yield event

                    # ResultMessage means the agent is done
                    if isinstance(message, ResultMessage):
                        result_message = message
                        got_result = True
                        break

                if got_result and result_message:
                    outcome = await self._persist_completed_run(
                        user_id=user_id,
                        session_id=session_id,
                        result_message=result_message,
                        model=model,
                        session=session,
                        detached=False,
                    )
                    terminal_info = outcome.terminal_info
                    final_messages = outcome.messages

                    # Yield result with authoritative messages. The legacy keys
                    # (num_turns/duration_ms/cost_usd/session_id/messages) are
                    # load-bearing for both frontend twins — additive only.
                    yield SSEEvent(
                        event_type="result",
                        data={
                            "num_turns": result_message.num_turns if hasattr(result_message, "num_turns") else 0,
                            "duration_ms": result_message.duration_ms if hasattr(result_message, "duration_ms") else 0,
                            "cost_usd": result_message.total_cost_usd if hasattr(result_message, "total_cost_usd") else None,
                            "session_id": session_id,
                            "messages": final_messages,
                            "subtype": terminal_info["subtype"],
                            "is_error": terminal_info["is_error"],
                            "terminal_reason": terminal_info["terminal_reason"],
                            "api_error_status": terminal_info["api_error_status"],
                            "errors": terminal_info["errors"],
                            "kind": terminal_info["kind"],
                            "reason_message": terminal_info["reason_message"],
                        },
                    )
                else:
                    # Stream ended without a ResultMessage — emit a synthetic result
                    logger.warning(
                        "brain_agent_stream_ended_without_result",
                        session_id=session_id,
                    )
                    logger.info(
                        "brain_agent_run_finished",
                        session_id=session_id,
                        user_id=user_id,
                        source=source,
                        detached=False,
                        subtype="incomplete",
                        is_error=True,
                        terminal_reason="stream_ended_without_result",
                        api_error_status=None,
                        kind="unknown_error",
                        num_turns=0,
                        duration_ms=0,
                        process_total_cost_usd=None,
                    )
                    await self._set_thread_streaming(session_id, False)
                    yield SSEEvent(
                        event_type="result",
                        data={
                            "num_turns": 0,
                            "duration_ms": 0,
                            "cost_usd": None,
                            "session_id": session_id,
                            "incomplete": True,
                            "subtype": "incomplete",
                            "is_error": True,
                            "terminal_reason": "stream_ended_without_result",
                            "api_error_status": None,
                            "errors": None,
                            "kind": "unknown_error",
                            "reason_message": (
                                "The connection to the agent ended before it "
                                "finished. The conversation so far has been saved."
                            ),
                        },
                    )

        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected mid-run (tab closed, network drop, abandoned
            # stream). Don't orphan the run: hand consumption of the live
            # subprocess to a background task that drains it to the
            # ResultMessage and persists the thread, so a reload or poll finds
            # the finished answer — and so the NEXT request can never eat this
            # run's buffered messages (EPR-98).
            live_session = self._sessions.get(session_id)
            if query_sent and not got_result and live_session is not None:
                live_session.detach_task = asyncio.create_task(
                    self._finish_orphaned_run(live_session, model=model)
                )
                logger.info("brain_agent_run_detached", session_id=session_id)
            raise

        except Exception as e:
            logger.error("brain_agent_error", error=str(e), session_id=session_id)
            if source == "client":
                logger.info(
                    "client_brain_agent_chat_finished",
                    user_id=user_id,
                    session_id=session_id,
                    outcome="errored",
                    error=str(e),
                )
            if started_run:
                await self._set_thread_streaming(session_id, False)
            yield SSEEvent(
                event_type="error",
                data={"message": str(e), "code": "agent_error", "kind": "unknown_error"},
            )
        else:
            if source == "client":
                logger.info(
                    "client_brain_agent_chat_finished",
                    user_id=user_id,
                    session_id=session_id,
                    outcome="completed",
                )
        finally:
            # Clear busy state ONLY if this generator owns the run and hasn't
            # handed it to a detach task. A request that exited while waiting on
            # the lock (started_run False) must not clear another run's state —
            # that was the original EPR-98 hole.
            s = self._sessions.get(session_id)
            if (
                started_run
                and s is not None
                and (s.detach_task is None or s.detach_task.done())
            ):
                s.is_busy = False
                s.current_message_id = None
                s.current_prompt = None

    @staticmethod
    def _convert_sdk_messages(sdk_messages: List[SessionMessage]) -> List[Dict[str, Any]]:
        """Convert SDK SessionMessage list to our AgentMessage format.

        Each SessionMessage has:
        - type: "user" or "assistant"
        - uuid: unique message ID
        - session_id: session ID
        - message: raw Anthropic API message dict with role and content blocks
        """
        messages: List[Dict[str, Any]] = []

        for sm in sdk_messages:
            raw_msg = sm.message
            if not raw_msg:
                continue

            raw_content = raw_msg.get("content", []) if isinstance(raw_msg, dict) else []
            # Content can be a plain string or a list of blocks
            if isinstance(raw_content, str):
                content_blocks = [{"type": "text", "text": raw_content}]
            else:
                content_blocks = raw_content if isinstance(raw_content, list) else []

            if sm.type == "user":
                # Extract text content from user message
                text_parts = []
                for block in content_blocks:
                    if isinstance(block, str):
                        text_parts.append(block)
                    elif isinstance(block, dict):
                        if block.get("type") == "text":
                            text_parts.append(block.get("text", ""))
                        elif block.get("type") == "tool_result":
                            # Tool results are part of user messages in Anthropic API format;
                            # we attach them to the preceding assistant message's toolCalls below.
                            pass

                content = "\n\n".join(text_parts).strip()
                if content:
                    messages.append({
                        "id": sm.uuid,
                        "type": "user",
                        "content": content,
                        "timestamp": int(time.time() * 1000),
                    })

            elif sm.type == "assistant":
                # Extract text and tool calls from assistant message
                text_parts = []
                tool_calls = []

                for block in content_blocks:
                    if isinstance(block, dict):
                        block_type = block.get("type", "")
                        if block_type == "text":
                            text_parts.append(block.get("text", ""))
                        elif block_type == "tool_use":
                            tool_calls.append({
                                "tool_name": block.get("name", ""),
                                "tool_id": block.get("id", ""),
                                "input": block.get("input", {}),
                                "isLoading": False,
                            })

                # Double newline: single "\n" is a markdown soft-break and the
                # blocks are separate paragraphs (typically split by tool use).
                content = "\n\n".join(text_parts).strip()

                # The transcript records each content block as its own assistant
                # entry, so an extended-thinking turn produces a thinking-only
                # entry with no text and no tool_use. Persisting it renders an
                # empty bubble in the UI. Thinking content is encrypted and
                # never displayed, so drop those entries entirely.
                if not content and not tool_calls:
                    continue

                msg: Dict[str, Any] = {
                    "id": sm.uuid,
                    "type": "assistant",
                    "content": content,
                    "timestamp": int(time.time() * 1000),
                }
                if tool_calls:
                    msg["toolCalls"] = tool_calls

                messages.append(msg)

        # Second pass: attach tool results from user messages to the corresponding
        # assistant tool calls.
        for i, sm in enumerate(sdk_messages):
            if sm.type != "user":
                continue
            raw_msg = sm.message
            if not raw_msg or not isinstance(raw_msg, dict):
                continue
            content_blocks = raw_msg.get("content", [])
            for block in content_blocks:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    tool_use_id = block.get("tool_use_id", "")
                    result_content = block.get("content", "")
                    if isinstance(result_content, list):
                        result_content = " ".join(
                            b.get("text", "") if isinstance(b, dict) else str(b)
                            for b in result_content
                        )
                    is_error = block.get("is_error", False)
                    # Find the matching tool call in a preceding assistant message
                    for msg in messages:
                        if msg.get("type") == "assistant":
                            for tc in msg.get("toolCalls", []):
                                if tc.get("tool_id") == tool_use_id:
                                    full_length = len(str(result_content))
                                    is_truncated = full_length > 2000
                                    tc["result"] = (str(result_content)[:2000] + "...") if is_truncated else str(result_content)
                                    tc["isError"] = is_error
                                    tc["isLoading"] = False

        return BrainAgentService._drop_consecutive_duplicates(messages)

    @staticmethod
    def _drop_consecutive_duplicates(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Collapse adjacent identical messages (EPR-98 residue).

        The duplicate-send storm wrote the same user turn into a session twice
        back-to-back; transcripts persisted before the guard landed still carry
        those pairs. Adjacent same-type same-content messages never occur in a
        legitimate chat (an assistant reply always sits between two user
        turns), so collapsing them is safe. Messages carrying toolCalls are
        never dropped.
        """
        deduped: List[Dict[str, Any]] = []
        for msg in messages:
            prev = deduped[-1] if deduped else None
            if (
                prev is not None
                and msg.get("type") == prev.get("type")
                and (msg.get("content") or "") == (prev.get("content") or "")
                and not msg.get("toolCalls")
                and not prev.get("toolCalls")
            ):
                continue
            deduped.append(msg)
        return deduped

    async def _save_thread_to_db(
        self,
        session_id: str,
        user_id: int,
        messages: List[Dict[str, Any]],
        model: Optional[str] = None,
        cost_usd: Any = None,
        num_turns: int = 0,
    ):
        """Save authoritative messages to the agent thread in DB (create or update)."""
        from app.models.agent_thread import AgentThread

        try:
            result = await self.db.execute(
                select(AgentThread).where(AgentThread.id == session_id)
            )
            thread = result.scalar_one_or_none()

            # Derive title from first user message
            title = None
            first_user = next((m for m in messages if m.get("type") == "user"), None)
            if first_user:
                title = first_user.get("content", "")[:80]

            if thread:
                thread.messages = messages
                thread.message_count = len(messages)
                thread.is_streaming = False
                if not thread.title and title:
                    thread.title = title
                if cost_usd is not None:
                    thread.total_cost_usd = cost_usd
                if num_turns:
                    thread.total_turns = (thread.total_turns or 0) + num_turns
            else:
                thread = AgentThread(
                    id=session_id,
                    user_id=user_id,
                    title=title,
                    model=model,
                    messages=messages,
                    message_count=len(messages),
                    is_streaming=False,
                    total_cost_usd=cost_usd,
                    total_turns=num_turns or 0,
                )
                self.db.add(thread)

            await self.db.commit()
            logger.info(
                "brain_agent_thread_saved",
                thread_id=session_id,
                message_count=len(messages),
            )
        except Exception as e:
            logger.error("brain_agent_save_thread_error", error=str(e), thread_id=session_id)
            try:
                await self.db.rollback()
            except Exception:
                pass

    async def _persist_completed_run(
        self,
        user_id: int,
        session_id: str,
        result_message: Any,
        model: Optional[str] = None,
        session: Optional[AgentSession] = None,
        detached: bool = False,
    ) -> PersistOutcome:
        """Read the authoritative SDK transcript for a finished run and save it.

        Shared by the live chat() path and the detached (client-disconnected)
        path. On a non-success terminal (budget kill, turn cap, API failure,
        interrupt) a terminal marker message is appended so a reloaded thread
        explains why the run ended instead of trailing off after a tool call.
        """
        terminal_info = derive_terminal_info(result_message)
        logger.info(
            "brain_agent_run_finished",
            session_id=session_id,
            user_id=user_id,
            detached=detached,
            subtype=terminal_info["subtype"],
            is_error=terminal_info["is_error"],
            terminal_reason=terminal_info["terminal_reason"],
            api_error_status=terminal_info["api_error_status"],
            kind=terminal_info["kind"],
            num_turns=getattr(result_message, "num_turns", 0),
            duration_ms=getattr(result_message, "duration_ms", 0),
            process_total_cost_usd=getattr(result_message, "total_cost_usd", None),
        )

        # Use the SDK's internal session_id (from ResultMessage), NOT our
        # session_id — they are different.
        sdk_session_id = (
            result_message.session_id
            if hasattr(result_message, "session_id")
            else session_id
        )
        work_dir = Path(f"/tmp/brain-agent/{user_id}/{session_id}")
        final_messages: List[Dict[str, Any]] = []
        # The CLI awaits the final transcript append (assistant tail included)
        # BEFORE emitting the ResultMessage, so on a clean success the tail is
        # normally already there — the short retry below only covers filesystem
        # lag. On an error terminal the transcript legitimately ends on a
        # tool_result (user-type) entry, so waiting for an assistant tail would
        # spin for nothing: read once and move on.
        attempts = 4 if terminal_info["kind"] == "success" else 1
        for attempt in range(attempts):
            try:
                sdk_messages = get_session_messages(
                    session_id=sdk_session_id,
                    directory=str(work_dir),
                )
                final_messages = self._convert_sdk_messages(sdk_messages)
            except Exception as e:
                logger.error("brain_agent_get_session_messages_error", error=str(e), session_id=session_id)
                final_messages = []
            if final_messages and final_messages[-1].get("type") == "assistant":
                break
            if attempt < attempts - 1:
                await asyncio.sleep(0.5)
        else:
            if terminal_info["kind"] == "success":
                logger.warning(
                    "brain_agent_transcript_missing_assistant_tail",
                    session_id=session_id,
                    message_count=len(final_messages),
                    kind=terminal_info["kind"],
                    terminal_reason=terminal_info["terminal_reason"],
                )

        if terminal_info["kind"] != "success":
            marker = self._terminal_marker(result_message, terminal_info)
            if final_messages:
                final_messages.append(marker)
            else:
                # Transcript read failed on an error terminal. Never replace
                # the thread's stored messages with a lone marker — append the
                # marker to whatever is already saved (or create the thread
                # with just the marker on a failed very first turn). Saving
                # also clears the advisory is_streaming flag.
                final_messages = [*await self._load_thread_messages(session_id), marker]

        if final_messages:
            await self._save_thread_to_db(
                session_id=session_id,
                user_id=user_id,
                messages=final_messages,
                model=model,
                cost_usd=result_message.total_cost_usd if hasattr(result_message, "total_cost_usd") else None,
                num_turns=result_message.num_turns if hasattr(result_message, "num_turns") else 0,
            )
        return PersistOutcome(messages=final_messages, terminal_info=terminal_info)

    async def _load_thread_messages(self, session_id: str) -> List[Dict[str, Any]]:
        """Best-effort read of a thread's stored messages (empty on any error)."""
        try:
            from app.models.agent_thread import AgentThread

            result = await self.db.execute(
                select(AgentThread.messages).where(AgentThread.id == session_id)
            )
            msgs = result.scalar_one_or_none()
            return list(msgs) if msgs else []
        except Exception as e:
            logger.warning(
                "brain_agent_thread_messages_read_failed",
                session_id=session_id,
                error=str(e),
            )
            try:
                await self.db.rollback()
            except Exception:
                pass
            return []

    @staticmethod
    def _terminal_marker(
        result_message: Any, terminal_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the terminal marker appended as a thread's final message.

        Shape notes (coordinated with both frontend twins):
        - non-empty ``content`` is load-bearing: it adds assistant text so the
          client's authText>=localText adoption logic takes the authoritative
          list over its partial local copy;
        - no ``toolCalls`` key, so the dedup and tool-result passes in
          _convert_sdk_messages never touch it (it is appended after
          conversion anyway);
        - the marker survives only while it is the thread's tail — the next
          successful turn rebuilds messages from the SDK transcript, which
          never contained it. That is exactly the window where it matters.
        """
        return {
            "id": f"terminal-{getattr(result_message, 'uuid', None) or uuid.uuid4()}",
            "type": "assistant",
            "content": terminal_info["reason_message"] or "The agent stopped unexpectedly.",
            "timestamp": int(time.time() * 1000),
            "terminal": {
                "kind": terminal_info["kind"],
                "subtype": terminal_info["subtype"],
                "is_error": terminal_info["is_error"],
                "terminal_reason": terminal_info["terminal_reason"],
                "api_error_status": terminal_info["api_error_status"],
            },
        }

    async def _finish_orphaned_run(self, session: AgentSession, model: Optional[str] = None) -> None:
        """Consume a disconnected turn to completion and persist the result.

        Runs as a background task after the SSE consumer vanished mid-run
        (EPR-98). The CLI subprocess keeps working regardless; without this its
        buffered messages would be eaten by the NEXT request's receive loop and
        the finished answer would never reach the thread. Exactly one consumer
        owns receive_messages() at any moment: the live chat() loop, this task,
        or _interrupt_and_drain.
        """
        session_id = session.session_id
        result_message = None
        try:
            try:
                async with asyncio.timeout(DETACH_MAX_SECONDS):
                    async for message in session.client.receive_messages():
                        if isinstance(message, ResultMessage):
                            result_message = message
                            break
            except asyncio.TimeoutError:
                # Run never finished — stop it and drain to a clean boundary so
                # the next turn doesn't inherit its tail.
                logger.warning("brain_agent_detached_run_timeout", session_id=session_id)
                await self._interrupt_and_drain(session)
                return

            if result_message is None:
                return

            # The request-scoped DB session that started this run is gone —
            # persist through a fresh one.
            from app.core.database import get_session_factory

            async with get_session_factory()() as db:
                svc = BrainAgentService(db)
                await svc._persist_completed_run(
                    user_id=session.user_id,
                    session_id=session_id,
                    result_message=result_message,
                    model=model,
                    session=session,
                    detached=True,
                )

            # Push any files the run produced to S3. There is no SSE consumer
            # to announce them; they surface via the thread's file URLs.
            for fname in self._scan_for_new_files(session):
                file_path = Path(f"/tmp/brain-agent/{session.user_id}/{session_id}") / fname
                await self._upload_file_to_s3(session.user_id, session_id, fname, file_path)

            logger.info("brain_agent_detached_run_saved", session_id=session_id)
        except asyncio.CancelledError:
            # Superseded by a new turn (which interrupts and drains after
            # cancelling us) or process shutdown. Best-effort stop.
            try:
                await session.client.interrupt()
            except Exception:
                pass
            raise
        except Exception as e:
            logger.error("brain_agent_detached_finish_error", session_id=session_id, error=str(e))
        finally:
            session.is_busy = False
            session.current_message_id = None
            session.current_prompt = None
            session.detach_task = None
            session.last_activity = time.time()

    async def _set_thread_streaming(self, session_id: str, value: bool) -> None:
        """Best-effort advisory flag so the UI can poll a thread for a live run.

        Never read as a server-side guard (the in-memory session state gates
        duplicate sends), so a crash that leaves it True cannot wedge a thread.
        The row may not exist yet on a thread's first turn — that's fine.
        """
        try:
            from app.models.agent_thread import AgentThread

            await self.db.execute(
                update(AgentThread)
                .where(AgentThread.id == session_id)
                .values(is_streaming=value)
            )
            await self.db.commit()
        except Exception:
            try:
                await self.db.rollback()
            except Exception:
                pass

    async def _interrupt_and_drain(self, session: AgentSession):
        """Interrupt any in-flight agent work and consume remaining buffered messages."""
        try:
            await session.client.interrupt()
        except Exception as e:
            logger.warning("brain_agent_interrupt_during_drain", error=str(e))
        try:
            async with asyncio.timeout(30):
                async for msg in session.client.receive_messages():
                    if isinstance(msg, ResultMessage):
                        break
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning("brain_agent_drain_timeout", error=str(e))

    async def interrupt(self, session_id: str, user_id: int) -> bool:
        """Interrupt the current agent task. Validates session ownership."""
        session = self._sessions.get(session_id)
        if session and session.user_id == user_id and session.is_busy:
            try:
                await session.client.interrupt()
                return True
            except Exception as e:
                logger.error("brain_agent_interrupt_error", error=str(e))
        return False

    async def end_session(self, session_id: str, user_id: int) -> bool:
        """End and clean up a session. Validates session ownership."""
        session = self._sessions.get(session_id)
        if session and session.user_id == user_id:
            self._sessions.pop(session_id, None)
            await self._destroy_session(session)
            return True
        return False

    def list_sessions(self, user_id: int) -> list:
        """List active sessions for a user."""
        return [
            {
                "session_id": s.session_id,
                "created_at": s.created_at,
                "last_activity": s.last_activity,
                "is_busy": s.is_busy,
            }
            for s in self._sessions.values()
            if s.user_id == user_id
        ]

    async def _scada_schema_present(self) -> bool:
        """True iff the SCADA gold schema exists in the connected database.

        Gates the scada skill files + prompt lines: present on staging today,
        on prod only after the scada prod cut, absent in local dev unless the
        pipeline has published there. Best effort — session creation must
        never fail on this check.
        """
        try:
            result = await self.db.execute(
                text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'scada' AND table_name = 'dim_farm' "
                    "LIMIT 1"
                )
            )
            return result.scalar() is not None
        except Exception as exc:
            logger.warning("brain_agent_scada_check_failed", error=str(exc))
            return False

    async def _get_or_create_session(
        self,
        user_id: int,
        session_id: str,
        user_name: Optional[str] = None,
        model: Optional[str] = None,
        source: Optional[str] = None,
        user_first_name: Optional[str] = None,
        user_company_name: Optional[str] = None,
    ) -> tuple[AgentSession, bool]:
        """Get existing session or create a new one. Returns (session, is_new)."""
        existing = self._existing_live_session(session_id)
        if existing is not None:
            return existing, False

        # EPR-98: serialize creation per session_id — two concurrent first-turn
        # POSTs would otherwise both spawn a CLI subprocess and orphan one.
        create_lock = self._create_locks.setdefault(session_id, asyncio.Lock())
        try:
            async with create_lock:
                # Re-check under the lock: another request may have finished
                # creating this session while we waited.
                existing = self._existing_live_session(session_id)
                if existing is not None:
                    return existing, False
                session = await self._create_session(
                    user_id,
                    session_id,
                    user_name,
                    model,
                    source=source,
                    user_first_name=user_first_name,
                    user_company_name=user_company_name,
                )
                return session, True
        finally:
            # Safe to drop: any waiter still holds its own reference to the
            # lock object, and late arrivals hit the fast path above.
            self._create_locks.pop(session_id, None)

    def _existing_live_session(self, session_id: str) -> Optional[AgentSession]:
        """Return the in-memory session iff its sandbox still exists on disk.

        The sandbox can be lost to container restart, tmpwatch, or manual
        cleanup — then the stale in-memory session is discarded so the caller
        recreates it.
        """
        if session_id not in self._sessions:
            return None
        existing = self._sessions[session_id]
        work_dir_check = Path(f"/tmp/brain-agent/{existing.user_id}/{existing.session_id}")
        if work_dir_check.exists():
            return existing
        logger.warning("brain_agent_session_workdir_missing", session_id=session_id)
        self._sessions.pop(session_id, None)
        return None

    async def _create_session(
        self,
        user_id: int,
        session_id: str,
        user_name: Optional[str] = None,
        model: Optional[str] = None,
        source: Optional[str] = None,
        user_first_name: Optional[str] = None,
        user_company_name: Optional[str] = None,
    ) -> AgentSession:
        """Build the sandbox, options and SDK client for a brand-new session."""
        # Enforce session limit
        user_sessions = [s for s in self._sessions.values() if s.user_id == user_id]
        if len(user_sessions) >= MAX_CONCURRENT_SESSIONS:
            # Remove oldest
            oldest = min(user_sessions, key=lambda s: s.last_activity)
            self._sessions.pop(oldest.session_id, None)

        # Create temp working directory
        work_dir = Path(f"/tmp/brain-agent/{user_id}/{session_id}")
        work_dir.mkdir(parents=True, exist_ok=True)

        # Pull back any attachments this thread already had. The sandbox is on a
        # Fargate task's ephemeral disk, so a deploy wipes it while the thread
        # lives on in the DB — without this, a follow-up question about an
        # uploaded file would find nothing there.
        await self._restore_uploads_from_s3(user_id, session_id, work_dir)

        settings = get_settings()
        profile = _get_profile(source)

        # Resolve source code repo paths (for read-only code access).
        # EPR-59: the client surface gets NO codebase access — clients must not
        # be able to read source and thereby reconstruct DB schema / internals.
        from app.services.brain_agent_repo_manager import get_repo_dirs
        repo_dirs_str = [] if source == "client" else get_repo_dirs()

        # SCADA gold layer (schema `scada`): admin surface only, and only when
        # the schema actually exists in the connected DB (staging today; prod
        # only after the scada prod cut) — the agent must never be taught
        # tables it can't query.
        scada_enabled = source != "client" and await self._scada_schema_present()

        system_prompt = self._build_system_prompt(
            user_name,
            repo_dirs=repo_dirs_str,
            prompt_file=profile["system_prompt_file"],
            user_first_name=user_first_name,
            user_company_name=user_company_name,
            user_id=user_id,
            scada_enabled=scada_enabled,
            silver_enabled=bool(settings.SCADA_SILVER_URI),
        )

        # Write db.py helper script and skill files to sandbox
        (work_dir / "db.py").write_text(DB_HELPER_SCRIPT)
        # #161 — platform chart theme module; `import eexe_style` in any chart
        # script applies the EnergyExe palette/design automatically.
        (work_dir / "eexe_style.py").write_text(CHART_STYLE_PY)
        # report_pdf.py — branded PDF report builder (EPR-68)
        (work_dir / "report_pdf.py").write_text(REPORT_PDF_PY)
        # Skill files carry a seeded: stamp so the agent can tell that a
        # resumed conversation's earlier read may be stale (content evolves
        # between deployments; the system prompt tells it to re-read).
        seed_stamp = (
            f"<!-- seeded: {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC — "
            "re-seeded fresh at every session start; content may have changed "
            "since earlier turns of a resumed conversation -->\n"
        )
        (work_dir / "skill_schema.md").write_text(seed_stamp + SKILL_SCHEMA)
        (work_dir / "skill_queries.md").write_text(seed_stamp + SKILL_QUERIES)
        (work_dir / "skill_domain.md").write_text(seed_stamp + SKILL_DOMAIN)
        (work_dir / "skill_sources.md").write_text(seed_stamp + SKILL_SOURCES)
        if scada_enabled:
            (work_dir / "skill_scada.md").write_text(seed_stamp + SKILL_SCADA)
            (work_dir / "skill_scada_queries.md").write_text(seed_stamp + SKILL_SCADA_QUERIES)
            # Silver Parquet lake access (10-min measurements, raw alarms):
            # DuckDB-over-S3 helper + its skill file. Gated on the same
            # scada_enabled flag plus an explicit URI (empty = off switch).
            if settings.SCADA_SILVER_URI:
                (work_dir / "silver.py").write_text(SILVER_HELPER_SCRIPT)
                (work_dir / "skill_scada_silver.md").write_text(seed_stamp + SKILL_SCADA_SILVER)

        # DB-driven methodology (client-ui #177): compose the admin-editable
        # methodology sections into a skill file so the agent answers
        # "how is X computed?" from the same text shown to clients. Best
        # effort — agent startup must never fail on this.
        try:
            from app.services.methodology_section import MethodologySectionService

            methodology_md = await MethodologySectionService.compose_markdown(self.db)
            if methodology_md:
                (work_dir / "skill_methodology.md").write_text(seed_stamp + methodology_md)
        except Exception as exc:
            logger.warning(
                "brain_agent_methodology_skill_failed",
                session_id=session_id,
                error=str(exc),
            )

        def _on_stderr(line: str):
            logger.warning("brain_agent_stderr", session_id=session_id, line=line.rstrip())

        # Resolve model: client profile locks the model regardless of caller request.
        if profile["model_locked"]:
            resolved_model = profile["model_default"]
        else:
            resolved_model = (
                model
                or profile["model_default"]
                or getattr(settings, "BRAIN_MODEL", DEFAULT_BRAIN_MODEL)
            )

        # Extended thinking. Sonnet 5 / Opus 5 (and Opus 4.7+/Fable 5) reject
        # thinking.type=enabled with a 400 and require adaptive, where the model
        # decides per-turn how much to reason. claude-agent-sdk 0.1.48 bundled
        # CLI 2.1.71, which had no way to emit adaptive — it silently downgraded
        # it to `--max-thinking-tokens 32000` — so those models had to run with
        # thinking off. SDK 0.2.128 bundles CLI 2.1.220, which passes
        # `--thinking adaptive` through, so thinking is back on for every model
        # in ALLOWED_BRAIN_MODELS. Verified 2026-07-30 against the live API with
        # the 2.1.220 bundled CLI: Sonnet 5, Opus 5, Sonnet 4.6 and Opus 4.6 all
        # succeed on adaptive, and Sonnet 5 / Opus 5 emit thinking blocks on a
        # multi-step analysis prompt (and skip them on trivial ones — that's the
        # point of adaptive).
        #
        # display="summarized" is what makes the reasoning readable — without it
        # the thinking blocks still arrive but their text is encrypted (verified
        # empirically: 0 chars by default, ~2k chars of prose with it). Only the
        # admin profile asks for it; the client agent thinks just as hard, we
        # just never see it.
        thinking_config: Dict[str, Any] = {"type": "adaptive"}
        if profile["stream_thinking"]:
            thinking_config["display"] = "summarized"

        # Strict read-only DB access for the agent process:
        #   1. Prefer the dedicated `brain_agent_ro` Postgres role — it has
        #      only SELECT grants, so the server rejects any write attempt
        #      regardless of which client the agent's bash spawns.
        #   2. Fall back to the main URL + PGOPTIONS session-level
        #      `default_transaction_read_only=on` if the role's password
        #      isn't configured yet.
        # EPR-59: client sessions connect with the locked-down client RO role
        # (SELECT on allowlisted tables only), so even a self-written psycopg2
        # connection can't read internal-table data or enumerate them via
        # information_schema. Fall back to the shared RO role, then PGOPTIONS.
        if source == "client":
            agent_db_url = (
                settings.database_url_agent_client_ro
                or settings.database_url_agent_ro
                or settings.database_url_sync
            )
            if not settings.database_url_agent_client_ro:
                logger.warning(
                    "brain_agent_client_ro_role_not_configured",
                    msg="BRAIN_AGENT_CLIENT_RO_PASSWORD is unset — client session "
                    "falling back to the shared RO role / PGOPTIONS guard.",
                )
        else:
            agent_db_url = settings.database_url_agent_ro or settings.database_url_sync
            if not settings.database_url_agent_ro:
                logger.warning(
                    "brain_agent_ro_role_not_configured",
                    msg="BRAIN_AGENT_RO_PASSWORD is unset — falling back to PGOPTIONS read-only enforcement.",
                )

        options = ClaudeAgentOptions(
            system_prompt=system_prompt,
            allowed_tools=[
                "Bash",
                "WebSearch",
                "WebFetch",
                "Read",
                "Glob",
                "Grep",
            ],
            disallowed_tools=[
                "ToolSearch",
                "TodoWrite",
                "Agent",
                "EnterPlanMode",
                "ExitPlanMode",
                "AskUserQuestion",
                "Skill",
                "NotebookEdit",
                "Write",
                "Edit",
            ],
            cwd=work_dir,
            add_dirs=repo_dirs_str,
            max_turns=profile["max_turns"],
            max_budget_usd=profile["max_budget_usd"],
            task_budget={"total": profile["task_budget_tokens"]},
            permission_mode="bypassPermissions",
            model=resolved_model,
            thinking=thinking_config,
            # Fires even under bypassPermissions (can_use_tool does not), so this
            # is where a Bash command can be inspected before it runs.
            hooks={
                "PreToolUse": [
                    HookMatcher(
                        matcher="Bash",
                        hooks=[make_pre_tool_use_hook(source, session_id=session_id)],
                    )
                ]
            },
            stderr=_on_stderr,
            max_buffer_size=10 * 1024 * 1024,
            setting_sources=[],  # Don't inherit global MCP servers (Gmail, Slack, etc.)
            mcp_servers={},  # No MCP servers needed for brain agent
            # setting_sources/mcp_servers above express the intent; this enforces
            # it — the CLI otherwise still tries to load .mcp.json, plugins and
            # claude.ai connectors (it logs that attempt to stderr on every run).
            strict_mcp_config=True,
            include_partial_messages=True,
            env={
                "DATABASE_URL": agent_db_url,
                # Belt-and-suspenders — forces every transaction to be
                # read-only at the session level even if the role somehow
                # gained write grants.
                "PGOPTIONS": "-c default_transaction_read_only=on",
                # EPR-59: client db.py rejects information_schema / pg_catalog
                # introspection so the client agent can't describe DB structure.
                # The PreToolUse hook above enforces the same rule for commands
                # that never touch db.py.
                "BRAIN_AGENT_BLOCK_INTROSPECTION": "1" if source == "client" else "0",
                # Silver Parquet lake root for silver.py (empty when disabled;
                # the helper isn't seeded then either).
                "SCADA_SILVER_URI": settings.SCADA_SILVER_URI if scada_enabled else "",
                "CLAUDE_CODE_STREAM_CLOSE_TIMEOUT": "1200000",  # 20 min (was 10)
                # Raise the CLI's API retry count (default ~10, hard clamp 15).
                # A 529-overload storm then costs extra in-outage latency
                # instead of killing the run; the terminal kind="api_error"
                # mapping covers the truly unrecoverable case.
                "CLAUDE_CODE_MAX_RETRIES": "15",
                "CLAUDECODE": "",  # Unset to prevent nested session detection
            },
        )

        client = ClaudeSDKClient(options=options)
        # Enter the async context manager
        await client.__aenter__()

        # Pre-populate known_files with existing files so old outputs
        # from prior turns aren't re-sent on a recreated session.
        existing_files = set()
        if work_dir.exists():
            for f in work_dir.iterdir():
                if (
                    f.is_file()
                    and f.name not in SANDBOX_SEED_FILES
                    and not _is_bookkeeping_file(f.name)
                ):
                    existing_files.add(f.name)

        session = AgentSession(
            session_id=session_id,
            user_id=user_id,
            client=client,
            created_at=time.time(),
            last_activity=time.time(),
            known_files=existing_files,
            stream_thinking=profile["stream_thinking"],
            # A recreated session has lost the SDK transcript, so re-announce
            # every upload rather than tracking which ones were mentioned before.
            pending_uploads=read_upload_manifest(work_dir),
        )
        self._sessions[session_id] = session
        return session

    def _scan_for_new_files(self, session: AgentSession) -> list:
        """Scan the session sandbox for new agent-generated output files (images, CSVs, etc.)."""
        work_dir = Path(f"/tmp/brain-agent/{session.user_id}/{session.session_id}")
        new_files = []
        if work_dir.exists():
            for f in work_dir.iterdir():
                if (
                    f.is_file()
                    and f.name not in session.known_files
                    and f.name not in SANDBOX_SEED_FILES
                    and not _is_bookkeeping_file(f.name)
                    and f.suffix.lower() not in WORKING_FILE_EXTENSIONS
                ):
                    session.known_files.add(f.name)
                    new_files.append(f.name)
        return new_files

    def register_upload(self, user_id: int, session_id: str, filename: str) -> None:
        """Tell a live session about a file the user just attached.

        Two jobs: queue the announcement for the next turn, and claim the name in
        known_files so _scan_for_new_files doesn't hand the user's own upload back
        to them as an agent-generated download. When no live session exists yet
        (attaching before the first message) neither is needed — session creation
        reads the manifest off disk.
        """
        session = self._sessions.get(session_id)
        if not session or session.user_id != user_id:
            return
        session.known_files.add(filename)
        if filename not in session.pending_uploads:
            session.pending_uploads.append(filename)

    @staticmethod
    async def _upload_file_to_s3(user_id: int, thread_id: str, filename: str, file_path: Path):
        """Upload a file to S3 for permanent storage. Failures are logged, not raised."""
        try:
            from app.services.s3_service import upload_file

            key = f"brain-agent/{user_id}/{thread_id}/{Path(filename).name}"
            await upload_file(key, file_path)
        except Exception as e:
            logger.error("brain_agent_file_upload_failed", error=str(e), filename=filename)

    @staticmethod
    async def _restore_uploads_from_s3(user_id: int, session_id: str, work_dir: Path) -> List[str]:
        """Restore this thread's attachments into a fresh sandbox from S3.

        Uses the same flat key shape as agent outputs, so the manifest is all
        that's needed to know what to fetch — no bucket listing. Best effort:
        a thread whose files are gone must still be able to chat.
        """
        from app.services.s3_service import download_file

        prefix = f"brain-agent/{user_id}/{session_id}"
        try:
            manifest_bytes = await download_file(f"{prefix}/{UPLOAD_MANIFEST}")
            if manifest_bytes is None:
                return []
            (work_dir / UPLOAD_MANIFEST).write_bytes(manifest_bytes)
        except Exception as exc:
            logger.warning(
                "brain_agent_upload_manifest_restore_failed",
                session_id=session_id,
                error=str(exc),
            )
            return []

        restored: List[str] = []
        for name in read_upload_manifest(work_dir):
            target = work_dir / name
            if target.exists():
                restored.append(name)
                continue
            try:
                data = await download_file(f"{prefix}/{name}")
                if data is None:
                    continue
                target.write_bytes(data)
                restored.append(name)
            except Exception as exc:
                logger.warning(
                    "brain_agent_upload_restore_failed",
                    session_id=session_id,
                    filename=name,
                    error=str(exc),
                )

        if restored:
            logger.info(
                "brain_agent_uploads_restored", session_id=session_id, files=restored
            )
        return restored

    async def _process_message(self, message, session: AgentSession = None) -> AsyncGenerator[SSEEvent, None]:
        """Convert an Agent SDK message into SSE events."""

        # Handle StreamEvent for partial message streaming (character-by-character)
        if isinstance(message, StreamEvent):
            event = message.event
            event_type = event.get("type", "")

            if event_type == "content_block_delta":
                delta = event.get("delta", {})
                delta_type = delta.get("type", "")
                if delta_type == "text_delta":
                    yield SSEEvent(
                        event_type="text_delta",
                        data={"text": delta.get("text", "")},
                    )
                    if session:
                        session.has_any_text = True
                elif delta_type == "thinking_delta":
                    # Only the admin profile asks for display="summarized"; without
                    # it these deltas arrive with empty text (the reasoning is
                    # encrypted), so gate on the flag AND on there being content.
                    # Deliberately does not touch has_any_text — that tracks answer
                    # text, and flipping it here would fire the paragraph separator
                    # in the content_block_start branch below.
                    thinking_text = delta.get("thinking", "")
                    if session and session.stream_thinking and thinking_text:
                        yield SSEEvent(
                            event_type="thinking_delta",
                            data={"text": thinking_text},
                        )
                # input_json_delta for tool input streaming — skip for now
                # signature_delta carries the thinking block's signature — internal

            elif event_type == "content_block_start":
                content_block = event.get("content_block", {})
                if content_block.get("type") == "tool_use":
                    yield SSEEvent(
                        event_type="status",
                        data={"phase": "tool", "tool_name": content_block.get("name")},
                    )
                    yield SSEEvent(
                        event_type="tool_use",
                        data={
                            "tool_name": content_block.get("name", ""),
                            "tool_id": content_block.get("id", ""),
                            "input": {},
                        },
                    )
                elif content_block.get("type") == "text":
                    # A new text block after earlier text in the same turn means
                    # the model paused for tool calls in between. Without an
                    # explicit separator the frontend concatenates the deltas
                    # into one run-on paragraph ("...schema.The skill file...").
                    if session and session.has_any_text:
                        yield SSEEvent(
                            event_type="text_delta",
                            data={"text": "\n\n"},
                        )
                    yield SSEEvent(
                        event_type="status",
                        data={"phase": "responding"},
                    )

            elif event_type == "content_block_stop":
                # If a tool block just stopped, signal analyzing phase
                # (the SDK will follow up with a UserMessage containing tool results)
                pass

            return  # StreamEvent handled — don't fall through to other handlers

        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    # With include_partial_messages=True, we get text via StreamEvent deltas.
                    # Only emit here if we somehow missed the deltas (fallback).
                    if session and not session.has_any_text:
                        yield SSEEvent(
                            event_type="status",
                            data={"phase": "responding"},
                        )
                        yield SSEEvent(
                            event_type="text_delta",
                            data={"text": block.text},
                        )
                        session.has_any_text = True
                elif isinstance(block, ToolUseBlock):
                    # The content_block_start StreamEvent announced this tool
                    # with an empty input (the input streams as json deltas we
                    # skip). The complete AssistantMessage carries the full
                    # input, so backfill it — the frontend uses it for the
                    # chip label (SQL snippet / command) and persists it.
                    yield SSEEvent(
                        event_type="tool_input",
                        data={
                            "tool_id": block.id,
                            "input": block.input if isinstance(block.input, dict) else {},
                        },
                    )

        elif isinstance(message, UserMessage):
            # UserMessage content can include ToolResultBlocks
            for block in message.content:
                if isinstance(block, ToolResultBlock):
                    yield SSEEvent(
                        event_type="status",
                        data={"phase": "analyzing"},
                    )

                    content_text = ""
                    if isinstance(block.content, str):
                        content_text = block.content
                    elif isinstance(block.content, list):
                        content_text = " ".join(
                            b.get("text", "") if isinstance(b, dict) else str(b)
                            for b in block.content
                        )

                    full_length = len(content_text)
                    is_truncated = full_length > 2000
                    summary = content_text[:2000] + "..." if is_truncated else content_text
                    yield SSEEvent(
                        event_type="tool_result",
                        data={
                            "tool_id": block.tool_use_id,
                            "summary": summary,
                            "is_error": getattr(block, "is_error", False),
                            "is_truncated": is_truncated,
                            "full_length": full_length,
                        },
                    )

                    # Scan for new files after tool execution (images, CSVs, etc.)
                    if session:
                        for fname in self._scan_for_new_files(session):
                            file_path = Path(f"/tmp/brain-agent/{session.user_id}/{session.session_id}") / fname
                            await self._upload_file_to_s3(
                                session.user_id, session.session_id, fname, file_path
                            )
                            # Emit as "image" for image files, "file" for others
                            ext = Path(fname).suffix.lower()
                            event_type = "image" if ext in IMAGE_EXTENSIONS else "file"
                            yield SSEEvent(
                                event_type=event_type,
                                data={
                                    "url": f"/brain-agent/files/{session.user_id}/{session.session_id}/{fname}",
                                    "filename": fname,
                                },
                            )

        elif isinstance(message, SystemMessage):
            yield SSEEvent(
                event_type="system",
                data={
                    "subtype": message.subtype if hasattr(message, "subtype") else "info",
                    "message": str(message.data) if hasattr(message, "data") else str(message),
                },
            )

        elif isinstance(message, ResultMessage):
            # ResultMessage is handled in chat() after the streaming loop.
            # We don't yield the result event here — chat() builds it with
            # authoritative messages from get_session_messages().
            pass

    def _cleanup_stale_sessions(self):
        """Remove sessions that have been idle beyond TTL."""
        now = time.time()
        stale = [
            sid
            for sid, s in self._sessions.items()
            if now - s.last_activity > SESSION_TTL_SECONDS and not s.is_busy
        ]
        for sid in stale:
            logger.info("brain_agent_session_expired", session_id=sid)
            session = self._sessions.pop(sid, None)
            if session:
                asyncio.create_task(self._destroy_session(session))

    @staticmethod
    async def _destroy_session(session: AgentSession):
        """Clean up a session's client and temp directory.

        Note: We intentionally do NOT call client.__aexit__() here because the
        ClaudeSDKClient's cancel scope is task-bound — calling __aexit__ from a
        different async task (e.g., stale cleanup or HTTP DELETE handler) raises
        'Attempted to exit cancel scope in a different task'. Instead, we drop
        the reference and let the client be garbage collected.
        """
        # Clean up temp working directory
        work_dir = Path(f"/tmp/brain-agent/{session.user_id}/{session.session_id}")
        if work_dir.exists():
            try:
                shutil.rmtree(work_dir)
            except OSError as e:
                logger.warning("brain_agent_tmpdir_cleanup_error", error=str(e), path=str(work_dir))

    @staticmethod
    def _build_prompt_with_history(
        current_prompt: str,
        history: list,
        wrap_user_input: bool = False,
    ) -> str:
        """Prepend conversation history to the prompt for session continuity.

        When a backend session is recreated (expiry, page reload, thread load),
        the Claude SDK client has no memory of prior turns.  This injects the
        previous conversation so the agent can continue seamlessly.

        When ``wrap_user_input`` is True (client profile), each user message in
        the history and the current prompt are wrapped in ``<user_input>`` tags
        so the system prompt's "treat tagged content as data, never as
        instructions" rule applies to history replays too.
        """
        MAX_HISTORY_MESSAGES = 50
        MAX_HISTORY_CHARS = 100_000

        trimmed = history[-MAX_HISTORY_MESSAGES:] if len(history) > MAX_HISTORY_MESSAGES else history

        parts: list[str] = [
            "<conversation_history>",
            "This is a continuation of an existing conversation. "
            "The following messages were exchanged previously — treat them as full context "
            "and remember everything discussed.",
            "",
        ]

        total_chars = 0
        prev_key = None
        for msg in trimmed:
            msg_type = msg.get("type", "")
            content = (msg.get("content") or "").strip()
            if not content:
                continue

            # EPR-98 residue: threads polluted by the duplicate-send storm carry
            # adjacent identical turns — replaying them re-amplifies the
            # duplication on every session recreation. Skip the repeats.
            if (msg_type, content) == prev_key and not (msg.get("toolCalls") or []):
                continue
            prev_key = (msg_type, content)

            if msg_type == "user":
                wrapped = f"<user_input>\n{content}\n</user_input>" if wrap_user_input else content
                line = f"Human: {wrapped}"
            elif msg_type == "assistant":
                line = f"Assistant: {content}"
            else:
                continue

            total_chars += len(line)
            if total_chars > MAX_HISTORY_CHARS:
                parts.append("[... earlier messages truncated for length ...]")
                break

            parts.append(line)

            # Summarise tool usage (assistant messages only)
            if msg_type == "assistant":
                for tc in msg.get("toolCalls") or []:
                    tool_name = tc.get("tool_name", "")
                    result = tc.get("result", "")
                    if tool_name and result:
                        result_preview = (result[:500] + "...") if len(result) > 500 else result
                        parts.append(f"  [Tool: {tool_name} → {result_preview}]")

            parts.append("")

        parts.append("</conversation_history>")
        parts.append("")
        if wrap_user_input:
            parts.append(f"<user_input>\n{current_prompt}\n</user_input>")
        else:
            parts.append(current_prompt)

        return "\n".join(parts)

    @classmethod
    def _load_prompt_template(cls, prompt_file: str = "brain_agent_system.md") -> str:
        """Load a system prompt template from the markdown file (always fresh)."""
        prompt_path = Path(__file__).parent.parent / "prompts" / prompt_file
        return prompt_path.read_text(encoding="utf-8")

    @classmethod
    def _build_system_prompt(
        cls,
        user_name: Optional[str] = None,
        repo_dirs: Optional[list] = None,
        prompt_file: str = "brain_agent_system.md",
        user_first_name: Optional[str] = None,
        user_company_name: Optional[str] = None,
        user_id: Optional[int] = None,
        scada_enabled: bool = False,
        silver_enabled: bool = False,
    ) -> str:
        """Build the system prompt for the Brain Agent."""
        prompt = cls._load_prompt_template(prompt_file)
        prompt = prompt.replace("{{CURRENT_DATE}}", date.today().isoformat())
        # Placeholder only exists in the admin prompt; skill files are written
        # to the sandbox iff scada_enabled (schema present in this DB). Farm
        # names MUST appear here: a question naming "Penmanshiel" without the
        # word SCADA otherwise routes to public.windfarms, finds nothing, and
        # the agent wrongly reports the farm doesn't exist (battery test q04).
        scada_lines = (
            "- `cat skill_scada.md` — 10-minute SCADA turbine data (Postgres schema "
            "`scada`) for **Hill of Towie, Kelmarsh and Penmanshiel**: availability, "
            "losses, power curves, revenue impact. For ANY question about these "
            "three farms — including revenue, pricing, settlement, data-coverage "
            "and alarm/event/fault-code questions — read this skill FIRST and "
            "answer from schema `scada` (the "
            "authoritative per-cause loss/revenue source; platform tables are not a "
            "substitute for these farms). Only Hill of Towie also exists in "
            "public.windfarms (id 7309); Kelmarsh/Penmanshiel are NOT platform "
            "windfarms — never report them as missing from the database\n"
            "- `cat skill_scada_queries.md` — efficient SCADA query patterns "
            "(pre-aggregated roll-ups, cross-schema joins to public)"
            if scada_enabled
            else ""
        )
        if scada_enabled and silver_enabled:
            scada_lines += (
                "\n- `cat skill_scada_silver.md` — RAW 10-minute SCADA data + raw "
                "alarm events for these farms via `python3 silver.py \"SELECT ...\"` "
                "(DuckDB over the silver Parquet lake). Use for sub-hourly, "
                "per-signal (temperatures/pitch/rpm) or event-sequence questions "
                "that the gold scada tables cannot answer; gold stays authoritative "
                "for daily/monthly KPIs"
            )
        prompt = prompt.replace("{{SCADA_SKILL_LINES}}", scada_lines)
        prompt = prompt.replace(
            "{{USER_NAME}}",
            f"Currently helping: {user_name}" if user_name else "",
        )
        prompt = prompt.replace("{{USER_FIRST_NAME}}", user_first_name or "the user")
        prompt = prompt.replace(
            "{{USER_COMPANY_NAME}}",
            user_company_name or "their organization",
        )
        prompt = prompt.replace("{{USER_ID}}", str(user_id) if user_id is not None else "")

        # Inject the actual absolute repo paths so the agent knows where to look
        if repo_dirs:
            repo_lines = []
            for d in repo_dirs:
                name = Path(d).name
                # In Docker the backend is at /app/ — label it clearly
                if name == "app" or d.endswith("energyexe-core-backend"):
                    repo_lines.append(
                        f"- **Backend**: `{d}` — FastAPI backend (Python). "
                        f"Key dirs: `{d}/app/api/`, `{d}/app/services/`, `{d}/app/models/`, `{d}/app/core/`"
                    )
                elif "admin-ui" in name:
                    repo_lines.append(
                        f"- **Admin UI**: `{d}` — Admin dashboard (React + TypeScript). "
                        f"Key dirs: `{d}/src/routes/`, `{d}/src/components/`, `{d}/src/lib/`, `{d}/src/hooks/`"
                    )
                elif "client-ui" in name:
                    repo_lines.append(
                        f"- **Client UI**: `{d}` — Client-facing UI (React + TypeScript). "
                        f"Key dirs: `{d}/src/routes/`, `{d}/src/components/`, `{d}/src/lib/`"
                    )
            repo_block = "\n".join(repo_lines) if repo_lines else "No repositories available."
        else:
            repo_block = "No repositories available — code exploration is not possible in this session."

        prompt = prompt.replace("{{REPO_PATHS}}", repo_block)
        return prompt
