"""Brain Agent service — orchestrates Claude Agent SDK sessions with energy data tools."""

import asyncio
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
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
from app.services.brain_agent_skill_files import SKILL_SCHEMA, SKILL_QUERIES, SKILL_DOMAIN, SKILL_SOURCES

logger = structlog.get_logger(__name__)

# Session TTL: clean up sessions idle for more than 30 minutes
SESSION_TTL_SECONDS = 30 * 60
MAX_CONCURRENT_SESSIONS = 20

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
        "max_budget_usd": None,
        "wrap_user_input": False,
    },
    "client": {
        "system_prompt_file": "brain_agent_system_client.md",
        "model_default": None,
        "model_locked": False,
        "max_turns": 25,
        "max_budget_usd": None,
        "wrap_user_input": False,
    },
}


def _get_profile(source: Optional[str]) -> Dict[str, Any]:
    """Return the profile dict for a given source, defaulting to admin."""
    return PROFILES.get(source or "admin", PROFILES["admin"])


@dataclass
class SSEEvent:
    """A single SSE event to stream to the client."""

    event_type: str  # text_delta, tool_use, tool_result, system, result, error, image, file
    data: Dict[str, Any]


IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif"}

# Files placed in the sandbox at session creation — skip when scanning for agent output
SANDBOX_SEED_FILES = {"db.py", "skill_schema.md", "skill_queries.md", "skill_domain.md", "skill_sources.md"}

# Working file extensions — scripts the agent writes to execute, not user-facing output
WORKING_FILE_EXTENSIONS = {".py", ".sh", ".bash", ".sql"}


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


class BrainAgentService:
    """Manages ClaudeSDKClient sessions and streams responses as SSE events."""

    _sessions: Dict[str, AgentSession] = {}
    _prompt_template: Optional[str] = None

    def __init__(self, db: AsyncSession):
        self.db = db

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
    ) -> AsyncGenerator[SSEEvent, None]:
        """Send a prompt to the agent and yield SSE events."""
        if not session_id:
            session_id = str(uuid.uuid4())

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

            async with session.lock:
                # If a previous turn was abandoned (e.g. SSE disconnect), drain leftover messages
                if session.is_busy:
                    logger.warning("brain_agent_draining_previous_turn", session_id=session_id)
                    await self._interrupt_and_drain(session)

                session.is_busy = True
                session.last_activity = time.time()
                session.has_any_text = False  # Reset text dedup for this turn

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

                # Stream response messages
                got_result = False
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
                    # Read authoritative conversation from SDK transcript.
                    # Use the SDK's internal session_id (from ResultMessage),
                    # NOT our session_id — they are different.
                    sdk_session_id = result_message.session_id if hasattr(result_message, "session_id") else session_id
                    work_dir = Path(f"/tmp/brain-agent/{user_id}/{session_id}")
                    try:
                        sdk_messages = get_session_messages(
                            session_id=sdk_session_id,
                            directory=str(work_dir),
                        )
                        final_messages = self._convert_sdk_messages(sdk_messages)
                    except Exception as e:
                        logger.error("brain_agent_get_session_messages_error", error=str(e), session_id=session_id)
                        final_messages = []

                    # Save to DB
                    if final_messages:
                        await self._save_thread_to_db(
                            session_id=session_id,
                            user_id=user_id,
                            messages=final_messages,
                            model=model,
                            cost_usd=result_message.total_cost_usd if hasattr(result_message, "total_cost_usd") else None,
                            num_turns=result_message.num_turns if hasattr(result_message, "num_turns") else 0,
                        )

                    # Yield result with authoritative messages
                    yield SSEEvent(
                        event_type="result",
                        data={
                            "num_turns": result_message.num_turns if hasattr(result_message, "num_turns") else 0,
                            "duration_ms": result_message.duration_ms if hasattr(result_message, "duration_ms") else 0,
                            "cost_usd": result_message.total_cost_usd if hasattr(result_message, "total_cost_usd") else None,
                            "session_id": session_id,
                            "messages": final_messages,
                        },
                    )
                else:
                    # Stream ended without a ResultMessage — emit a synthetic result
                    logger.warning(
                        "brain_agent_stream_ended_without_result",
                        session_id=session_id,
                    )
                    yield SSEEvent(
                        event_type="result",
                        data={
                            "num_turns": 0,
                            "duration_ms": 0,
                            "cost_usd": None,
                            "session_id": session_id,
                            "incomplete": True,
                        },
                    )

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
            yield SSEEvent(
                event_type="error",
                data={"message": str(e), "code": "agent_error"},
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
            if session_id in self._sessions:
                self._sessions[session_id].is_busy = False

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

                content = "\n".join(text_parts).strip()
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

                content = "\n".join(text_parts).strip()

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

        return messages

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
        if session_id in self._sessions:
            # Verify the sandbox directory still exists (could be lost to
            # container restart, tmpwatch, or manual cleanup).  If it's gone,
            # discard the stale in-memory session and recreate below.
            existing = self._sessions[session_id]
            work_dir_check = Path(f"/tmp/brain-agent/{existing.user_id}/{existing.session_id}")
            if work_dir_check.exists():
                return existing, False
            logger.warning("brain_agent_session_workdir_missing", session_id=session_id)
            self._sessions.pop(session_id, None)

        # Enforce session limit
        user_sessions = [s for s in self._sessions.values() if s.user_id == user_id]
        if len(user_sessions) >= MAX_CONCURRENT_SESSIONS:
            # Remove oldest
            oldest = min(user_sessions, key=lambda s: s.last_activity)
            self._sessions.pop(oldest.session_id, None)

        # Create temp working directory
        work_dir = Path(f"/tmp/brain-agent/{user_id}/{session_id}")
        work_dir.mkdir(parents=True, exist_ok=True)

        settings = get_settings()
        profile = _get_profile(source)

        # Resolve source code repo paths (for read-only code access)
        from app.services.brain_agent_repo_manager import get_repo_dirs
        repo_dirs_str = get_repo_dirs()

        system_prompt = self._build_system_prompt(
            user_name,
            repo_dirs=repo_dirs_str,
            prompt_file=profile["system_prompt_file"],
            user_first_name=user_first_name,
            user_company_name=user_company_name,
            user_id=user_id,
        )

        # Write db.py helper script and skill files to sandbox
        (work_dir / "db.py").write_text(DB_HELPER_SCRIPT)
        (work_dir / "skill_schema.md").write_text(SKILL_SCHEMA)
        (work_dir / "skill_queries.md").write_text(SKILL_QUERIES)
        (work_dir / "skill_domain.md").write_text(SKILL_DOMAIN)
        (work_dir / "skill_sources.md").write_text(SKILL_SOURCES)

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

        # Strict read-only DB access for the agent process:
        #   1. Prefer the dedicated `brain_agent_ro` Postgres role — it has
        #      only SELECT grants, so the server rejects any write attempt
        #      regardless of which client the agent's bash spawns.
        #   2. Fall back to the main URL + PGOPTIONS session-level
        #      `default_transaction_read_only=on` if the role's password
        #      isn't configured yet.
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
            permission_mode="bypassPermissions",
            model=resolved_model,
            stderr=_on_stderr,
            max_buffer_size=10 * 1024 * 1024,
            setting_sources=[],  # Don't inherit global MCP servers (Gmail, Slack, etc.)
            mcp_servers={},  # No MCP servers needed for brain agent
            include_partial_messages=True,
            env={
                "DATABASE_URL": agent_db_url,
                # Belt-and-suspenders — forces every transaction to be
                # read-only at the session level even if the role somehow
                # gained write grants.
                "PGOPTIONS": "-c default_transaction_read_only=on",
                "CLAUDE_CODE_STREAM_CLOSE_TIMEOUT": "1200000",  # 20 min (was 10)
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
                if f.is_file() and f.name not in SANDBOX_SEED_FILES:
                    existing_files.add(f.name)

        session = AgentSession(
            session_id=session_id,
            user_id=user_id,
            client=client,
            created_at=time.time(),
            last_activity=time.time(),
            known_files=existing_files,
        )
        self._sessions[session_id] = session
        return session, True

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
                    and f.suffix.lower() not in WORKING_FILE_EXTENSIONS
                ):
                    session.known_files.add(f.name)
                    new_files.append(f.name)
        return new_files

    @staticmethod
    async def _upload_file_to_s3(user_id: int, thread_id: str, filename: str, file_path: Path):
        """Upload a file to S3 for permanent storage. Failures are logged, not raised."""
        try:
            from app.services.s3_service import upload_file

            key = f"brain-agent/{user_id}/{thread_id}/{Path(filename).name}"
            await upload_file(key, file_path)
        except Exception as e:
            logger.error("brain_agent_file_upload_failed", error=str(e), filename=filename)

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
                # input_json_delta for tool input streaming — skip for now

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
                    # Tool use blocks are already emitted via StreamEvent content_block_start.
                    # Only emit here as fallback if StreamEvent didn't fire.
                    pass

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
        for msg in trimmed:
            msg_type = msg.get("type", "")
            content = (msg.get("content") or "").strip()
            if not content:
                continue

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
    ) -> str:
        """Build the system prompt for the Brain Agent."""
        prompt = cls._load_prompt_template(prompt_file)
        prompt = prompt.replace("{{CURRENT_DATE}}", date.today().isoformat())
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
