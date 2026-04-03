"""Brain Agent service — orchestrates Claude Agent SDK sessions with energy data tools."""

import asyncio
import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Optional

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from app.core.config import get_settings
from app.schemas.brain_agent import DEFAULT_BRAIN_MODEL
from app.services.brain_agent_tools import (
    ENERGYEXE_TOOL_NAMES,
    energyexe_mcp_server,
    set_user_id,
    clear_user_id,
)

logger = structlog.get_logger(__name__)

# Session TTL: clean up sessions idle for more than 30 minutes
SESSION_TTL_SECONDS = 30 * 60
MAX_CONCURRENT_SESSIONS = 20


@dataclass
class SSEEvent:
    """A single SSE event to stream to the client."""

    event_type: str  # text_delta, tool_use, tool_result, system, result, error, image
    data: Dict[str, Any]


IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif"}


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
    known_images: set = field(default_factory=set)


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
        model: Optional[str] = None,
    ) -> AsyncGenerator[SSEEvent, None]:
        """Send a prompt to the agent and yield SSE events."""
        if not session_id:
            session_id = str(uuid.uuid4())

        # Clean up stale sessions
        self._cleanup_stale_sessions()

        # Set up user context for MCP tools (ContextVar — per-task safe)
        # Note: DB sessions are now created per-tool-call (short-lived) to avoid
        # holding connections for the entire agent conversation and exhausting the pool.
        set_user_id(user_id)

        try:
            session = await self._get_or_create_session(user_id, session_id, user_name, model)

            async with session.lock:
                # If a previous turn was abandoned (e.g. SSE disconnect), drain leftover messages
                if session.is_busy:
                    logger.warning("brain_agent_draining_previous_turn", session_id=session_id)
                    await self._interrupt_and_drain(session)

                session.is_busy = True
                session.last_activity = time.time()

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
                async for message in session.client.receive_messages():
                    async for event in self._process_message(message, session):
                        yield event

                    # ResultMessage means the agent is done
                    if isinstance(message, ResultMessage):
                        got_result = True
                        break

                # If the message stream ended without a ResultMessage, emit a
                # synthetic result so the frontend knows the turn is over.
                if not got_result:
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
            yield SSEEvent(
                event_type="error",
                data={"message": str(e), "code": "agent_error"},
            )
        finally:
            if session_id in self._sessions:
                self._sessions[session_id].is_busy = False
            clear_user_id()

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
        self, user_id: int, session_id: str, user_name: Optional[str] = None, model: Optional[str] = None
    ) -> AgentSession:
        """Get existing session or create a new one."""
        if session_id in self._sessions:
            return self._sessions[session_id]

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

        system_prompt = self._build_system_prompt(user_name)

        def _on_stderr(line: str):
            logger.warning("brain_agent_stderr", session_id=session_id, line=line.rstrip())

        options = ClaudeAgentOptions(
            system_prompt=system_prompt,
            allowed_tools=[
                "Bash",
                "WebSearch",
                "WebFetch",
                *ENERGYEXE_TOOL_NAMES,
            ],
            disallowed_tools=[
                "ToolSearch",  # MCP tools are already allowed — no need to discover them
                "TodoWrite",
                "Agent",
                "EnterPlanMode",
                "ExitPlanMode",
                "AskUserQuestion",
                "Skill",
                "NotebookEdit",
            ],
            mcp_servers={"energyexe": energyexe_mcp_server},
            cwd=work_dir,
            max_turns=None,
            max_budget_usd=None,
            permission_mode="bypassPermissions",
            model=model or getattr(settings, "BRAIN_MODEL", DEFAULT_BRAIN_MODEL),
            stderr=_on_stderr,
            max_buffer_size=10 * 1024 * 1024,  # 10MB — default 1MB is too small for large query results
            env={"CLAUDE_CODE_STREAM_CLOSE_TIMEOUT": "600000"},  # 10 min — default 60s too aggressive for DB queries
        )

        client = ClaudeSDKClient(options=options)
        # Enter the async context manager
        await client.__aenter__()

        session = AgentSession(
            session_id=session_id,
            user_id=user_id,
            client=client,
            created_at=time.time(),
            last_activity=time.time(),
        )
        self._sessions[session_id] = session
        return session

    def _scan_for_new_images(self, session: AgentSession) -> list:
        """Scan the session sandbox for new image files."""
        work_dir = Path(f"/tmp/brain-agent/{session.user_id}/{session.session_id}")
        new_images = []
        if work_dir.exists():
            for f in work_dir.iterdir():
                if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS and f.name not in session.known_images:
                    session.known_images.add(f.name)
                    new_images.append(f.name)
        return new_images

    async def _process_message(self, message, session: AgentSession = None) -> AsyncGenerator[SSEEvent, None]:
        """Convert an Agent SDK message into SSE events."""
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    yield SSEEvent(
                        event_type="status",
                        data={"phase": "responding"},
                    )
                    yield SSEEvent(
                        event_type="text_delta",
                        data={"text": block.text},
                    )
                elif isinstance(block, ToolUseBlock):
                    yield SSEEvent(
                        event_type="status",
                        data={"phase": "tool", "tool_name": block.name},
                    )
                    yield SSEEvent(
                        event_type="tool_use",
                        data={
                            "tool_name": block.name,
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

                    # Scan for new images after tool execution
                    if session:
                        for img_name in self._scan_for_new_images(session):
                            yield SSEEvent(
                                event_type="image",
                                data={
                                    "url": f"/brain-agent/sessions/{session.session_id}/files/{img_name}",
                                    "filename": img_name,
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
            yield SSEEvent(
                event_type="result",
                data={
                    "num_turns": message.num_turns if hasattr(message, "num_turns") else 0,
                    "duration_ms": message.duration_ms if hasattr(message, "duration_ms") else 0,
                    "cost_usd": message.total_cost_usd
                    if hasattr(message, "total_cost_usd")
                    else None,
                    "session_id": None,  # Will be set by the caller
                },
            )

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

    @classmethod
    def _load_prompt_template(cls) -> str:
        """Load and cache the system prompt template from the markdown file."""
        if cls._prompt_template is None:
            prompt_path = Path(__file__).parent.parent / "prompts" / "brain_agent_system.md"
            cls._prompt_template = prompt_path.read_text(encoding="utf-8")
        return cls._prompt_template

    @classmethod
    def _build_system_prompt(cls, user_name: Optional[str] = None) -> str:
        """Build the system prompt for the Brain Agent."""
        prompt = cls._load_prompt_template()
        prompt = prompt.replace("{{CURRENT_DATE}}", date.today().isoformat())
        prompt = prompt.replace(
            "{{USER_NAME}}",
            f"Currently helping: {user_name}" if user_name else "",
        )
        return prompt
