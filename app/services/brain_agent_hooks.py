"""PreToolUse hooks for the brain agent's sandbox.

The agent runs with ``permission_mode="bypassPermissions"`` because every Bash
command it issues is meant to execute without a human in the loop. A
``PreToolUse`` hook still fires in that mode (unlike ``can_use_tool``, which is
only consulted when the permission flow reaches a prompt), which makes it the
one place we can inspect a command before it runs.

Why this exists on top of the guard inside the sandbox's ``db.py``: that guard
only runs when the agent goes through ``db.py``. Nothing stops it from opening
its own psycopg2 connection or shelling out to ``psql``, which bypasses the
guard entirely. This hook sits in the harness, so it sees the command either
way.

Scope note — this is defence in depth, not a security boundary. Matching tokens
in a shell command string can be evaded by obfuscation (base64, string
concatenation, a generated script file). The real control remains the
``brain_agent_client_ro`` Postgres role, whose grants make internal tables
unreadable no matter how the query is spelled. This hook closes the
bypass-``db.py`` gap and gives us a log line for every command the agent runs.
"""

import re
from typing import Any, Awaitable, Callable, Dict, Optional

import structlog

logger = structlog.get_logger(__name__)

# Introspection surfaces the client agent must not reach (EPR-59). Kept in sync
# with the identical list inside DB_HELPER_SCRIPT by a drift canary in
# tests/test_brain_agent_hooks.py — change both or the test fails.
INTROSPECTION_TOKENS = (
    "INFORMATION_SCHEMA",
    "PG_CATALOG",
    "PG_CLASS",
    "PG_ATTRIBUTE",
    "PG_CONSTRAINT",
    "PG_NAMESPACE",
    "PG_TABLES",
    "PG_VIEWS",
    "PG_INDEXES",
    "PG_ROLES",
    "PG_STAT",
    "PG_DESCRIPTION",
)

_INTROSPECTION_RE = re.compile(
    r"\b(" + "|".join(INTROSPECTION_TOKENS) + r")\b", re.IGNORECASE
)

INTROSPECTION_DENIAL = "Schema introspection is not available."

# Bash commands are logged for telemetry; long heredocs would flood the logs.
_LOG_COMMAND_CHARS = 400


def command_is_introspection(command: str) -> bool:
    """True if a shell command reaches for Postgres catalog/schema metadata."""
    return bool(command) and bool(_INTROSPECTION_RE.search(command))


def _deny(reason: str) -> Dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def make_pre_tool_use_hook(
    source: Optional[str],
    session_id: Optional[str] = None,
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """Build the PreToolUse hook for one session.

    Logs every Bash command, and on the client surface denies the ones that
    reach for schema introspection. An empty dict means "no opinion" — the
    permission flow proceeds as it would without the hook.
    """
    block_introspection = source == "client"

    async def pre_tool_use(
        input_data: Dict[str, Any],
        tool_use_id: Optional[str],
        context: Any,
    ) -> Dict[str, Any]:
        tool_name = input_data.get("tool_name")
        if tool_name != "Bash":
            return {}

        tool_input = input_data.get("tool_input") or {}
        command = tool_input.get("command") or ""

        if block_introspection and command_is_introspection(command):
            logger.warning(
                "brain_agent_bash_denied",
                session_id=session_id,
                source=source,
                reason="introspection",
                command=command[:_LOG_COMMAND_CHARS],
            )
            return _deny(INTROSPECTION_DENIAL)

        logger.info(
            "brain_agent_bash",
            session_id=session_id,
            source=source,
            command=command[:_LOG_COMMAND_CHARS],
        )
        return {}

    return pre_tool_use
