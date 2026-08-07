"""Tests for the brain agent's PreToolUse hook.

The hook is the harness-level half of the EPR-59 introspection block: the guard
inside the sandbox's db.py only runs when the agent goes through db.py, and
nothing stops it opening its own psycopg2 connection or shelling out to psql.
"""

import pytest

from app.services.brain_agent_db_script import DB_HELPER_SCRIPT
from app.services.brain_agent_hooks import (
    INTROSPECTION_DENIAL,
    INTROSPECTION_TOKENS,
    command_is_introspection,
    make_pre_tool_use_hook,
)


def _bash(command: str) -> dict:
    return {"tool_name": "Bash", "tool_input": {"command": command}}


async def _run(source: str, command: str) -> dict:
    hook = make_pre_tool_use_hook(source, session_id="s-1")
    return await hook(_bash(command), "tool-1", None)


def _decision(result: dict):
    return (result.get("hookSpecificOutput") or {}).get("permissionDecision")


# ── the introspection matcher ──


@pytest.mark.parametrize("token", INTROSPECTION_TOKENS)
def test_every_token_is_detected(token):
    assert command_is_introspection(f'psql -c "select * from {token.lower()}.foo"')


@pytest.mark.parametrize(
    "command",
    [
        'python3 db.py "SELECT name FROM windfarms LIMIT 5"',
        "python3 analysis.py",
        "ls -la",
        "",
    ],
)
def test_ordinary_commands_are_not_flagged(command):
    assert not command_is_introspection(command)


def test_matcher_is_word_bounded():
    # A column or alias that merely contains a token substring isn't introspection.
    assert not command_is_introspection('db.py "SELECT pg_statement_id FROM t"')


# ── the hook decision ──


@pytest.mark.asyncio
async def test_client_introspection_is_denied():
    result = await _run("client", 'psql -c "select * from information_schema.tables"')
    assert _decision(result) == "deny"
    assert result["hookSpecificOutput"]["permissionDecisionReason"] == INTROSPECTION_DENIAL


@pytest.mark.asyncio
async def test_admin_introspection_is_allowed():
    """Admin is meant to have full access — the block is a client-surface rule."""
    result = await _run("admin", 'psql -c "select * from information_schema.tables"')
    assert result == {}


@pytest.mark.asyncio
async def test_unknown_source_defaults_to_admin_behaviour():
    # _get_profile() treats an unknown/None source as admin; stay consistent.
    assert await _run(None, "select * from pg_catalog.pg_tables") == {}


@pytest.mark.asyncio
async def test_ordinary_client_command_is_allowed():
    assert await _run("client", 'python3 db.py "SELECT 1"') == {}


@pytest.mark.asyncio
async def test_non_bash_tools_are_ignored():
    hook = make_pre_tool_use_hook("client")
    payload = {"tool_name": "Read", "tool_input": {"file_path": "information_schema.md"}}
    assert await hook(payload, "tool-1", None) == {}


@pytest.mark.asyncio
async def test_missing_command_does_not_raise():
    hook = make_pre_tool_use_hook("client")
    assert await hook({"tool_name": "Bash", "tool_input": {}}, None, None) == {}
    assert await hook({"tool_name": "Bash"}, None, None) == {}


# ── drift canary ──


def test_token_list_matches_the_in_sandbox_guard():
    """The hook and db.py must block the same surfaces.

    Two enforcement points, one rule — if someone adds a token to db.py's regex
    and not here, the client agent could reach it by shelling out to psql
    instead. Fails loudly rather than silently diverging.
    """
    script = DB_HELPER_SCRIPT.upper()
    missing = [token for token in INTROSPECTION_TOKENS if token not in script]
    assert not missing, f"tokens guarded by the hook but not by db.py: {missing}"
