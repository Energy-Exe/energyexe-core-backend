"""sweep_stuck_agent_threads clears is_streaming stranded by a task death and
appends a restart marker the UI twins already know how to render."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.dialects import postgresql

pytest.importorskip("claude_agent_sdk")

from app.services.brain_agent_service import (  # noqa: E402
    RESTART_TERMINAL_INFO,
    BrainAgentService,
    derive_terminal_info,
    sweep_stuck_agent_threads,
)


class _Ctx:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _thread(messages, **overrides):
    fields = dict(id="t1", messages=messages, message_count=len(messages), is_streaming=True)
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _db(threads):
    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = threads
    db.execute = AsyncMock(return_value=result)
    db.commit = AsyncMock()
    return db


@pytest.fixture
def factory(monkeypatch):
    def _install(threads):
        db = _db(threads)
        monkeypatch.setattr("app.core.database.get_session_factory", lambda: lambda: _Ctx(db))
        return db

    return _install


async def test_appends_restart_marker_and_clears_flag(factory):
    original = [{"id": "u1", "type": "user", "content": "hi", "timestamp": 1}]
    thread = _thread(original)
    db = factory([thread])

    assert await sweep_stuck_agent_threads() == 1

    db.commit.assert_awaited_once()
    assert thread.is_streaming is False
    assert thread.message_count == 2
    assert thread.messages is not original  # reassigned, not appended in place
    tail = thread.messages[-1]
    assert tail["type"] == "assistant"
    assert "restarted" in tail["content"]
    assert tail["terminal"]["kind"] == "unknown_error"
    assert tail["terminal"]["subtype"] == "service_restart"
    assert tail["terminal"]["is_error"] is True


def test_marker_shape_matches_the_live_terminal_marker():
    live = BrainAgentService._terminal_marker(
        SimpleNamespace(uuid="x"), derive_terminal_info(SimpleNamespace(is_error=True))
    )
    swept = BrainAgentService._terminal_marker(None, RESTART_TERMINAL_INFO)

    assert set(swept) == set(live)
    assert set(swept["terminal"]) == set(live["terminal"])
    assert swept["id"].startswith("terminal-")


async def test_idempotent_when_tail_is_already_terminal(factory):
    marker = BrainAgentService._terminal_marker(None, RESTART_TERMINAL_INFO)
    thread = _thread([{"id": "u1", "type": "user", "content": "hi"}, marker])
    factory([thread])

    await sweep_stuck_agent_threads()

    assert thread.message_count == 2
    assert thread.messages[-1] is marker
    assert thread.is_streaming is False


async def test_no_streaming_threads_is_a_noop(factory):
    db = factory([])

    assert await sweep_stuck_agent_threads() == 0
    db.commit.assert_awaited_once()


async def test_selects_only_streaming_threads(factory):
    db = factory([])

    await sweep_stuck_agent_threads()

    stmt = db.execute.await_args.args[0]
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "agent_threads.is_streaming IS true" in sql
