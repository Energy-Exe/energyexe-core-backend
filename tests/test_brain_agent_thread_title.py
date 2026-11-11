"""EPR-207: a renamed agent chat keeps its name through the frontends' full-thread PUT.

Calls the endpoint function directly with a fake DB — the shared app fixture
needs Postgres/Valkey.
"""

import asyncio
from datetime import datetime
from types import SimpleNamespace

import pytest

pytest.importorskip("claude_agent_sdk")

from app.api.v1.endpoints.brain_agent import upsert_thread  # noqa: E402
from app.models.agent_thread import AgentThread  # noqa: E402
from app.schemas.brain_agent import ThreadUpsertRequest  # noqa: E402

USER = SimpleNamespace(id=7)


class _FakeDB:
    def __init__(self, thread):
        self.thread = thread
        self.added = []

    async def execute(self, *_a, **_kw):
        return SimpleNamespace(scalar_one_or_none=lambda: self.thread)

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        return None

    async def refresh(self, obj):
        now = datetime(2026, 11, 11)
        obj.created_at = obj.created_at or now
        obj.updated_at = obj.updated_at or now
        obj.is_streaming = bool(obj.is_streaming)
        obj.message_count = obj.message_count or 0
        obj.total_turns = obj.total_turns or 0


def _thread(title):
    return AgentThread(
        id="t-1",
        user_id=USER.id,
        title=title,
        messages=[],
        message_count=0,
        total_turns=0,
        is_streaming=False,
        created_at=datetime(2026, 11, 1),
        updated_at=datetime(2026, 11, 1),
    )


def _put(thread, title):
    db = _FakeDB(thread)
    body = ThreadUpsertRequest(title=title, messages=[{"type": "user", "content": "q"}])
    out = asyncio.run(upsert_thread("t-1", body, db=db, current_user=USER))
    return out, db


def test_put_keeps_a_user_chosen_title():
    out, _ = _put(_thread("Norway Q3 review"), "how many farms are in NO4?")
    assert out.title == "Norway Q3 review"


def test_put_sets_the_title_of_a_new_thread():
    out, db = _put(None, "how many farms are in NO4?")
    assert out.title == "how many farms are in NO4?"
    assert len(db.added) == 1


def test_put_fills_an_empty_title():
    out, _ = _put(_thread(None), "how many farms are in NO4?")
    assert out.title == "how many farms are in NO4?"


def test_put_replaces_a_leaked_history_title_but_never_with_another():
    out, _ = _put(_thread("<conversation_history> This is a continu"), "real prompt")
    assert out.title == "real prompt"
    out, _ = _put(_thread("<attachments> The user has attached th"), "<conversation_history> x")
    assert out.title == "<attachments> The user has attached th"  # left for the server to heal
