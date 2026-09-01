"""Endpoint tests for the EPR-98 duplicate-send guard on POST /brain-agent/chat.

Builds a minimal app (same approach as test_brain_agent_upload_endpoint.py):
the shared `client` fixture runs the full lifespan, which needs Postgres/Valkey.
The agent itself is stubbed — these tests only exercise the 409 pre-check.
"""

import time
import uuid
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

pytest.importorskip("claude_agent_sdk")

from app.api.v1.endpoints import brain_agent as endpoint_module  # noqa: E402
from app.core.deps import get_current_user, get_db  # noqa: E402
from app.services.brain_agent_service import BrainAgentService, SSEEvent  # noqa: E402

USER_ID = 42


class _FakeUser:
    id = USER_ID
    role = "admin"
    first_name = "Test"
    last_name = None
    company_name = None
    is_superuser = True


@pytest.fixture
def client(monkeypatch):
    # Rate limiter talks to Valkey — always allow here.
    import app.core.redis as redis_module

    async def _allow(**_kwargs):
        return True, 0

    monkeypatch.setattr(redis_module, "check_rate_limit", _allow)

    # Stub the agent run: a non-duplicate request must stream a result without
    # touching the SDK. If the guard under test fails, the request lands here
    # and the 409 assertion catches it.
    async def _stub_chat(self, **_kwargs):
        yield SSEEvent(event_type="result", data={"session_id": "stub"})

    monkeypatch.setattr(BrainAgentService, "chat", _stub_chat)

    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/brain-agent")

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def busy_session():
    """Seed a live in-memory run and restore the session dict afterwards."""
    sid = str(uuid.uuid4())
    saved = dict(BrainAgentService._sessions)
    BrainAgentService._sessions[sid] = SimpleNamespace(
        session_id=sid,
        user_id=USER_ID,
        is_busy=True,
        run_started_at=time.time(),
        current_message_id="m1",
        last_message_id="m1",
        current_prompt="show me T21",
        detach_task=None,
    )
    try:
        yield sid
    finally:
        BrainAgentService._sessions.clear()
        BrainAgentService._sessions.update(saved)


def test_duplicate_message_id_gets_409(client, busy_session):
    resp = client.post(
        "/brain-agent/chat",
        json={"prompt": "show me T21", "session_id": busy_session, "message_id": "m1"},
    )
    assert resp.status_code == 409
    assert "still working" in resp.json()["detail"]


def test_duplicate_prompt_without_message_id_gets_409(client, busy_session):
    # Old deployed clients send no message_id — the raw prompt is the match key.
    resp = client.post(
        "/brain-agent/chat",
        json={"prompt": "show me T21", "session_id": busy_session},
    )
    assert resp.status_code == 409


def test_different_prompt_while_busy_is_allowed(client, busy_session):
    # Supersede path: a genuinely new question must reach the agent.
    resp = client.post(
        "/brain-agent/chat",
        json={"prompt": "a different question", "session_id": busy_session, "message_id": "m2"},
    )
    assert resp.status_code == 200
    assert "event: result" in resp.text


def test_new_session_is_allowed(client):
    resp = client.post(
        "/brain-agent/chat",
        json={"prompt": "hello", "session_id": str(uuid.uuid4()), "message_id": "m9"},
    )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# DELETE /sessions/{id} busy guard: a live (or detached, still-draining) run's
# session must never be destroyed out from under it — its sandbox holds the
# CLI subprocess cwd, transcript, and output files.
# ---------------------------------------------------------------------------


def test_end_session_while_busy_is_deferred(client, busy_session):
    resp = client.delete(f"/brain-agent/sessions/{busy_session}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["busy"] is True
    # The session survives for the TTL cleanup to reclaim later.
    assert busy_session in BrainAgentService._sessions


def test_end_session_while_detach_draining_is_deferred(client, busy_session, monkeypatch):
    session = BrainAgentService._sessions[busy_session]
    session.is_busy = False
    session.detach_task = SimpleNamespace(done=lambda: False)
    resp = client.delete(f"/brain-agent/sessions/{busy_session}")
    assert resp.json() == {"success": False, "session_id": busy_session, "busy": True}
    assert busy_session in BrainAgentService._sessions


def test_end_session_idle_is_destroyed(client, busy_session, monkeypatch):
    session = BrainAgentService._sessions[busy_session]
    session.is_busy = False
    session.detach_task = None

    destroyed = []

    async def _fake_destroy(s):
        destroyed.append(s.session_id)

    monkeypatch.setattr(BrainAgentService, "_destroy_session", staticmethod(_fake_destroy))
    resp = client.delete(f"/brain-agent/sessions/{busy_session}")
    assert resp.json() == {"success": True, "session_id": busy_session, "busy": False}
    assert busy_session not in BrainAgentService._sessions
    assert destroyed == [busy_session]


def test_end_session_unknown_or_foreign_is_a_noop(client):
    resp = client.delete(f"/brain-agent/sessions/{uuid.uuid4()}")
    assert resp.json()["success"] is False
    assert resp.json()["busy"] is False
