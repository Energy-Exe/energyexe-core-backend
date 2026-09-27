"""EPR-143 (review round 2): the brain agent's ADMIN profile is internal-staff only.

The admin profile connects with the shared read-only Postgres role (migration b9d8e3a5c2f1:
``GRANT SELECT ON ALL TABLES IN SCHEMA public``), which includes the internal-only SCADA PPA
register. ``require_agent_access(..., source="admin")`` therefore needs superuser AND
``is_internal``; the chat endpoint downgrades any other non-client caller to the client profile
before the credentials are chosen, and the service re-checks with a fresh row read.
"""

import uuid
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.core.agent_access import (
    is_internal_staff,
    require_agent_access,
    require_fresh_agent_access,
)


def _user(**over):
    base = dict(
        id=7,
        is_active=True,
        role="admin",
        email_verified=True,
        is_approved=True,
        is_superuser=True,
        is_internal=True,
        first_name="T",
        last_name=None,
        company_name=None,
    )
    base.update(over)
    return SimpleNamespace(**base)


def test_admin_source_requires_superuser_and_internal():
    require_agent_access(_user(), source="admin")
    require_agent_access(_user(is_internal=False), source=None)  # policy unchanged for no source
    require_agent_access(_user(is_internal=False), source="client")
    for bad in (_user(is_internal=False), _user(is_superuser=False)):
        with pytest.raises(HTTPException) as exc:
            require_agent_access(bad, source="admin")
        assert exc.value.status_code == 403
    assert is_internal_staff(_user()) and not is_internal_staff(_user(is_internal=False))
    assert not is_internal_staff(None)


@pytest.mark.asyncio
async def test_fresh_check_reads_the_flag_from_the_row():
    class _Row(SimpleNamespace):
        pass

    class _DB:
        def __init__(self, row):
            self.row = row
            self.statement = None

        async def execute(self, statement):
            self.statement = statement
            row = self.row
            return SimpleNamespace(one_or_none=lambda: row)

    staff = _DB(
        _Row(
            is_active=True,
            role="admin",
            email_verified=True,
            is_approved=True,
            is_superuser=True,
            is_internal=True,
        )
    )
    await require_fresh_agent_access(staff, 7, source="admin")
    assert "is_internal" in str(staff.statement)  # the column is part of the fresh select
    outsider = _DB(
        _Row(
            is_active=True,
            role="admin",
            email_verified=True,
            is_approved=True,
            is_superuser=True,
            is_internal=False,
        )
    )
    with pytest.raises(HTTPException):
        await require_fresh_agent_access(outsider, 7, source="admin")
    await require_fresh_agent_access(outsider, 7)  # no source: unchanged policy


# --------------------------------------------------------------------------- endpoint

pytest.importorskip("claude_agent_sdk")

from app.api.v1.endpoints import brain_agent as endpoint_module  # noqa: E402
from app.core.deps import get_current_user, get_db  # noqa: E402
from app.services.brain_agent_service import BrainAgentService, SSEEvent  # noqa: E402


@pytest.fixture
def chat_client(monkeypatch):
    import app.core.redis as redis_module

    async def _allow(**_kwargs):
        return True, 0

    monkeypatch.setattr(redis_module, "check_rate_limit", _allow)
    seen: list[str] = []

    async def _stub_chat(self, **kwargs):
        seen.append(kwargs.get("source"))
        yield SSEEvent(event_type="result", data={"session_id": "stub"})

    monkeypatch.setattr(BrainAgentService, "chat", _stub_chat)
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/brain-agent")

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    with TestClient(app) as c:
        c.seen = seen
        yield c
    app.dependency_overrides.clear()


def _post(client, user, source):
    client.app.dependency_overrides[get_current_user] = lambda: user
    body = {"prompt": "hello", "session_id": str(uuid.uuid4()), "message_id": "m1"}
    if source:
        body["source"] = source
    return client.post("/brain-agent/chat", json=body)


def test_non_internal_superuser_is_downgraded_to_the_client_profile(chat_client):
    assert _post(chat_client, _user(is_internal=False), "admin").status_code == 200
    assert _post(chat_client, _user(is_internal=False), None).status_code == 200
    assert chat_client.seen == ["client", "client"]


def test_internal_superuser_keeps_the_admin_profile(chat_client):
    assert _post(chat_client, _user(), "admin").status_code == 200
    assert _post(chat_client, _user(), None).status_code == 200
    assert _post(chat_client, _user(), "client").status_code == 200  # opt-in still honoured
    assert chat_client.seen == ["admin", "admin", "client"]
