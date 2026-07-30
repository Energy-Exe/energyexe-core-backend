"""Endpoint tests for POST /brain-agent/sessions/{session_id}/files.

Builds a minimal app rather than using the shared `client` fixture: that fixture
runs the full application lifespan, which needs Postgres/Valkey and isn't
available in a unit-test environment.
"""

import json
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import brain_agent as endpoint_module
from app.core.deps import get_current_user, get_db
from app.services.brain_agent_service import BrainAgentService
from app.services.brain_agent_uploads import (
    MAX_SESSION_UPLOADS,
    MAX_UPLOAD_BYTES,
    UPLOAD_MANIFEST,
    read_upload_manifest,
)

USER_ID = 42


class _FakeUser:
    id = USER_ID
    first_name = "Test"
    company_name = None
    is_superuser = True


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Redirect the sandbox root into tmp_path and stub the S3 mirror."""
    monkeypatch.setattr(
        endpoint_module, "work_dir_for", lambda user_id, session_id: tmp_path / str(session_id)
    )

    async def _no_s3(*_args, **_kwargs):
        return None

    monkeypatch.setattr(BrainAgentService, "_upload_file_to_s3", staticmethod(_no_s3))
    return tmp_path


@pytest.fixture
def client(sandbox):
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/brain-agent")

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _sid() -> str:
    return str(uuid.uuid4())


def _post(client, sid, filename, content=b"a,b\n1,2\n"):
    return client.post(
        f"/brain-agent/sessions/{sid}/files",
        files={"file": (filename, content, "text/csv")},
    )


# ── happy path ──


def test_upload_writes_file_and_manifest(client, sandbox):
    sid = _sid()
    response = _post(client, sid, "data.csv")

    assert response.status_code == 201
    body = response.json()
    assert body["filename"] == "data.csv"
    assert body["size"] == len(b"a,b\n1,2\n")

    work_dir = sandbox / sid
    assert (work_dir / "data.csv").read_bytes() == b"a,b\n1,2\n"
    assert read_upload_manifest(work_dir) == ["data.csv"]


def test_reupload_replaces_without_duplicating_the_manifest(client, sandbox):
    sid = _sid()
    _post(client, sid, "data.csv", b"old")
    response = _post(client, sid, "data.csv", b"new content")

    assert response.status_code == 201
    work_dir = sandbox / sid
    assert (work_dir / "data.csv").read_bytes() == b"new content"
    assert read_upload_manifest(work_dir) == ["data.csv"]


# ── rejections ──


def test_executable_file_is_rejected(client, sandbox):
    """The agent runs Bash in this directory."""
    sid = _sid()
    response = _post(client, sid, "evil.py", b"import os; os.system('id')")

    assert response.status_code == 400
    assert not (sandbox / sid / "evil.py").exists()


def test_path_traversal_lands_inside_the_sandbox(client, sandbox):
    sid = _sid()
    response = _post(client, sid, "../../escape.csv")

    assert response.status_code == 201
    assert response.json()["filename"] == "escape.csv"
    assert (sandbox / sid / "escape.csv").exists()
    # Nothing written outside the session directory.
    assert not (sandbox / "escape.csv").exists()


def test_non_uuid_session_id_is_rejected(client):
    """session_id is interpolated into a path and there's no session to own it yet."""
    response = _post(client, "..%2F..%2Fetc", "data.csv")
    assert response.status_code in (400, 404)


def test_seed_file_name_is_rejected(client):
    response = _post(client, _sid(), "db.py")
    assert response.status_code == 400


def test_empty_file_is_rejected(client, sandbox):
    sid = _sid()
    response = _post(client, sid, "empty.csv", b"")

    assert response.status_code == 400
    assert not (sandbox / sid / "empty.csv").exists()


def test_oversize_file_is_rejected_and_cleaned_up(client, sandbox):
    sid = _sid()
    oversize = b"x" * (MAX_UPLOAD_BYTES + 1024)
    response = _post(client, sid, "big.csv", oversize)

    assert response.status_code == 400
    assert "larger than" in response.json()["detail"]
    # A partial write must not linger — it would count against the session cap
    # and be announced to the agent as a real attachment.
    assert not (sandbox / sid / "big.csv").exists()


def test_session_file_count_is_capped(client, sandbox):
    sid = _sid()
    for i in range(MAX_SESSION_UPLOADS):
        assert _post(client, sid, f"file{i}.csv").status_code == 201

    response = _post(client, sid, "one-too-many.csv")
    assert response.status_code == 400
    assert read_upload_manifest(sandbox / sid) == [
        f"file{i}.csv" for i in range(MAX_SESSION_UPLOADS)
    ]


def test_manifest_is_valid_json_after_several_uploads(client, sandbox):
    sid = _sid()
    _post(client, sid, "a.csv")
    _post(client, sid, "b.json", b"{}")

    stored = json.loads((sandbox / sid / UPLOAD_MANIFEST).read_text())
    assert stored == {"files": ["a.csv", "b.json"]}
