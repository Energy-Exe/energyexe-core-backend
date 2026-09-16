"""Security regression tests without the full application's startup or live DB."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from app.api.v1.endpoints import brain_agent, map as map_endpoints, scada
from app.core.config import Settings
from app.core.deps import get_current_user, get_db
from app.core.exceptions import AuthenticationException
from app.scada_preview.service import IngestionSummaryService
from app.services.brain_agent_service import BrainAgentService
from preview_tests.test_sfe_preview import db_mock, summary  # noqa: F401


def user(**overrides):
    return SimpleNamespace(
        **dict(
            id=42,
            role="admin",
            is_active=True,
            is_superuser=False,
            email_verified=True,
            is_approved=True,
            **overrides,
        )
    )


@pytest.fixture(autouse=True)
def restricted(monkeypatch):
    monkeypatch.setenv("BRAIN_AGENT_ACCESS_POLICY", "superusers")
    monkeypatch.setenv("SCADA_INGESTION_ENABLED", "true")


def make_app(identity=None, db=None):
    app = FastAPI()
    app.include_router(brain_agent.router, prefix="/brain-agent")
    app.include_router(map_endpoints.router, prefix="/map")
    app.include_router(scada.router, prefix="/scada")
    if identity is not None:
        app.dependency_overrides[get_current_user] = lambda: identity

    async def database():
        yield db

    app.dependency_overrides[get_db] = database
    # Same authentication status as the normal application's handler.
    from fastapi.responses import JSONResponse

    @app.exception_handler(AuthenticationException)
    async def invalid_auth(request, exc):
        return JSONResponse(status_code=401, content={"detail": "Invalid credentials"})

    return app


def test_safe_defaults_and_invalid_policy(monkeypatch):
    monkeypatch.delenv("BRAIN_AGENT_ACCESS_POLICY")
    monkeypatch.delenv("SCADA_INGESTION_ENABLED")
    settings = Settings(_env_file=None)
    assert settings.SCADA_INGESTION_ENABLED is False
    assert settings.BRAIN_AGENT_ACCESS_POLICY == "authenticated"
    with pytest.raises(ValueError):
        Settings(_env_file=None, BRAIN_AGENT_ACCESS_POLICY="everyone")


@pytest.mark.parametrize("role", ["admin", "client"])
def test_all_agent_routes_and_map_deny_ordinary_users(role):
    identity = user()
    identity.role = role
    app = make_app(identity)
    routes = [route for route in app.routes if route.path.startswith("/brain-agent/")]
    assert len(routes) == 12
    with TestClient(app) as client:
        for route in routes:
            path = route.path
            for name in route.param_convertors:
                path = path.replace("{" + name + "}", "42")
            response = client.request(next(iter(route.methods)), path, json={})
            assert response.status_code == 403, (path, response.text)
        assert client.post("/map/interpret-view", json={}).status_code == 403
        assert client.get("/scada/ingestion-summary").status_code == 403


def test_missing_and_invalid_auth_keep_existing_statuses():
    with TestClient(make_app()) as client:
        for path in ["/scada/ingestion-summary", "/brain-agent/threads"]:
            assert client.get(path).status_code == 403
            assert client.get(path, headers={"Authorization": "Bearer invalid"}).status_code == 401


@pytest.mark.parametrize("run_id", [None, "run-first"])
def test_feature_disabled_blocks_current_and_historical(monkeypatch, run_id):
    monkeypatch.setenv("SCADA_INGESTION_ENABLED", "false")
    identity = user()
    identity.is_superuser = True
    db = AsyncMock()
    with TestClient(make_app(identity, db)) as client:
        response = client.get(
            "/scada/ingestion-summary", params={"run_id": run_id} if run_id else {}
        )
        assert response.status_code == 503
    db.execute.assert_not_called()


def test_superadmin_current_and_historical_summary(summary):
    identity = user()
    identity.is_superuser = True
    for run in [None, "run-first"]:
        with TestClient(make_app(identity, db_mock(summary))) as client:
            response = client.get("/scada/ingestion-summary", params={"run_id": run} if run else {})
            assert response.status_code == 200
            assert response.json() == summary


@pytest.mark.asyncio
async def test_store_selection_is_fixed_and_local_preview_explicit(summary):
    for local, expected in [
        (False, "scada_ingestion.ingestion_run"),
        (True, "scada.ingestion_run"),
    ]:
        db = db_mock(summary)
        await IngestionSummaryService(db, local_preview=local).get("lutelandet")
        assert all(expected in str(call.args[0]) for call in db.execute.call_args_list)


@pytest.mark.asyncio
async def test_invalid_and_oversized_stored_payloads_are_generic(summary, monkeypatch):
    from app.scada_preview import service as service_module

    log = MagicMock()
    monkeypatch.setattr(service_module, "logger", log)
    for value in [None, {"secret_payload": "must not be returned"}, "{" * 10]:
        db = db_mock(summary)
        result = MagicMock()
        result.mappings.return_value.one_or_none.return_value = {
            "run_id": "run-first",
            "summary": value,
        }
        present = MagicMock()
        present.scalar_one_or_none.return_value = "present"
        db.execute.side_effect = [present, result]
        with pytest.raises(HTTPException) as exc:
            await IngestionSummaryService(db).get("lutelandet")
        assert exc.value.status_code == 503
        assert exc.value.detail == "Measured ingestion data not available"
        query, params = db.execute.call_args.args
        assert "octet_length" in str(query) and params["max_bytes"] == 2_000_000
        log.warning.assert_called_with(
            "ingestion_summary_invalid", farm="lutelandet", run_id="run-first"
        )


@pytest.mark.asyncio
async def test_store_failure_returns_503_and_optional_metadata_uses_savepoint():
    db = AsyncMock(begin_nested=MagicMock(return_value=AsyncMock()))
    db.execute.side_effect = SQLAlchemyError("sensitive query or payload")
    service = IngestionSummaryService(db)
    with pytest.raises(HTTPException) as exc:
        await service.get("lutelandet")
    assert exc.value.status_code == 503
    assert "sensitive" not in exc.value.detail
    assert await service.farms() == {"farms": []}
    db.begin_nested.assert_called_once()


@pytest.mark.parametrize("enabled,superuser", [(False, True), (True, False), (True, True)])
def test_farm_metadata_gate_and_legacy_availability(monkeypatch, enabled, superuser):
    identity = user()
    identity.is_superuser = superuser
    monkeypatch.setenv("SCADA_INGESTION_ENABLED", str(enabled))
    legacy = {"farms": []}
    monkeypatch.setattr(
        scada,
        "_service",
        AsyncMock(return_value=SimpleNamespace(farms=AsyncMock(return_value=legacy))),
    )
    metadata = AsyncMock(return_value={"farms": []})
    monkeypatch.setattr(IngestionSummaryService, "farms", metadata)
    with TestClient(make_app(identity)) as client:
        assert client.get("/scada/farms").json() == legacy
    assert metadata.await_count == int(enabled and superuser)


@pytest.mark.parametrize(
    "kind", ["missing", "inactive", "admin", "client", "unapproved", "unverified", "db_failure"]
)
@pytest.mark.asyncio
async def test_direct_chat_denies_before_cached_sessions(monkeypatch, kind):
    identity = user()
    if kind == "missing":
        identity = None
    elif kind == "inactive":
        identity.is_active = False
        identity.is_superuser = True
    elif kind in {"client", "unapproved", "unverified"}:
        identity.role = "client"
        if kind == "unapproved":
            identity.is_approved = False
            identity.is_superuser = True
        if kind == "unverified":
            identity.email_verified = False
            identity.is_superuser = True
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(one_or_none=lambda: identity)
    if kind == "db_failure":
        db.execute.side_effect = SQLAlchemyError("database unavailable")
    cached = {"cached-session": object()}
    monkeypatch.setattr(BrainAgentService, "_sessions", cached)
    cleanup = MagicMock(side_effect=AssertionError("must not touch caches"))
    monkeypatch.setattr(BrainAgentService, "_cleanup_stale_sessions", cleanup)
    stream = BrainAgentService(db).chat(42, "cached-session", "hello")
    with pytest.raises(SQLAlchemyError if kind == "db_failure" else HTTPException):
        await anext(stream)
    cleanup.assert_not_called()
    assert BrainAgentService._sessions == cached
    assert "users.is_active" in str(db.execute.call_args.args[0])


@pytest.mark.parametrize("policy,superuser", [("superusers", True), ("authenticated", False)])
@pytest.mark.asyncio
async def test_authorized_direct_chat_passes_fresh_lookup(monkeypatch, policy, superuser):
    monkeypatch.setenv("BRAIN_AGENT_ACCESS_POLICY", policy)
    identity = user()
    identity.is_superuser = superuser
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(one_or_none=lambda: identity)
    cleanup = MagicMock(side_effect=RuntimeError("authorization passed"))
    monkeypatch.setattr(BrainAgentService, "_cleanup_stale_sessions", cleanup)
    with pytest.raises(RuntimeError, match="authorization passed"):
        await anext(BrainAgentService(db).chat(42, "cached", "hello"))
    db.execute.assert_awaited_once()
    cleanup.assert_called_once()


def test_normal_map_data_remains_available_to_ordinary_users(monkeypatch):
    async def scores(self, **kwargs):
        return {
            "period_type": "yearly",
            "period_year": 2025,
            "period_month": None,
            "scores": [],
            "coverage": {
                key: 0
                for key in [
                    "total_count",
                    "commercial_count",
                    "generation_count",
                    "no_count",
                    "no_with_generation_data",
                    "no_coverage_pct",
                    "uk_count",
                    "uk_with_generation_data",
                    "uk_coverage_pct",
                ]
            },
        }

    monkeypatch.setattr(map_endpoints.MapPerformanceService, "get_scores", scores)
    with TestClient(make_app(user())) as client:
        response = client.get("/map/performance-scores", params={"year": 2025})
        assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_invalid_metadata_does_not_hide_valid_or_legacy_farms(summary, monkeypatch):
    db = db_mock(summary)
    farms = MagicMock()
    farms.scalars.return_value.all.return_value = ["broken", "lutelandet"]
    db.execute.side_effect = [next(iter(db.execute.side_effect)), farms]
    service = IngestionSummaryService(db)
    from app.scada_preview.schemas import IngestionSummary

    monkeypatch.setattr(
        service,
        "_read",
        AsyncMock(
            side_effect=[
                HTTPException(503, "Measured ingestion data not available"),
                IngestionSummary.model_validate(summary),
            ]
        ),
    )
    result = await service.farms()
    assert [item["farm"] for item in result["farms"]] == ["lutelandet"]
