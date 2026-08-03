"""Wiring tests for /scada/* endpoints.

Builds a minimal app rather than using the shared `client` fixture (conftest's
in-memory SQLite cannot run the Postgres-only scada.* SQL). Numeric correctness
is verified live against staging (see energyexe-scada-pipeline/docs/ui/README.md
fixtures); these tests cover routing, auth, the schema-presence guard, and the
farm-slug 404.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada as endpoint_module
from app.core.deps import get_current_active_user, get_db
from app.services import scada_service


class _FakeUser:
    id = 42
    role = "client"
    is_active = True
    is_superuser = False


@pytest.fixture
def app_client(monkeypatch):
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada")

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def schema_present(monkeypatch):
    async def _present(_db):
        return True

    monkeypatch.setattr(endpoint_module, "scada_schema_present", _present)


@pytest.fixture
def known_farms(monkeypatch):
    async def _slugs(self):
        return ["hill_of_towie", "kelmarsh", "penmanshiel"]

    monkeypatch.setattr(scada_service.ScadaService, "farm_slugs", _slugs)


def test_schema_absent_returns_503(app_client, monkeypatch):
    async def _absent(_db):
        return False

    monkeypatch.setattr(endpoint_module, "scada_schema_present", _absent)
    resp = app_client.get("/scada/farms")
    assert resp.status_code == 503
    assert "not available" in resp.json()["detail"]


def test_unknown_farm_returns_404(app_client, schema_present, known_farms):
    resp = app_client.get("/scada/degradation", params={"farm": "nope"})
    assert resp.status_code == 404


def test_missing_required_params_return_422(app_client, schema_present):
    assert app_client.get("/scada/energy-waterfall").status_code == 422
    assert app_client.get("/scada/league").status_code == 422
    assert app_client.get("/scada/downtime-fingerprint").status_code == 422


def test_auth_required():
    """Without the dependency override, requests carry no credentials -> 401/403."""
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada")

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    with TestClient(app) as c:
        resp = c.get("/scada/farms")
    assert resp.status_code in (401, 403)


def test_endpoint_calls_service(app_client, schema_present, known_farms, monkeypatch):
    async def _waterfall(self, farm, start, end):
        assert farm == "hill_of_towie"
        assert start.isoformat() == "2024-01-01"
        return {"pot": 1.0, "dt": 0.1, "ct": 0.2, "pf": -0.05, "lt": 0.25, "energy": 0.75}

    monkeypatch.setattr(scada_service.ScadaService, "energy_waterfall", _waterfall)
    resp = app_client.get(
        "/scada/energy-waterfall",
        params={"farm": "hill_of_towie", "start": "2024-01-01", "end": "2024-12-31"},
    )
    assert resp.status_code == 200
    assert resp.json()["pot"] == 1.0


def test_scada_schema_present_caches_true(monkeypatch):
    """Once the schema is seen, later calls never re-query."""

    calls = {"n": 0}

    class _Result:
        def scalar(self):
            return 1

    class _Db:
        async def execute(self, *_a, **_k):
            calls["n"] += 1
            return _Result()

    import asyncio

    monkeypatch.setattr(scada_service, "_schema_seen", False)
    assert asyncio.run(scada_service.scada_schema_present(_Db())) is True
    assert asyncio.run(scada_service.scada_schema_present(_Db())) is True
    assert calls["n"] == 1
