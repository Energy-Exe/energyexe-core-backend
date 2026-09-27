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


def test_viz_charts_all_registered(app_client, schema_present, known_farms, monkeypatch):
    """Every loop-registered viz chart routes to its service method with farm+year."""
    for path, method in endpoint_module._FARM_YEAR_CHARTS.items():

        async def _fake(self, farm, year, **kwargs):
            return {"ok": True}

        monkeypatch.setattr(scada_service.ScadaService, method, _fake)
        resp = app_client.get(f"/scada/{path}", params={"farm": "kelmarsh", "year": 2023})
        assert resp.status_code == 200, f"{path} -> {resp.status_code}"
        assert resp.json() == {"ok": True}, path


def test_viz_charts_require_year(app_client, schema_present):
    assert app_client.get("/scada/outage-gantt").status_code == 422
    assert app_client.get("/scada/temp-cohort").status_code == 422


def test_viz_farm_scoped_charts(app_client, schema_present, known_farms, monkeypatch):
    for path, method in (
        ("wind-index", "wind_index"),
        ("self-consumption", "self_consumption"),
        ("midwind-fade", "midwind_fade"),
        ("replay-days", "replay_days"),
    ):

        async def _fake(self, farm):
            return {"farm_seen": farm}

        monkeypatch.setattr(scada_service.ScadaService, method, _fake)
        resp = app_client.get(f"/scada/{path}", params={"farm": "penmanshiel"})
        assert resp.status_code == 200, path
        assert resp.json() == {"farm_seen": "penmanshiel"}, path


def test_replay_takes_farm_and_day(app_client, schema_present, known_farms, monkeypatch):
    async def _fake(self, farm, day):
        return {"day_seen": day.isoformat()}

    monkeypatch.setattr(scada_service.ScadaService, "replay", _fake)
    resp = app_client.get("/scada/replay", params={"farm": "hill_of_towie", "day": "2025-06-23"})
    assert resp.status_code == 200
    assert resp.json() == {"day_seen": "2025-06-23"}
    assert app_client.get("/scada/replay", params={"farm": "hill_of_towie"}).status_code == 422


def test_portfolio_needs_no_farm(app_client, schema_present, monkeypatch):
    async def _fake(self):
        return {"farms": []}

    monkeypatch.setattr(scada_service.ScadaService, "portfolio", _fake)
    resp = app_client.get("/scada/portfolio")
    assert resp.status_code == 200
    assert resp.json() == {"farms": []}


def test_turbine_charts_all_registered(app_client, schema_present, known_farms, monkeypatch):
    """Every turbine-dossier chart routes to its service method."""
    for path, method in endpoint_module._TURBINE_YEAR_CHARTS.items():

        async def _fake(self, farm, turbine, year, **kwargs):
            return {"t_seen": turbine}

        monkeypatch.setattr(scada_service.ScadaService, method, _fake)
        resp = app_client.get(
            f"/scada/{path}",
            params={"farm": "hill_of_towie", "turbine": "T07", "year": 2024},
        )
        assert resp.status_code == 200, f"{path} -> {resp.status_code}"
        assert resp.json() == {"t_seen": "T07"}, path

    for path, method in endpoint_module._TURBINE_CHARTS.items():

        async def _fake_life(self, farm, turbine, **kwargs):
            return {"t_seen": turbine}

        monkeypatch.setattr(scada_service.ScadaService, method, _fake_life)
        resp = app_client.get(f"/scada/{path}", params={"farm": "hill_of_towie", "turbine": "T07"})
        assert resp.status_code == 200, f"{path} -> {resp.status_code}"
        assert resp.json() == {"t_seen": "T07"}, path


def test_turbine_charts_require_turbine(app_client, schema_present):
    assert app_client.get("/scada/turbine-summary", params={"year": 2024}).status_code == 422
    assert app_client.get("/scada/turbine-life").status_code == 422
    assert app_client.get("/scada/turbine-timers", params={"turbine": "T07"}).status_code == 422


def test_turbines_list(app_client, schema_present, known_farms, monkeypatch):
    async def _fake(self, farm):
        return {"turbines": [{"t": "T01"}]}

    monkeypatch.setattr(scada_service.ScadaService, "turbines", _fake)
    resp = app_client.get("/scada/turbines", params={"farm": "hill_of_towie"})
    assert resp.status_code == 200
    assert resp.json()["turbines"][0]["t"] == "T01"


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


# --- SFE measured delivery: internal-staff boundary (is_superuser AND is_internal) ---


class _Superuser(_FakeUser):
    role = "admin"
    is_superuser = True
    is_internal = False


class _InternalSuperuser(_Superuser):
    is_internal = True


class _IngestionOn:
    SCADA_INGESTION_ENABLED = True


@pytest.fixture
def ingestion_on(monkeypatch):
    monkeypatch.setattr(endpoint_module, "get_settings", lambda: _IngestionOn())

    async def _legacy_farms(self):
        return {"farms": [{"farm": "hill_of_towie", "name": "Hill of Towie"}]}

    async def _measured_farms(self):
        return {"farms": [{"farm": "lutelandet", "ingestion": {"profile": "measured_native"}}]}

    async def _summary(self, farm, run_id=None):
        # Reaching the service is the proof the gate let the caller through; a 503 keeps the
        # response valid without building a full IngestionSummary payload.
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail=f"stub reached for {farm}")

    monkeypatch.setattr(scada_service.ScadaService, "farms", _legacy_farms)
    monkeypatch.setattr(endpoint_module.IngestionSummaryService, "farms", _measured_farms)
    monkeypatch.setattr(endpoint_module.IngestionSummaryService, "get", _summary)


def _client_as(user_cls):
    from app.core.deps import get_current_superuser

    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada")

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: user_cls()
    app.dependency_overrides[get_current_superuser] = lambda: user_cls()
    return TestClient(app)


def test_farms_merge_measured_profile_for_internal_staff_only(schema_present, ingestion_on):
    with _client_as(_InternalSuperuser) as c:
        farms = {row["farm"]: row for row in c.get("/scada/farms").json()["farms"]}
    assert farms["lutelandet"]["ingestion"]["profile"] == "measured_native"

    with _client_as(_Superuser) as c:
        farms = {row["farm"]: row for row in c.get("/scada/farms").json()["farms"]}
    assert "lutelandet" not in farms
    assert "ingestion" not in farms["hill_of_towie"]

    with _client_as(_FakeUser) as c:
        farms = {row["farm"]: row for row in c.get("/scada/farms").json()["farms"]}
    assert list(farms) == ["hill_of_towie"]


def test_ingestion_summary_requires_internal_staff(ingestion_on):
    with _client_as(_InternalSuperuser) as c:
        response = c.get("/scada/ingestion-summary?farm=lutelandet")
    assert response.status_code == 503
    assert "stub reached for lutelandet" in response.text

    with _client_as(_Superuser) as c:
        response = c.get("/scada/ingestion-summary?farm=lutelandet")
    assert response.status_code == 403
    assert "Internal EnergyExe access required" in response.text
