"""/scada/ledger endpoints: reader gate (internal staff or a farm-limited SCADA grant), off switch, absent tables, 404, stale, validation and the typed
document (service stubbed; the SQL is covered by test_scada_ledger_postgres.py)."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada_ledger as endpoint_module
from app.core.config import Settings
from app.core.deps import (
    get_current_active_user,
    get_current_internal_user,
    get_current_superuser,
    get_db,
)
from app.core.scada_access import get_ledger_reader
from app.services.scada_ledger_service import ScadaLedgerService, build_document
from tests.test_scada_ledger_document import close, sample

PREFIX = "/scada/ledger"


class _FakeUser:
    id = 42
    is_active = True
    is_superuser = True
    is_internal = True


def _ledger(build="rg2-b", stale=False):
    rows, run, doc = sample(build)
    return {
        "status": "stale" if stale else "ok",
        "register_run_id": run["register_run_id"],
        "current_register_run_id": "newer" if stale else run["register_run_id"],
        "register": {"window_start_year": 2025, "window_end_year": 2025, "ann_years": 1},
        "document": None if stale else build_document(rows, run),
        "statuses": [
            {
                "trigger": "OPS_01",
                "scope": "VL:downtime_fault",
                "cls": "REALIZED",
                "status": "CONFIRMED",
                "notes": None,
                "acknowledged_at": "2026-11-17T10:00:00",
                "resolved_at": None,
                "updated_at": "2026-11-17T10:00:00",
                "actor": "Ada Lovelace",
            }
        ],
    }


@pytest.fixture
def enabled(monkeypatch):
    s = Settings(SCADA_LEDGER_ENABLED=True)
    monkeypatch.setattr(endpoint_module, "get_settings", lambda: s)


@pytest.fixture
def present(monkeypatch):
    async def _present(_db):
        return True

    monkeypatch.setattr(endpoint_module, "scada_ledger_present", _present)


@pytest.fixture
def ledgers(monkeypatch, present):
    """RG2 current, HOT stale, anything else absent."""

    async def _get(self, farm, asset):
        return {"RG2": _ledger(), "HOT": _ledger("hot-b", stale=True)}.get(asset)

    async def _assets(self, farm):
        if farm != "raggovidda":
            return []
        return [
            {
                "asset_code": "RG2",
                "currency": "NOK",
                "window_from": "2025-01-01",
                "window_to": "2025-12-31",
                "status": "ok",
            }
        ]

    monkeypatch.setattr(ScadaLedgerService, "get", _get)
    monkeypatch.setattr(ScadaLedgerService, "assets", _assets)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_ledger_reader] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c


def test_returns_the_typed_document_and_statuses(client, enabled, ledgers):
    r = client.get(f"{PREFIX}/", params={"farm": "raggovidda", "asset": "RG2"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok" and body["register_run_id"] == body["current_register_run_id"]
    assert body["register"] == {"window_start_year": 2025, "window_end_year": 2025, "ann_years": 1}
    assert close(body["document"], sample("rg2-b")[2])
    assert body["statuses"][0]["scope"] == "VL:downtime_fault"


def test_a_stale_ledger_has_no_document(client, enabled, ledgers):
    body = client.get(f"{PREFIX}/", params={"farm": "hill_of_towie", "asset": "HOT"}).json()
    assert body["status"] == "stale" and body["document"] is None
    assert body["current_register_run_id"] == "newer"


def test_lists_a_farms_assets(client, enabled, ledgers):
    r = client.get(f"{PREFIX}/assets", params={"farm": "raggovidda"})
    assert r.status_code == 200
    assert r.json() == [
        {
            "asset_code": "RG2",
            "currency": "NOK",
            "window_from": "2025-01-01",
            "window_to": "2025-12-31",
            "status": "ok",
        }
    ]
    assert client.get(f"{PREFIX}/assets", params={"farm": "nowhere"}).json() == []


def test_an_asset_without_a_ledger_is_404(client, enabled, ledgers):
    r = client.get(f"{PREFIX}/", params={"farm": "raggovidda", "asset": "RG1"})
    assert r.status_code == 404


def test_absent_tables_list_nothing_and_404(client, enabled, monkeypatch):
    async def _absent(_db):
        return False

    monkeypatch.setattr(endpoint_module, "scada_ledger_present", _absent)
    assert client.get(f"{PREFIX}/assets", params={"farm": "raggovidda"}).json() == []
    assert (
        client.get(f"{PREFIX}/", params={"farm": "raggovidda", "asset": "RG2"}).status_code == 404
    )


@pytest.mark.parametrize(
    "params",
    [
        {"farm": "raggovidda"},
        {"asset": "RG2"},
        {"farm": "Raggovidda", "asset": "RG2"},
        {"farm": "raggovidda", "asset": "rg2"},
        {"farm": "x" * 65, "asset": "RG2"},
        {"farm": "raggovidda", "asset": "R" * 17},
    ],
)
def test_bad_queries_are_422(client, enabled, ledgers, params):
    assert client.get(f"{PREFIX}/", params=params).status_code == 422


def test_off_switch_is_503(client, monkeypatch, ledgers):
    s = Settings(SCADA_LEDGER_ENABLED=False)
    monkeypatch.setattr(endpoint_module, "get_settings", lambda: s)
    assert (
        client.get(f"{PREFIX}/", params={"farm": "raggovidda", "asset": "RG2"}).status_code == 503
    )
    assert client.get(f"{PREFIX}/assets", params={"farm": "raggovidda"}).status_code == 503


def test_default_is_on_everywhere_and_can_be_switched_off():
    # Stage 9 (2026-11-26): the production exception is gone.
    assert Settings(SENTRY_ENVIRONMENT="production").scada_ledger_enabled is True
    assert Settings(SENTRY_ENVIRONMENT="staging").scada_ledger_enabled is True
    assert (
        Settings(SENTRY_ENVIRONMENT="production", SCADA_LEDGER_ENABLED=False).scada_ledger_enabled
        is False
    )


def test_without_a_login_every_route_is_refused():
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)
    with TestClient(app) as c:
        assert c.get(f"{PREFIX}/", params={"farm": "x", "asset": "X"}).status_code in (401, 403)
        assert c.get(f"{PREFIX}/assets", params={"farm": "x"}).status_code in (401, 403)


def test_routes_use_the_ledger_reader_dependency():
    for route in endpoint_module.router.routes:
        deps = {d.call for d in route.dependant.dependencies}
        assert get_ledger_reader in deps, route.path
        assert get_current_internal_user not in deps
        assert get_current_superuser not in deps and get_current_active_user not in deps
