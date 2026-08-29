"""Wiring tests for /scada/opportunities/* endpoints.

Minimal app (conftest's in-memory SQLite cannot run the Postgres-only scada.* SQL). Numeric
correctness is verified live against the loaded scada schema (43 rows / 779,922); these cover
routing, auth, the findings-tables guard, the farm 404, and the {items,total,summary} envelope.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada_opportunities as endpoint_module
from app.core.deps import get_current_active_user, get_db
from app.services import scada_opportunity_service


class _FakeUser:
    id = 42
    role = "client"
    is_active = True
    is_superuser = True
    first_name = "Ada"
    last_name = "Lovelace"
    username = "admin"
    email = "admin@energyexe.com"


_SUMMARY = {
    "farm": "hill_of_towie",
    "run_id": "decade-x",
    "site_name": "Hill of Towie",
    "generated_at": "2026-08-27T10:00:00+00:00",
    "window_start_year": 2016,
    "window_end_year": 2026,
    "ann_years": 10,
    "realized_gbp_year": 329280.0,
    "recoverable_gbp_year": 72019.0,
    "curtailment_gbp_year": 378623.0,
    "headline_gbp_year": 779922.0,
    "n_rows": 43,
    "register_version": "v1",
    "by_class": {
        "CURTAILMENT": {"count": 2, "gbp_year": 378623.0},
        "REALIZED": {"count": 7, "gbp_year": 329280.0},
    },
}
_ITEM = {
    "farm": "hill_of_towie",
    "id": 0,
    "run_id": "decade-x",
    "trigger": "MKT_03",
    "trigger_name": "Curtailment Value Recovery",
    "domain": "MKT",
    "action_type": None,
    "persona_primary": None,
    "commercial_upside": None,
    "lead_time": None,
    "scope": "FLEET",
    "scope_kind": "FLEET",
    "item": "Curtailment value forgone (net)",
    "status": None,
    "cls": "CURTAILMENT",
    "basis": "GBP/yr",
    "gbp_year": 378623.0,
    "cond_mean_lo": None,
    "cond_mean_hi": None,
    "cond_worst_hi": None,
    "cond_worst_month": None,
    "value_of_acting_early": None,
    "additive": True,
    "confidence": "MEASURED",
    "note": "",
    "now_costing_gbp": None,
    "now_floor_gbp": None,
    "now_basis": None,
    "now_available": None,
    "now_confounded": None,
    "rank_gbp": 378623.0,
}


@pytest.fixture
def app_client():
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada/opportunities")

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def present(monkeypatch):
    async def _present(_db):
        return True

    monkeypatch.setattr(endpoint_module, "scada_opportunities_present", _present)


@pytest.fixture
def service(monkeypatch, present):
    svc = scada_opportunity_service.ScadaOpportunityService

    async def _slugs(self):
        return ["hill_of_towie"]

    async def _list(self, farm, **kw):
        return {"items": [_ITEM], "total": 1, "summary": _SUMMARY}

    async def _summary(self, farm):
        return _SUMMARY

    async def _get(self, farm, id):
        return _ITEM if id == 0 else None

    async def _triggers(self):
        return [{"code": "OPS_01", "name": "Priced Downtime Losses", "domain": "OPS"}]

    monkeypatch.setattr(svc, "farm_slugs", _slugs)
    monkeypatch.setattr(svc, "list_opportunities", _list)
    monkeypatch.setattr(svc, "summary", _summary)
    monkeypatch.setattr(svc, "get", _get)
    monkeypatch.setattr(svc, "triggers", _triggers)


def test_tables_absent_returns_503(app_client, monkeypatch):
    async def _absent(_db):
        return False

    monkeypatch.setattr(endpoint_module, "scada_opportunities_present", _absent)
    resp = app_client.get("/scada/opportunities/")
    assert resp.status_code == 503
    assert "not available" in resp.json()["detail"]


def test_list_envelope(app_client, service):
    resp = app_client.get("/scada/opportunities/", params={"farm": "hill_of_towie"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["trigger"] == "MKT_03"
    assert body["summary"]["headline_gbp_year"] == 779922.0
    assert body["summary"]["by_class"]["CURTAILMENT"]["count"] == 2


def test_unknown_farm_returns_404(app_client, service):
    resp = app_client.get("/scada/opportunities/", params={"farm": "nope"})
    assert resp.status_code == 404


def test_summary_and_triggers(app_client, service):
    assert (
        app_client.get("/scada/opportunities/summary", params={"farm": "hill_of_towie"}).status_code
        == 200
    )
    trg = app_client.get("/scada/opportunities/triggers")
    assert trg.status_code == 200 and trg.json()[0]["code"] == "OPS_01"


def test_detail_404_for_missing_id(app_client, service):
    assert (
        app_client.get("/scada/opportunities/0", params={"farm": "hill_of_towie"}).status_code
        == 200
    )
    assert (
        app_client.get("/scada/opportunities/999", params={"farm": "hill_of_towie"}).status_code
        == 404
    )


def test_auth_required():
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada/opportunities")

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    with TestClient(app) as c:
        assert c.get("/scada/opportunities/").status_code in (401, 403)


# --- Phase 7b: PUT /actions (finding lifecycle) ---

from types import SimpleNamespace  # noqa: E402

from app.services import scada_finding_service  # noqa: E402


def _fake_action(status="ACKNOWLEDGED", notes="looks real"):
    return SimpleNamespace(
        farm="hill_of_towie",
        trigger="DET_aux",
        scope="T03",
        cls="CONDITIONAL",
        status=status,
        notes=notes,
        acknowledged_at=None,
        resolved_at=None,
        updated_at=None,
    )


@pytest.fixture
def finding(monkeypatch, present):
    """set_action returns a persisted row for the known key, None otherwise."""

    async def _set_action(self, *, farm, trigger, scope, cls, status, notes, user_id):
        if scope == "T03":
            return _fake_action(status=status, notes=notes)
        return None

    monkeypatch.setattr(scada_finding_service.ScadaFindingService, "set_action", _set_action)


_ACTION_BODY = {
    "farm": "hill_of_towie",
    "trigger": "DET_aux",
    "scope": "T03",
    "cls": "CONDITIONAL",
    "status": "ACKNOWLEDGED",
    "notes": "looks real",
}


def test_action_bad_status_returns_400(app_client, present):
    body = {**_ACTION_BODY, "status": "BOGUS"}
    resp = app_client.put("/scada/opportunities/actions", json=body)
    assert resp.status_code == 400
    assert "status must be one of" in resp.json()["detail"]


def test_action_upsert_ok_records_actor(app_client, finding):
    resp = app_client.put("/scada/opportunities/actions", json=_ACTION_BODY)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ACKNOWLEDGED"
    assert body["scope"] == "T03"
    assert body["actor"] == "Ada Lovelace"  # derived from the auth token, not the request


def test_action_unknown_finding_returns_404(app_client, finding):
    body = {**_ACTION_BODY, "scope": "T99"}  # no register row → set_action returns None
    resp = app_client.put("/scada/opportunities/actions", json=body)
    assert resp.status_code == 404


def test_action_tables_absent_returns_503(app_client, monkeypatch):
    async def _absent(_db):
        return False

    monkeypatch.setattr(endpoint_module, "scada_opportunities_present", _absent)
    resp = app_client.put("/scada/opportunities/actions", json=_ACTION_BODY)
    assert resp.status_code == 503
