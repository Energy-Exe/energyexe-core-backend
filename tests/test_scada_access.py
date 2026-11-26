"""Per-user SCADA grant (app/core/scada_access.py): a farm-limited demo / pilot login.

* the grant is read from ``user_features`` (``scada_portal`` + ``scada_farm:<slug>``); superusers are
  never farm-limited;
* the router-level guard 404s any other farm, including the implicit default farm;
* the farm list and the all-farm charts are filtered to the scope;
* the value ledger opens for a farm-limited grant (and staff), not for a plain client;
* a farm-limited login is read-only: finding statuses and its own profile;
* ``/users/me`` returns the grant so the portal can open SCADA without making the login a superuser.
"""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada as scada_endpoints
from app.api.v1.endpoints import scada_opportunities as opp_endpoints
from app.api.v1.router import api_router
from app.core import scada_access
from app.core.deps import get_current_active_user, get_db
from app.core.scada_access import (
    ScadaGrant,
    enforce_scada_farm_scope,
    get_ledger_reader,
    grant_from_features,
    scada_farm_scope,
)
from app.core.security import get_password_hash
from app.models.user import User
from app.models.user_feature import UserFeature
from app.services import scada_service

HOT = frozenset({"hill_of_towie"})


class _Client:
    id = 7
    role = "client"
    is_active = True
    is_superuser = False
    is_internal = False
    first_name = last_name = None
    username = email = "demo"


# ---- the grant -----------------------------------------------------------------------------------


def test_grant_from_features():
    g = grant_from_features([("scada_portal", True), ("scada_farm:hill_of_towie", True)])
    assert g == ScadaGrant(can_access=True, farms=HOT)
    # a disabled row is no grant; portal access without farms is not farm-limited
    assert (
        grant_from_features([("scada_portal", True), ("scada_farm:kelmarsh", False)]).farms is None
    )
    assert grant_from_features([("scada_farm:", True)]).farms is None
    assert grant_from_features([]) == ScadaGrant(can_access=False, farms=None)


@pytest.mark.asyncio
async def test_superusers_are_never_farm_limited():
    class _Super(_Client):
        is_superuser = True

    # no DB needed: superusers skip the query
    assert await scada_access.load_scada_grant(None, _Super()) == scada_access.UNRESTRICTED


def test_every_scada_router_carries_the_guard():
    prefixes = ("/scada", "/scada/builder", "/scada/opportunities", "/scada/ledger")
    guarded = {
        r.path
        for r in api_router.routes
        if any(d.call is enforce_scada_farm_scope for d in r.dependant.dependencies)
    }
    for r in api_router.routes:
        path = getattr(r, "path", "")
        if path.startswith(prefixes) and not path.startswith("/scada/ppas"):
            assert path in guarded, path


# ---- the guard and the filters over a mounted SCADA router ---------------------------------------


@pytest.fixture
def scoped(monkeypatch):
    """A /scada + /scada/opportunities app for a login limited to Hill of Towie."""
    from fastapi import Depends

    app = FastAPI()
    guard = [Depends(enforce_scada_farm_scope)]
    app.include_router(scada_endpoints.router, prefix="/scada", dependencies=guard)
    app.include_router(opp_endpoints.router, prefix="/scada/opportunities", dependencies=guard)

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: _Client()
    app.dependency_overrides[scada_farm_scope] = lambda: HOT

    async def _present(_db):
        return True

    monkeypatch.setattr(scada_endpoints, "scada_schema_present", _present)

    async def _slugs(self):
        return ["hill_of_towie", "kelmarsh", "penmanshiel"]

    monkeypatch.setattr(scada_service.ScadaService, "farm_slugs", _slugs)
    with TestClient(app) as c:
        yield c


def test_other_farms_are_404(scoped):
    assert scoped.get("/scada/degradation", params={"farm": "kelmarsh"}).status_code == 404
    assert (
        scoped.get("/scada/opportunities/summary", params={"farm": "kelmarsh"}).status_code == 404
    )


def test_the_implicit_default_farm_is_checked_too(monkeypatch, scoped):
    scoped.app.dependency_overrides[scada_farm_scope] = lambda: frozenset({"kelmarsh"})
    # /scada/wind-index defaults farm to hill_of_towie when the query leaves it out
    assert scoped.get("/scada/wind-index").status_code == 404


def test_own_farm_passes_the_guard(monkeypatch, scoped):
    async def _wind(self, farm):
        return {"farm": farm}

    monkeypatch.setattr(scada_service.ScadaService, "wind_index", _wind)
    r = scoped.get("/scada/wind-index", params={"farm": "hill_of_towie"})
    assert r.status_code == 200 and r.json() == {"farm": "hill_of_towie"}


def test_farm_list_and_all_farm_charts_are_filtered(monkeypatch, scoped):
    async def _farms(self, include_internal=False):
        return {"farms": [{"farm": "hill_of_towie"}, {"farm": "kelmarsh"}]}

    async def _heartbeat(self, days):
        return {"hill_of_towie": {"e": [1]}, "kelmarsh": {"e": [2]}}

    async def _portfolio(self):
        return {"farms": [{"farm": "hill_of_towie"}, {"farm": "penmanshiel"}]}

    async def _annual(self, start_year, end_year):
        return {"rows": [{"farm": "hill_of_towie"}, {"farm": "kelmarsh"}], "gbp": {"2023": 1}}

    seen = {}

    async def _mix(self, farms=None):
        seen["farms"] = farms
        return []

    monkeypatch.setattr(scada_service.ScadaService, "farms", _farms)
    monkeypatch.setattr(scada_service.ScadaService, "heartbeat", _heartbeat)
    monkeypatch.setattr(scada_service.ScadaService, "portfolio", _portfolio)
    monkeypatch.setattr(scada_service.ScadaService, "annual_cost", _annual)
    monkeypatch.setattr(scada_service.ScadaService, "method_mix", _mix)

    assert [f["farm"] for f in scoped.get("/scada/farms").json()["farms"]] == ["hill_of_towie"]
    assert list(scoped.get("/scada/heartbeat").json()) == ["hill_of_towie"]
    assert [f["farm"] for f in scoped.get("/scada/portfolio").json()["farms"]] == ["hill_of_towie"]
    annual = scoped.get("/scada/annual-cost").json()
    assert [r["farm"] for r in annual["rows"]] == ["hill_of_towie"] and annual["gbp"] == {"2023": 1}
    assert scoped.get("/scada/method-mix").status_code == 200 and seen["farms"] == ["hill_of_towie"]


def test_unscoped_users_see_every_farm(monkeypatch, scoped):
    scoped.app.dependency_overrides[scada_farm_scope] = lambda: None

    async def _heartbeat(self, days):
        return {"hill_of_towie": {}, "kelmarsh": {}}

    monkeypatch.setattr(scada_service.ScadaService, "heartbeat", _heartbeat)
    assert sorted(scoped.get("/scada/heartbeat").json()) == ["hill_of_towie", "kelmarsh"]


def test_finding_status_is_read_only_for_a_limited_login(scoped):
    body = {
        "farm": "hill_of_towie",
        "trigger": "OPS_01",
        "scope": "FARM",
        "cls": "RECOVERABLE",
        "status": "ACKNOWLEDGED",
    }
    r = scoped.put("/scada/opportunities/actions", json=body)
    assert r.status_code == 403 and "read-only" in r.json()["detail"]


# ---- the value ledger reader ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ledger_reader(monkeypatch):
    class _Staff(_Client):
        is_superuser = True
        is_internal = True

    assert await get_ledger_reader(_Staff(), None) is not None

    async def _grant(db, user):
        return ScadaGrant(can_access=True, farms=HOT)

    monkeypatch.setattr(scada_access, "load_scada_grant", _grant)
    demo = _Client()
    assert await get_ledger_reader(demo, None) is demo

    async def _none(db, user):
        return ScadaGrant(can_access=False, farms=None)

    monkeypatch.setattr(scada_access, "load_scada_grant", _none)
    with pytest.raises(HTTPException) as exc:
        await get_ledger_reader(_Client(), None)
    assert exc.value.status_code == 403


# ---- /users/me against the SQLite fixture ---------------------------------------------------------


async def _seed_demo(session, farms=("hill_of_towie",)):
    user = User(
        email="demo-user@example.com",
        username="demo-user",
        hashed_password=get_password_hash("Password123!"),
        is_active=True,
        is_superuser=False,
        role="client",
        is_approved=True,
        email_verified=True,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    session.add(UserFeature(user_id=user.id, feature_key="scada_portal", enabled=True))
    for f in farms:
        session.add(UserFeature(user_id=user.id, feature_key=f"scada_farm:{f}", enabled=True))
    await session.commit()
    return user


@pytest.mark.asyncio
async def test_users_me_returns_the_grant_and_blocks_profile_edits(client, test_session):
    demo = await _seed_demo(test_session)
    client.app.dependency_overrides[get_current_active_user] = lambda: demo
    me = client.get("/api/v1/users/me")
    assert me.status_code == 200, me.text
    body = me.json()
    assert body["can_access_scada"] is True and body["scada_farms"] == ["hill_of_towie"]
    assert body["is_superuser"] is False and body["role"] == "client"
    put = client.put("/api/v1/users/me", json={"password": "Changed123!"})
    assert put.status_code == 403
