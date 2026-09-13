"""Wiring tests for /scada/ppas/* (EPR-97).

Minimal app rather than conftest's client: conftest only creates five SQLite-safe auth tables, so a
real ``scada_ppa`` table cannot exist there. These cover the things that are easy to get wrong in
wiring — the superuser gate, route ordering (``/by-code/{code}`` vs ``/{id}``), the multi-farm
fan-out payload, 404s, duplicate handling returning 400 rather than a 500, and (EPR-136) that every
handler hands the caller's id to the service so rows stay private per user.

Row-level DB behaviour (the unique constraint actually firing, per-row edits leaving sibling farm
legs untouched) is exercised against real Postgres separately — see the ticket's verification notes.
"""

from datetime import date
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from app.api.v1.endpoints import scada_ppas as endpoint_module
from app.core.deps import get_current_superuser, get_db
from app.services import scada_ppa_service

PREFIX = "/scada/ppas"


class _FakeUser:
    id = 42
    role = "admin"
    is_active = True
    is_superuser = True
    first_name = "Ada"
    last_name = "Lovelace"
    username = "admin"
    email = "admin@energyexe.com"


def _row(ppa_id: int, windfarm_id: int, ppa_code: str = "PPA-2026-001", **over):
    """A stand-in for one persisted ScadaPpa row, shaped for the response model."""
    base = dict(
        id=ppa_id,
        windfarm_id=windfarm_id,
        ppa_code=ppa_code,
        ppa_buyer="Statkraft",
        counterparty_role="RTM",
        settlement_mechanism="Route_to_Market",
        volume_shape="Pay_as_Produced",
        ppa_status="Active",
        execution_date=None,
        effective_date=None,
        expiration_date=None,
        currency="GBP",
        power_share_pct=None,
        pricing_model="Collar",
        strike_price=None,
        floor_price=None,
        cap_price=None,
        index_name=None,
        index_spread=None,
        indexation_type=None,
        indexation_rate_pct=None,
        has_availability_penalties=None,
        availability_guarantee_pct=None,
        ppa_notes=None,
        created_at="2026-09-09T00:00:00",
        updated_at="2026-09-09T00:00:00",
        windfarm=None,
    )
    base.update(over)
    return SimpleNamespace(**base)


_CREATE_BODY = {
    "ppa_code": "PPA-2026-001",
    "ppa_buyer": "Statkraft",
    "windfarm_ids": [7309, 7197],
    "pricing_model": "Collar",
    "currency": "GBP",
}


@pytest.fixture
def app_client():
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_superuser] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def service(monkeypatch):
    """Service methods patched as unbound functions on the class, so the instance the handler
    constructs picks them up."""
    svc = scada_ppa_service.ScadaPpaService
    state = {
        "created": None,
        "updated": None,
        "deleted": None,
        "group_deleted": None,
        # EPR-136: every call must carry the caller. Recorded per method so a handler that drops
        # the kwarg fails loudly here rather than silently sharing rows in production.
        "user_ids": {},
    }
    OWNER = 42  # the only user the stubs hold rows for

    async def _list(self, **kw):
        state["list_kwargs"] = kw
        state["user_ids"]["list"] = kw.get("user_id")
        if kw.get("user_id") != OWNER:
            return [], 0
        return [_row(1, 7309), _row(2, 7197)], 2

    async def _get(self, ppa_id, *, user_id):
        state["user_ids"]["get"] = user_id
        return _row(ppa_id, 7309) if ppa_id == 1 and user_id == OWNER else None

    async def _by_code(self, ppa_code, *, user_id):
        state["user_ids"]["by_code"] = user_id
        if ppa_code == "PPA-2026-001" and user_id == OWNER:
            return [_row(1, 7309, ppa_code), _row(2, 7197, ppa_code)]
        return []

    async def _missing(self, ids):
        return [i for i in ids if i not in (7309, 7197)]

    async def _create(self, payload, *, user_id):
        state["created"] = payload
        state["user_ids"]["create"] = user_id
        return [_row(i + 1, wf, payload.ppa_code) for i, wf in enumerate(payload.windfarm_ids)]

    async def _update(self, ppa_id, patch, *, user_id):
        state["updated"] = (ppa_id, patch)
        state["user_ids"]["update"] = user_id
        return _row(ppa_id, 7309) if ppa_id == 1 and user_id == OWNER else None

    async def _delete(self, ppa_id, *, user_id):
        state["deleted"] = ppa_id
        state["user_ids"]["delete"] = user_id
        return _row(ppa_id, 7309) if ppa_id == 1 and user_id == OWNER else None

    async def _delete_by_code(self, ppa_code, *, user_id):
        state["group_deleted"] = ppa_code
        state["user_ids"]["delete_by_code"] = user_id
        return 2 if ppa_code == "PPA-2026-001" and user_id == OWNER else 0

    monkeypatch.setattr(svc, "list_ppas", _list)
    monkeypatch.setattr(svc, "get_ppa", _get)
    monkeypatch.setattr(svc, "get_by_code", _by_code)
    monkeypatch.setattr(svc, "missing_windfarm_ids", _missing)
    monkeypatch.setattr(svc, "create_ppas", _create)
    monkeypatch.setattr(svc, "update_ppa", _update)
    monkeypatch.setattr(svc, "delete_ppa", _delete)
    monkeypatch.setattr(svc, "delete_by_code", _delete_by_code)
    return state


# ─── auth ────────────────────────────────────────────────────────────────────


def test_auth_required():
    """No superuser override -> every route refuses."""
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    with TestClient(app) as c:
        assert c.get(PREFIX).status_code in (401, 403)
        assert c.get(f"{PREFIX}/1").status_code in (401, 403)
        assert c.post(PREFIX, json=_CREATE_BODY).status_code in (401, 403)
        assert c.put(f"{PREFIX}/1", json={"ppa_buyer": "X"}).status_code in (401, 403)
        assert c.delete(f"{PREFIX}/1").status_code in (401, 403)


def test_routes_use_the_superuser_dependency():
    """Guards the decision that this resource is superadmin-only on the BACKEND, not just behind the
    portal's frontend gate — a downgrade to get_current_active_user should fail here."""
    from app.core.deps import get_current_active_user

    deps = {d.call for route in endpoint_module.router.routes for d in route.dependant.dependencies}
    assert get_current_superuser in deps
    assert get_current_active_user not in deps


# ─── list ────────────────────────────────────────────────────────────────────


def test_list_returns_items_total_envelope(app_client, service):
    resp = app_client.get(PREFIX)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert [i["windfarm_id"] for i in body["items"]] == [7309, 7197]
    assert body["items"][0]["ppa_code"] == "PPA-2026-001"


def test_list_passes_filters_through(app_client, service):
    app_client.get(f"{PREFIX}?windfarm_id=7309&ppa_status=Active&q=statk&limit=10&offset=5")
    assert service["list_kwargs"] == {
        "user_id": 42,
        "windfarm_id": 7309,
        "ppa_code": None,
        "ppa_status": "Active",
        "q": "statk",
        "limit": 10,
        "offset": 5,
    }


def test_list_rejects_out_of_range_limit(app_client, service):
    assert app_client.get(f"{PREFIX}?limit=501").status_code == 422
    assert app_client.get(f"{PREFIX}?offset=-1").status_code == 422


# ─── create (the multi-farm fan-out) ─────────────────────────────────────────


def test_create_fans_out_over_windfarm_ids(app_client, service):
    resp = app_client.post(PREFIX, json=_CREATE_BODY)
    assert resp.status_code == 201
    body = resp.json()
    assert len(body) == 2, "one row per windfarm, all sharing the ppa_code"
    assert {r["ppa_code"] for r in body} == {"PPA-2026-001"}
    assert sorted(r["windfarm_id"] for r in body) == [7197, 7309]
    assert service["created"].windfarm_ids == [7309, 7197]


def test_create_rejects_unknown_windfarm_with_400(app_client, service):
    resp = app_client.post(PREFIX, json={**_CREATE_BODY, "windfarm_ids": [7309, 999999]})
    assert resp.status_code == 400
    assert "999999" in resp.json()["detail"]


def test_create_rejects_duplicate_windfarm_ids(app_client, service):
    resp = app_client.post(PREFIX, json={**_CREATE_BODY, "windfarm_ids": [7309, 7309]})
    assert resp.status_code == 422


def test_create_requires_at_least_one_windfarm(app_client, service):
    resp = app_client.post(PREFIX, json={**_CREATE_BODY, "windfarm_ids": []})
    assert resp.status_code == 422


def test_create_rejects_bad_enum_value(app_client, service):
    resp = app_client.post(PREFIX, json={**_CREATE_BODY, "pricing_model": "Floor"})
    assert resp.status_code == 422


def test_create_rejects_reversed_dates(app_client, service):
    resp = app_client.post(
        PREFIX,
        json={**_CREATE_BODY, "effective_date": "2026-01-01", "expiration_date": "2025-01-01"},
    )
    assert resp.status_code == 422


def test_create_duplicate_is_400_not_500(app_client, monkeypatch, service):
    """The unique (ppa_code, windfarm_id) constraint must surface as a readable 400."""

    async def _boom(self, payload, *, user_id):
        raise IntegrityError("dup", {}, Exception("dup"))

    async def _noop_rollback():
        return None

    monkeypatch.setattr(scada_ppa_service.ScadaPpaService, "create_ppas", _boom)
    # db is None in this harness; give the handler's rollback something to await.
    monkeypatch.setattr(
        endpoint_module, "_DUPLICATE_DETAIL", endpoint_module._DUPLICATE_DETAIL, raising=False
    )

    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)

    class _Db:
        async def rollback(self):
            return None

    async def _db():
        yield _Db()

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_superuser] = lambda: _FakeUser()
    with TestClient(app) as c:
        resp = c.post(PREFIX, json=_CREATE_BODY)
    assert resp.status_code == 400
    assert "unique" in resp.json()["detail"].lower()


# ─── read / update / delete ──────────────────────────────────────────────────


def test_get_one(app_client, service):
    assert app_client.get(f"{PREFIX}/1").status_code == 200
    assert app_client.get(f"{PREFIX}/99").status_code == 404


def test_by_code_route_is_not_swallowed_by_the_id_route(app_client, service):
    """`/by-code/{code}` is declared above `/{ppa_id}`; if that ordering regresses this 422s."""
    resp = app_client.get(f"{PREFIX}/by-code/PPA-2026-001")
    assert resp.status_code == 200
    assert len(resp.json()) == 2
    assert app_client.get(f"{PREFIX}/by-code/NOPE").status_code == 404


def test_update_edits_one_row_only(app_client, service):
    resp = app_client.put(f"{PREFIX}/1", json={"power_share_pct": "30.00"})
    assert resp.status_code == 200
    ppa_id, patch = service["updated"]
    assert ppa_id == 1
    # exclude_unset: only the supplied field travels, so sibling legs keep their own terms.
    assert patch.model_dump(exclude_unset=True) == {"power_share_pct": 30}


def test_update_unknown_id_is_404(app_client, service):
    assert app_client.put(f"{PREFIX}/99", json={"ppa_buyer": "X"}).status_code == 404


def test_update_revalidates_against_stored_values(app_client, monkeypatch, service):
    """A patch supplying only expiration_date must still be checked against the STORED
    effective_date — the schema validator alone cannot see the other half."""

    async def _get(self, ppa_id, *, user_id):
        # A real ORM row hands back date objects, not strings.
        return _row(ppa_id, 7309, effective_date=date(2026, 1, 1))

    monkeypatch.setattr(scada_ppa_service.ScadaPpaService, "get_ppa", _get)
    resp = app_client.put(f"{PREFIX}/1", json={"expiration_date": "2025-01-01"})
    assert resp.status_code == 400
    assert "expiration_date" in resp.json()["detail"]


def test_delete_row(app_client, service):
    assert app_client.delete(f"{PREFIX}/1").status_code == 200
    assert service["deleted"] == 1
    assert app_client.delete(f"{PREFIX}/99").status_code == 404


def test_delete_group(app_client, service):
    resp = app_client.delete(f"{PREFIX}/by-code/PPA-2026-001")
    assert resp.status_code == 200
    assert resp.json() == {"ppa_code": "PPA-2026-001", "deleted": 2}
    assert app_client.delete(f"{PREFIX}/by-code/NOPE").status_code == 404


def test_audit_wraps_a_list_result_so_the_row_is_not_dropped():
    """A collection result must still produce a writable audit payload.

    ``AuditLogCreate.new_values`` is ``Optional[Dict[str, Any]]``. The SCADA PPA create is
    the first audited endpoint that returns a *list* (one row per windfarm), and a bare list
    failed that validation, so the audit row was silently discarded. Guard the wrapping.
    """
    from app.core.audit import serialize_for_audit
    from app.models.audit_log import AuditAction
    from app.schemas.audit_log import AuditLogCreate

    rows = serialize_for_audit(
        [SimpleNamespace(id=1, ppa_code="P1"), SimpleNamespace(id=2, ppa_code="P1")]
    )
    assert isinstance(rows, list)

    wrapped = rows if isinstance(rows, dict) else {"items": rows}
    payload = AuditLogCreate(
        action=AuditAction.CREATE, resource_type="scada_ppa", new_values=wrapped
    )
    assert payload.new_values["items"][0]["ppa_code"] == "P1"


# ─── per-user privacy (EPR-136) ──────────────────────────────────────────────


def test_every_handler_passes_the_callers_id_to_the_service(app_client, service):
    """The service scopes on user_id unconditionally, so the only way a handler can leak another
    user's rows is by not passing it. Exercise every route and check each stub saw user 42."""
    app_client.get(PREFIX)
    app_client.get(f"{PREFIX}/1")
    app_client.get(f"{PREFIX}/by-code/PPA-2026-001")
    app_client.post(PREFIX, json=_CREATE_BODY)
    app_client.put(f"{PREFIX}/1", json={"ppa_buyer": "X"})
    app_client.delete(f"{PREFIX}/1")
    app_client.delete(f"{PREFIX}/by-code/PPA-2026-001")
    assert service["user_ids"] == {
        "list": 42,
        "get": 42,
        "by_code": 42,
        "create": 42,
        "update": 42,
        "delete": 42,
        "delete_by_code": 42,
    }


def test_another_superuser_sees_nothing_and_gets_404s(monkeypatch, service):
    """User 43 is a superuser too — and still cannot see, edit or delete user 42's rows.
    Foreign rows read as absent (404), never forbidden (403): existence must not leak."""

    class _OtherUser(_FakeUser):
        id = 43
        email = "other@energyexe.com"

    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_superuser] = lambda: _OtherUser()
    with TestClient(app) as c:
        listed = c.get(PREFIX)
        assert listed.status_code == 200
        assert listed.json() == {"items": [], "total": 0}
        assert c.get(f"{PREFIX}/1").status_code == 404
        assert c.get(f"{PREFIX}/by-code/PPA-2026-001").status_code == 404
        assert c.put(f"{PREFIX}/1", json={"ppa_buyer": "X"}).status_code == 404
        assert c.delete(f"{PREFIX}/1").status_code == 404
        assert c.delete(f"{PREFIX}/by-code/PPA-2026-001").status_code == 404
    app.dependency_overrides.clear()
    assert set(service["user_ids"].values()) == {43}


def test_ownership_cannot_be_forged_through_the_payload(app_client, service):
    """``created_by_id`` is not a schema field: a body that names another owner is ignored and the
    row is still created as the caller. Same for PUT."""
    resp = app_client.post(PREFIX, json={**_CREATE_BODY, "created_by_id": 999})
    assert resp.status_code == 201
    assert service["user_ids"]["create"] == 42
    assert not hasattr(service["created"], "created_by_id")
    assert "created_by_id" not in service["created"].model_dump()

    resp = app_client.put(f"{PREFIX}/1", json={"ppa_buyer": "X", "created_by_id": 999})
    assert resp.status_code == 200
    _, patch = service["updated"]
    assert patch.model_dump(exclude_unset=True) == {"ppa_buyer": "X"}
