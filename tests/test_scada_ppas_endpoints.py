"""Wiring tests for /scada/ppas/* (EPR-97).

Minimal app rather than conftest's client: conftest only creates five SQLite-safe auth tables, so a
real ``scada_ppa`` table cannot exist there. These cover the things that are easy to get wrong in
wiring — the internal-user gate (EPR-143: superuser AND is_internal), route ordering
(``/by-code/{code}`` vs ``/{id}``), the multi-farm fan-out payload, 404s, duplicate handling
returning 409 rather than a 500, the shared register (no handler passes a user to the service
except create, which stamps "entered by"), and the ppa_status filter rejecting unknown values.

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
from app.core.deps import get_current_internal_user, get_current_superuser, get_db
from app.services import scada_ppa_service

PREFIX = "/scada/ppas"


class _FakeUser:
    id = 42
    role = "admin"
    is_active = True
    is_superuser = True
    is_internal = True
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
        created_by_id=42,
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
    app.dependency_overrides[get_current_internal_user] = lambda: _FakeUser()
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
        # EPR-143: only create carries the caller (as "entered by"); everything else is shared.
        "create_user_id": None,
        "existing_pairs": [],
    }

    async def _list(self, **kw):
        state["list_kwargs"] = kw
        return [_row(1, 7309), _row(2, 7197)], 2

    async def _get(self, ppa_id):
        return _row(ppa_id, 7309) if ppa_id == 1 else None

    async def _by_code(self, ppa_code):
        if ppa_code == "PPA-2026-001":
            return [_row(1, 7309, ppa_code), _row(2, 7197, ppa_code)]
        return []

    async def _missing(self, ids):
        return [i for i in ids if i not in (7309, 7197)]

    async def _existing_pairs(self, ppa_code, ids):
        return list(state["existing_pairs"])

    async def _create(self, payload, *, user_id):
        state["created"] = payload
        state["create_user_id"] = user_id
        return [_row(i + 1, wf, payload.ppa_code) for i, wf in enumerate(payload.windfarm_ids)]

    async def _update(self, ppa_id, patch):
        state["updated"] = (ppa_id, patch)
        return _row(ppa_id, 7309) if ppa_id == 1 else None

    async def _delete(self, ppa_id):
        state["deleted"] = ppa_id
        return _row(ppa_id, 7309) if ppa_id == 1 else None

    async def _delete_by_code(self, ppa_code):
        state["group_deleted"] = ppa_code
        return 2 if ppa_code == "PPA-2026-001" else 0

    monkeypatch.setattr(svc, "list_ppas", _list)
    monkeypatch.setattr(svc, "get_ppa", _get)
    monkeypatch.setattr(svc, "get_by_code", _by_code)
    monkeypatch.setattr(svc, "missing_windfarm_ids", _missing)
    monkeypatch.setattr(svc, "existing_pairs", _existing_pairs)
    monkeypatch.setattr(svc, "create_ppas", _create)
    monkeypatch.setattr(svc, "update_ppa", _update)
    monkeypatch.setattr(svc, "delete_ppa", _delete)
    monkeypatch.setattr(svc, "delete_by_code", _delete_by_code)
    return state


# ─── auth ────────────────────────────────────────────────────────────────────


def test_auth_required():
    """No internal-user override -> every route refuses."""
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


def test_routes_use_the_internal_user_dependency():
    """Guards the decision that this resource is internal-staff-only on the BACKEND (EPR-143:
    superuser AND is_internal), not just behind the portal's frontend gate — a downgrade to
    get_current_superuser or get_current_active_user should fail here."""
    from app.core.deps import get_current_active_user

    deps = {d.call for route in endpoint_module.router.routes for d in route.dependant.dependencies}
    assert get_current_internal_user in deps
    assert get_current_superuser not in deps
    assert get_current_active_user not in deps
    assert len(endpoint_module.router.routes) == 7
    for route in endpoint_module.router.routes:
        assert get_current_internal_user in {d.call for d in route.dependant.dependencies}, route.path


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


def test_list_rejects_an_unknown_status_instead_of_returning_an_empty_page(app_client, service):
    """EPR-143: ``ppa_status`` is the Literal, so a typo is a 422, not a silent empty register."""
    assert app_client.get(f"{PREFIX}?ppa_status=bogus").status_code == 422
    assert app_client.get(f"{PREFIX}?ppa_status=active").status_code == 422
    assert app_client.get(f"{PREFIX}?ppa_status=Active").status_code == 200


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


def test_create_names_the_farms_that_already_carry_the_code(app_client, service):
    """EPR-143: the shared register pre-checks (ppa_code, windfarm_id) and answers 409 with the ids."""
    service["existing_pairs"] = [7197]
    resp = app_client.post(PREFIX, json=_CREATE_BODY)
    assert resp.status_code == 409
    assert "7197" in resp.json()["detail"] and "shared" in resp.json()["detail"]
    assert service["created"] is None


def test_create_rejects_an_out_of_range_indexation_rate(app_client, service):
    for bad in ("100.01", "-100.01", "250"):
        resp = app_client.post(PREFIX, json={**_CREATE_BODY, "indexation_rate_pct": bad})
        assert resp.status_code == 422, bad
    for ok in ("100", "-100", "2.50", "0"):
        assert app_client.post(PREFIX, json={**_CREATE_BODY, "indexation_rate_pct": ok}).status_code == 201
    assert app_client.put(f"{PREFIX}/1", json={"indexation_rate_pct": "101"}).status_code == 422
    assert app_client.put(f"{PREFIX}/1", json={"indexation_rate_pct": "-99.5"}).status_code == 200


def test_create_duplicate_is_409_not_500(app_client, monkeypatch, service):
    """The unique (ppa_code, windfarm_id) constraint must surface as a readable 409 (the pre-check
    can lose a race; the constraint is the authority)."""

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
    app.dependency_overrides[get_current_internal_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        resp = c.post(PREFIX, json=_CREATE_BODY)
    assert resp.status_code == 409
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

    async def _get(self, ppa_id):
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


# ─── shared register behind the internal gate (EPR-143) ──────────────────────


def test_only_create_passes_the_caller_to_the_service(app_client, service):
    """The register is shared: no handler scopes by user. Create stamps the caller as "entered by"."""
    app_client.get(PREFIX)
    app_client.get(f"{PREFIX}/1")
    app_client.get(f"{PREFIX}/by-code/PPA-2026-001")
    app_client.post(PREFIX, json=_CREATE_BODY)
    app_client.put(f"{PREFIX}/1", json={"ppa_buyer": "X"})
    app_client.delete(f"{PREFIX}/1")
    app_client.delete(f"{PREFIX}/by-code/PPA-2026-001")
    assert service["create_user_id"] == 42
    assert "user_id" not in service["list_kwargs"]


def test_another_internal_user_sees_and_edits_everything(service):
    """User 43 is internal too: same rows, same edits — one official register per farm."""

    class _OtherUser(_FakeUser):
        id = 43
        email = "other@energyexe.com"

    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_internal_user] = lambda: _OtherUser()
    with TestClient(app) as c:
        listed = c.get(PREFIX)
        assert listed.status_code == 200
        assert listed.json()["total"] == 2
        assert [i["created_by_id"] for i in listed.json()["items"]] == [42, 42]
        assert c.get(f"{PREFIX}/1").status_code == 200
        assert c.get(f"{PREFIX}/by-code/PPA-2026-001").status_code == 200
        assert c.put(f"{PREFIX}/1", json={"ppa_buyer": "X"}).status_code == 200
        assert c.delete(f"{PREFIX}/1").status_code == 200
        assert c.delete(f"{PREFIX}/by-code/PPA-2026-001").status_code == 200
        created = c.post(PREFIX, json=_CREATE_BODY)
        assert created.status_code == 201
    app.dependency_overrides.clear()
    assert service["create_user_id"] == 43


@pytest.mark.parametrize("is_superuser,is_internal", [(True, False), (False, True), (False, False)])
def test_a_superuser_without_the_internal_flag_gets_403_on_every_route(service, is_superuser, is_internal):
    """The REAL get_current_internal_user runs (only the superuser layer is stubbed): a superuser
    that is not staff, or a staff-flagged non-superuser, is refused everywhere — 403, not 404."""

    class _Outsider(_FakeUser):
        id = 44
        email = "outsider@example.com"

    _Outsider.is_superuser = is_superuser
    _Outsider.is_internal = is_internal

    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)

    async def _db():
        yield None

    app.dependency_overrides[get_db] = _db
    if is_superuser:
        app.dependency_overrides[get_current_superuser] = lambda: _Outsider()
    else:
        from app.core.deps import get_current_user

        app.dependency_overrides[get_current_user] = lambda: _Outsider()
    with TestClient(app) as c:
        for call in (
            lambda: c.get(PREFIX),
            lambda: c.get(f"{PREFIX}/1"),
            lambda: c.get(f"{PREFIX}/by-code/PPA-2026-001"),
            lambda: c.post(PREFIX, json=_CREATE_BODY),
            lambda: c.put(f"{PREFIX}/1", json={"ppa_buyer": "X"}),
            lambda: c.delete(f"{PREFIX}/1"),
            lambda: c.delete(f"{PREFIX}/by-code/PPA-2026-001"),
        ):
            resp = call()
            assert resp.status_code == 403, resp.text
    app.dependency_overrides.clear()
    assert service["created"] is None and service["updated"] is None and service["deleted"] is None


def test_ownership_cannot_be_forged_through_the_payload(app_client, service):
    """``created_by_id`` is not a schema field: a body that names another owner is ignored and the
    row is still created as the caller. Same for PUT."""
    resp = app_client.post(PREFIX, json={**_CREATE_BODY, "created_by_id": 999})
    assert resp.status_code == 201
    assert service["create_user_id"] == 42
    assert not hasattr(service["created"], "created_by_id")
    assert "created_by_id" not in service["created"].model_dump()

    resp = app_client.put(f"{PREFIX}/1", json={"ppa_buyer": "X", "created_by_id": 999})
    assert resp.status_code == 200
    _, patch = service["updated"]
    assert patch.model_dump(exclude_unset=True) == {"ppa_buyer": "X"}
