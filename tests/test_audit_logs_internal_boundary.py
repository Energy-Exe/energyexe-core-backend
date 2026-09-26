"""EPR-143 (review round 2): the internal-only PPA boundary holds on the audit-log READ paths.

``@audit_action`` serializes the SCADA PPA row into ``audit_logs.new_values``; the write side is
gated by ``get_current_internal_user`` but the read routes only required superuser, so a superuser
without ``is_internal`` (403 on ``/scada/ppas``) could read buyer and strike price from
``/audit-logs``. Every read path now hides ``INTERNAL_ONLY_RESOURCE_TYPES`` from such a caller.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoints import audit_logs as endpoint_module
from app.core.database import get_db as database_get_db
from app.core.deps import (
    INTERNAL_ONLY_RESOURCE_TYPES,
    get_current_superuser,
    get_current_user,
    get_db,
)
from app.core.exceptions import add_exception_handlers
from app.models.audit_log import AuditAction
from app.models.user import User
from app.schemas.audit_log import AuditLogFilter
from app.services.audit_log import AuditLogService

OUTSIDER_ID, STAFF_ID = 501, 502
CONTRACT = {"ppa_buyer": "Secret Buyer Ltd", "strike_price": "61.50"}


def _user(user_id: int, *, is_internal: bool) -> User:
    return User(
        id=user_id,
        email=f"u{user_id}@example.com",
        username=f"u{user_id}",
        hashed_password="x",
        is_active=True,
        is_superuser=True,
        is_internal=is_internal,
        role="admin",
    )


async def _seed(session: AsyncSession) -> dict:
    """Two scada_ppa rows (one per user) and one ordinary row per user."""
    rows = {}
    for who, uid in (("outsider", OUTSIDER_ID), ("staff", STAFF_ID)):
        rows[f"{who}_ppa"] = await AuditLogService.log_action(
            session,
            action=AuditAction.CREATE,
            resource_type="scada_ppa",
            user_id=uid,
            user_email=f"u{uid}@example.com",
            resource_id="7",
            resource_name="PPA-7",
            new_values=CONTRACT,
        )
        rows[f"{who}_user"] = await AuditLogService.log_action(
            session,
            action=AuditAction.UPDATE,
            resource_type="user",
            user_id=uid,
            user_email=f"u{uid}@example.com",
            resource_id="3",
            resource_name="someone",
        )
    return rows


def test_the_boundary_names_the_ppa_register():
    assert "scada_ppa" in INTERNAL_ONLY_RESOURCE_TYPES
    assert endpoint_module._hidden_resource_types(_user(1, is_internal=True)) == []
    assert endpoint_module._hidden_resource_types(_user(1, is_internal=False)) == ["scada_ppa"]


# --------------------------------------------------------------------------- service level


@pytest.mark.asyncio
async def test_service_exclusion_applies_to_list_count_summary_and_histories(test_session):
    rows = await _seed(test_session)
    hidden = ["scada_ppa"]

    listed = await AuditLogService.get_audit_logs(
        test_session, filters=AuditLogFilter(exclude_resource_types=hidden)
    )
    assert {r.resource_type for r in listed} == {"user"} and len(listed) == 2
    assert (
        await AuditLogService.count_audit_logs(
            test_session, filters=AuditLogFilter(exclude_resource_types=hidden)
        )
        == 2
    )
    assert await AuditLogService.count_audit_logs(test_session, filters=AuditLogFilter()) == 4

    summary = await AuditLogService.get_audit_summary(
        test_session, filters=AuditLogFilter(exclude_resource_types=hidden)
    )
    assert summary.total_actions == 2
    assert "scada_ppa" not in summary.actions_by_resource
    assert summary.actions_by_type == {AuditAction.UPDATE.value: 2} or summary.actions_by_type == {
        AuditAction.UPDATE: 2
    }
    full = await AuditLogService.get_audit_summary(test_session, filters=AuditLogFilter())
    assert full.total_actions == 4 and full.actions_by_resource.get("scada_ppa") == 2

    history = await AuditLogService.get_resource_audit_history(
        test_session, resource_type="scada_ppa", resource_id="7", exclude_resource_types=hidden
    )
    assert history == []
    mine = await AuditLogService.get_user_audit_history(
        test_session, user_id=OUTSIDER_ID, exclude_resource_types=hidden
    )
    assert [r.id for r in mine] == [rows["outsider_user"].id]
    # no exclusion -> everything, as before
    assert len(await AuditLogService.get_user_audit_history(test_session, user_id=OUTSIDER_ID)) == 2


# --------------------------------------------------------------------------- route level


@pytest.fixture
def app_client(test_session):
    app = FastAPI()
    add_exception_handlers(app)
    app.include_router(endpoint_module.router, prefix="/audit-logs")

    async def _db():
        yield test_session

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[database_get_db] = _db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _login(client: TestClient, user: User) -> None:
    client.app.dependency_overrides[get_current_superuser] = lambda: user
    client.app.dependency_overrides[get_current_user] = lambda: user


def _ids(resp) -> list[int]:
    return [row["id"] for row in resp.json()]


@pytest.mark.asyncio
async def test_non_internal_superuser_never_sees_a_ppa_row(app_client, test_session):
    rows = await _seed(test_session)
    outsider = _user(OUTSIDER_ID, is_internal=False)
    _login(app_client, outsider)

    listed = app_client.get("/audit-logs/")
    assert listed.status_code == 200
    assert set(_ids(listed)) == {rows["outsider_user"].id, rows["staff_user"].id}
    assert "Secret Buyer" not in listed.text
    assert app_client.get("/audit-logs/count").json() == 2
    # search cannot smuggle the row back
    assert _ids(app_client.get("/audit-logs/", params={"search": "PPA-7"})) == []

    summary = app_client.get("/audit-logs/summary").json()
    assert summary["total_actions"] == 2 and "scada_ppa" not in summary["actions_by_resource"]

    # naming the type is refused outright, like the PPA routes
    for resp in (
        app_client.get("/audit-logs/", params={"resource_type": "scada_ppa"}),
        app_client.get("/audit-logs/count", params={"resource_type": "scada_ppa"}),
        app_client.get("/audit-logs/resource/scada_ppa/7"),
    ):
        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["message"] == "Internal EnergyExe access required"

    # a hidden row by id is indistinguishable from a missing one
    assert app_client.get(f"/audit-logs/{rows['staff_ppa'].id}").status_code == 404
    assert app_client.get(f"/audit-logs/{rows['outsider_ppa'].id}").status_code == 404
    assert app_client.get(f"/audit-logs/{rows['staff_user'].id}").status_code == 200

    # histories: the outsider's OWN ppa write is hidden from them too
    assert _ids(app_client.get(f"/audit-logs/user/{OUTSIDER_ID}/history")) == [
        rows["outsider_user"].id
    ]
    assert _ids(app_client.get("/audit-logs/my/history")) == [rows["outsider_user"].id]
    assert _ids(app_client.get(f"/audit-logs/user/{STAFF_ID}/history")) == [rows["staff_user"].id]
    # an ordinary resource history still works
    assert _ids(app_client.get("/audit-logs/resource/user/3")) == [
        rows["staff_user"].id,
        rows["outsider_user"].id,
    ]


@pytest.mark.asyncio
async def test_internal_superuser_still_sees_everything(app_client, test_session):
    rows = await _seed(test_session)
    _login(app_client, _user(STAFF_ID, is_internal=True))

    listed = app_client.get("/audit-logs/")
    assert set(_ids(listed)) == {r.id for r in rows.values()}
    assert "Secret Buyer Ltd" in listed.text
    assert app_client.get("/audit-logs/count").json() == 4
    assert app_client.get("/audit-logs/summary").json()["actions_by_resource"]["scada_ppa"] == 2
    filtered = app_client.get("/audit-logs/", params={"resource_type": "scada_ppa"})
    assert filtered.status_code == 200 and len(filtered.json()) == 2
    assert app_client.get("/audit-logs/count", params={"resource_type": "scada_ppa"}).json() == 2
    assert len(app_client.get("/audit-logs/resource/scada_ppa/7").json()) == 2
    by_id = app_client.get(f"/audit-logs/{rows['outsider_ppa'].id}")
    assert by_id.status_code == 200 and by_id.json()["new_values"] == CONTRACT
    assert len(app_client.get(f"/audit-logs/user/{OUTSIDER_ID}/history").json()) == 2
    assert len(app_client.get("/audit-logs/my/history").json()) == 2
