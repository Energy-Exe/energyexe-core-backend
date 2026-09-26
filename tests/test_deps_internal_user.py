"""EPR-143: ``users.is_internal`` is a boundary, not a decoration.

* ``get_current_internal_user`` = superuser AND is_internal, else 403.
* ``PUT /users/{id}`` and ``DELETE /users/{id}`` refuse (403) when the TARGET is internal and the
  caller is not — otherwise any superuser could reset an internal user's password and log in as
  them, which would empty the flag of meaning.
* ``is_internal`` is not on ``UserUpdate`` (same posture as ``is_superuser``) and is exposed
  read-only on ``UserResponse`` (defaulted, so the hand-built registration / invitation dicts stay
  valid).
"""

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_internal_user, get_current_superuser
from app.core.security import get_password_hash
from app.models.user import User
from app.schemas.user import UserResponse, UserResponseExtended, UserUpdate


def _user(**over) -> User:
    base = dict(
        email="x@energyexe.com", username="x", hashed_password=get_password_hash("Password123!"),
        is_active=True, is_superuser=True, is_internal=True, role="admin", is_approved=True,
        email_verified=True,
    )
    base.update(over)
    return User(**base)


@pytest.mark.asyncio
async def test_internal_dependency_requires_the_flag_on_top_of_superuser():
    staff = _user()
    assert await get_current_internal_user(staff) is staff
    with pytest.raises(HTTPException) as exc:
        await get_current_internal_user(_user(is_internal=False))
    assert exc.value.status_code == 403 and "Internal" in exc.value.detail
    # the superuser layer still runs first: a non-superuser never reaches the flag check
    with pytest.raises(HTTPException) as exc:
        await get_current_superuser(_user(is_superuser=False, is_internal=True))
    assert exc.value.status_code == 403


def test_is_internal_is_read_only_on_the_schemas():
    assert "is_internal" not in UserUpdate.model_fields
    assert "is_superuser" not in UserUpdate.model_fields
    field = UserResponse.model_fields["is_internal"]
    assert field.default is False and not field.is_required()
    assert "is_internal" in UserResponseExtended.model_fields
    # the hand-built dicts in endpoints/auth.py carry the key (registration + invitation)
    import inspect

    from app.api.v1.endpoints import auth

    assert inspect.getsource(auth).count('"is_internal": user.is_internal,') == 2


def test_the_column_lands_false_with_a_server_default():
    col = User.__table__.c.is_internal
    assert not col.nullable
    assert col.server_default is not None and "false" in str(col.server_default.arg)


async def _seed(session: AsyncSession, *users: User) -> None:
    session.add_all(users)
    await session.commit()
    for u in users:
        await session.refresh(u)


def _stub_service_delete(monkeypatch) -> list[int]:
    """Record ``UserService.delete`` calls instead of running them: a real delete cascades over
    portfolios / alert rules / notifications, tables the SQLite test DB deliberately does not create.
    The guard under test runs in the route BEFORE the service is reached."""
    from app.services import user as user_service_module

    calls: list[int] = []

    async def _delete(self, user_id: int) -> bool:
        calls.append(user_id)
        return True

    monkeypatch.setattr(user_service_module.UserService, "delete", _delete)
    return calls


@pytest.mark.asyncio
async def test_non_internal_superuser_cannot_edit_or_delete_an_internal_account(client, test_session, monkeypatch):
    deleted = _stub_service_delete(monkeypatch)
    staff = _user(email="staff@energyexe.com", username="staff")
    outsider = _user(email="outsider@example.com", username="outsider", is_internal=False)
    await _seed(test_session, staff, outsider)
    client.app.dependency_overrides[get_current_superuser] = lambda: outsider

    resp = client.put(f"/api/v1/users/{staff.id}", json={"password": "Hijacked123!"})
    assert resp.status_code == 403, resp.text
    assert "internal" in resp.json()["error"]["message"].lower()  # house envelope: {"error": {"message"}}
    assert client.delete(f"/api/v1/users/{staff.id}").status_code == 403
    # the outsider can still manage a non-internal account
    other = _user(email="client@example.com", username="client", is_superuser=False, is_internal=False, role="client")
    await _seed(test_session, other)
    assert client.put(f"/api/v1/users/{other.id}", json={"first_name": "Renamed"}).status_code == 200
    assert client.delete(f"/api/v1/users/{other.id}").status_code == 200
    assert deleted == [other.id]  # the internal target never reached the service
    assert client.put("/api/v1/users/999999", json={"first_name": "x"}).status_code == 404


@pytest.mark.asyncio
async def test_internal_superuser_can_manage_internal_and_external_accounts(client, test_session, monkeypatch):
    deleted = _stub_service_delete(monkeypatch)
    staff = _user(email="staff@energyexe.com", username="staff")
    colleague = _user(email="colleague@energyexe.com", username="colleague")
    await _seed(test_session, staff, colleague)
    client.app.dependency_overrides[get_current_superuser] = lambda: staff

    resp = client.put(f"/api/v1/users/{colleague.id}", json={"first_name": "Ada"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["first_name"] == "Ada" and resp.json()["is_internal"] is True
    assert client.delete(f"/api/v1/users/{colleague.id}").status_code == 200
    assert deleted == [colleague.id]


@pytest.mark.asyncio
async def test_is_internal_cannot_be_set_through_the_update_payload(client, test_session):
    staff = _user(email="staff@energyexe.com", username="staff")
    outsider = _user(email="outsider@example.com", username="outsider", is_internal=False)
    await _seed(test_session, staff, outsider)
    client.app.dependency_overrides[get_current_superuser] = lambda: staff
    resp = client.put(f"/api/v1/users/{outsider.id}", json={"is_internal": True, "is_superuser": True})
    assert resp.status_code == 200
    assert resp.json()["is_internal"] is False
    await test_session.refresh(outsider)
    assert outsider.is_internal is False
    # and /users/me cannot grant it either
    from app.core.deps import get_current_active_user

    client.app.dependency_overrides[get_current_active_user] = lambda: outsider
    me = client.put("/api/v1/users/me", json={"is_internal": True})
    assert me.status_code == 200 and me.json()["is_internal"] is False
