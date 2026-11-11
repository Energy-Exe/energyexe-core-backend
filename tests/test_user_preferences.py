"""Per-user saved settings (EPR-202) against a real in-memory SQLite.

Pins the allowlist (unknown keys 404), the horizon validator (whole years 2027–2050 only),
upsert, clear, and that one user never sees another's value.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.v1.endpoints import users as users_endpoint
from app.core.database import Base
from app.core.database import get_db as database_get_db
from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.user_preference import UserPreference
from app.services.user_preference_service import (
    InvalidPreference,
    UnknownPreference,
    UserPreferenceService,
)

KEY = "scada_degradation_horizon"


@pytest.fixture
async def factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all, tables=[User.__table__, UserPreference.__table__]
        )
    make = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with make() as session:
        for uid in (1, 2):
            session.add(
                User(id=uid, email=f"u{uid}@x.test", username=f"u{uid}", hashed_password="x")
            )
        await session.commit()
    yield make
    await engine.dispose()


async def test_set_get_overwrite_and_clear(factory):
    async with factory() as db:
        svc = UserPreferenceService(db)
        assert await svc.get(1, KEY) is None
        assert await svc.set(1, KEY, 2030) == 2030
        assert await svc.get(1, KEY) == 2030
        await svc.set(1, KEY, 2045)
        assert await svc.get(1, KEY) == 2045
        await svc.clear(1, KEY)
        assert await svc.get(1, KEY) is None
        await svc.clear(1, KEY)  # clearing nothing is fine


async def test_one_user_never_sees_another_users_value(factory):
    async with factory() as db:
        svc = UserPreferenceService(db)
        await svc.set(1, KEY, 2040)
        assert await svc.get(2, KEY) is None
        await svc.clear(2, KEY)
        assert await svc.get(1, KEY) == 2040


@pytest.mark.parametrize("bad", [2026, 2051, "2030", 2030.5, True, None, [2030]])
async def test_horizon_must_be_a_whole_year_in_range(factory, bad):
    async with factory() as db:
        with pytest.raises(InvalidPreference):
            await UserPreferenceService(db).set(1, KEY, bad)


async def test_unknown_keys_are_refused(factory):
    async with factory() as db:
        svc = UserPreferenceService(db)
        for call in (svc.get(1, "theme"), svc.set(1, "theme", 1), svc.clear(1, "theme")):
            with pytest.raises(UnknownPreference):
                await call


class _User:
    def __init__(self, uid):
        self.id = uid
        self.is_active = True
        self.is_superuser = False
        self.email = f"u{uid}@x.test"


@pytest.fixture
def client(factory):
    app = FastAPI()
    app.include_router(users_endpoint.router, prefix="/users")
    state = {"user": _User(1)}

    async def _db():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[database_get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: state["user"]
    with TestClient(app) as c:
        c.state = state
        yield c


def test_preference_endpoints_round_trip(client):
    url = f"/users/me/preferences/{KEY}"
    assert client.get(url).json() == {"key": KEY, "value": None}
    assert client.put(url, json={"value": 2030}).json() == {"key": KEY, "value": 2030}
    assert client.get(url).json()["value"] == 2030
    assert client.put(url, json={"value": 2051}).status_code == 422
    assert client.put(url, json={"value": "2030"}).status_code == 422
    assert client.put(url, json={}).status_code == 422
    assert client.get(url).json()["value"] == 2030  # a refused write changes nothing
    client.state["user"] = _User(2)
    assert client.get(url).json()["value"] is None
    client.state["user"] = _User(1)
    assert client.delete(url).json() == {"key": KEY, "value": None}
    assert client.get(url).json()["value"] is None


def test_unknown_or_malformed_keys(client):
    assert client.get("/users/me/preferences/theme").status_code == 404
    assert client.put("/users/me/preferences/theme", json={"value": 1}).status_code == 404
    assert client.delete("/users/me/preferences/theme").status_code == 404
    assert client.get("/users/me/preferences/Bad-Key").status_code == 422


def test_preferences_need_a_signed_in_user():
    app = FastAPI()
    app.include_router(users_endpoint.router, prefix="/users")
    with TestClient(app) as c:
        assert c.get(f"/users/me/preferences/{KEY}").status_code in (401, 403)
