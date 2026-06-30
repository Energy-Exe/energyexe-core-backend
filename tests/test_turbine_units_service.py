"""
Regression tests for TurbineUnitService relationship eager-loading.

Background: the TurbineUnit response_model embeds the `windfarm` and
`turbine_model` relationships. When a service method returns a TurbineUnit whose
relationships are NOT eager-loaded, serializing it (FastAPI response_model /
``TurbineUnit.model_validate``) accesses those attributes on an async session,
triggering a lazy load outside the async greenlet → ``MissingGreenlet`` →
ResponseValidationError (HTTP 500). This was the most frequent unhandled
backend exception in production (the client's PUT /turbine-units/{id} save loop).

Each test exercises a write/read path and then serializes the returned object
through the schema; before the fix these raised MissingGreenlet.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import app.models  # noqa: F401  — register all ORM mappers so relationships configure
from app.models.turbine_model import TurbineModel
from app.models.turbine_unit import TurbineUnit as TurbineUnitModel
from app.models.windfarm import Windfarm
from app.schemas.turbine_unit import (
    TurbineUnit as TurbineUnitSchema,
    TurbineUnitCreate,
    TurbineUnitUpdate,
)
from app.services.turbine_unit import TurbineUnitService


@pytest_asyncio.fixture
async def test_session():
    """In-memory async SQLite session with only the turbine tables created.

    conftest's shared engine deliberately creates auth tables only, so this
    fixture builds its own engine. expire_on_commit=False mirrors the production
    session factory (app/core/database.py) — important because the bug under test
    depends on attribute access after commit."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    tables = [Windfarm.__table__, TurbineModel.__table__, TurbineUnitModel.__table__]
    async with engine.begin() as conn:
        for table in tables:
            await conn.run_sync(table.create, checkfirst=True)

    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session

    await engine.dispose()


async def _seed_windfarm_and_model(session) -> tuple[int, int]:
    """Create the FK prerequisites and return (windfarm_id, turbine_model_id)."""
    windfarm = Windfarm(
        code="WF_TEST_TU",
        name="Test Windfarm",
        country_id=1,  # SQLite test engine does not enforce FKs
        status="operational",
        is_deleted=False,
        permits_obtained=False,
    )
    model = TurbineModel(model="MOD_TEST_TU", supplier="Acme", original_supplier="Acme")
    session.add_all([windfarm, model])
    await session.commit()
    await session.refresh(windfarm)
    await session.refresh(model)
    return windfarm.id, model.id


async def _seed_turbine_unit(session, code: str = "WF_TEST_TU_001") -> TurbineUnitModel:
    windfarm_id, model_id = await _seed_windfarm_and_model(session)
    unit = TurbineUnitModel(
        code=code,
        windfarm_id=windfarm_id,
        turbine_model_id=model_id,
        lat=55.0,
        lng=8.0,
        status="operational",
    )
    session.add(unit)
    await session.commit()
    await session.refresh(unit)
    return unit


def _assert_relations_serialize(obj: TurbineUnitModel) -> None:
    """Serialize through the response schema — raises MissingGreenlet if a
    relationship is not eager-loaded."""
    dumped = TurbineUnitSchema.model_validate(obj).model_dump()
    assert dumped["windfarm"] is not None
    assert dumped["windfarm"]["code"] == "WF_TEST_TU"
    assert dumped["turbine_model"] is not None
    assert dumped["turbine_model"]["model"] == "MOD_TEST_TU"


@pytest.mark.asyncio
async def test_update_turbine_unit_serializes_relations(test_session):
    """PUT /turbine-units/{id} — the production failure path."""
    unit = await _seed_turbine_unit(test_session)

    updated = await TurbineUnitService.update_turbine_unit(
        test_session, unit.id, TurbineUnitUpdate(status="installing")
    )

    assert updated is not None
    assert updated.status == "installing"
    _assert_relations_serialize(updated)


@pytest.mark.asyncio
async def test_create_turbine_unit_serializes_relations(test_session):
    """POST /turbine-units/ returns the created unit through the response_model."""
    windfarm_id, model_id = await _seed_windfarm_and_model(test_session)

    created = await TurbineUnitService.create_turbine_unit(
        test_session,
        TurbineUnitCreate(
            code="WF_TEST_TU_NEW",
            windfarm_id=windfarm_id,
            turbine_model_id=model_id,
            lat=55.0,
            lng=8.0,
            status="operational",
        ),
    )

    assert created is not None
    _assert_relations_serialize(created)


@pytest.mark.asyncio
async def test_get_turbine_unit_by_code_serializes_relations(test_session):
    """GET /turbine-units/code/{code}."""
    unit = await _seed_turbine_unit(test_session)

    fetched = await TurbineUnitService.get_turbine_unit_by_code(test_session, unit.code)

    assert fetched is not None
    _assert_relations_serialize(fetched)


# NOTE: delete_turbine_unit also got the eager-load fix, but it can't be unit-tested
# on SQLite: deleting a TurbineUnit cascades through its `generation_data`
# relationship, whose table uses ARRAY(BigInteger) (Postgres-only) and therefore
# can't be created on the SQLite test engine. The fix there mirrors the others
# (selectinload windfarm + turbine_model before delete; relations survive the commit
# because expire_on_commit=False) and is verified on staging.
