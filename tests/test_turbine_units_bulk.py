"""
Tests for TurbineUnitService.bulk_create_turbine_units (EPR-107).

The bulk create is all-or-nothing: every row is validated (in-payload duplicate
codes, already-existing codes, FK existence) before a single commit; any error
raises TurbineUnitBulkValidationError carrying per-row errors and writes nothing.
Returned units must have windfarm/turbine_model eager-loaded (MissingGreenlet
guard — see test_turbine_units_service.py for background).
"""

import pydantic
import pytest
import pytest_asyncio
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.future import select
from sqlalchemy.pool import StaticPool

import app.models  # noqa: F401  — register all ORM mappers so relationships configure
from app.models.turbine_model import TurbineModel
from app.models.turbine_unit import TurbineUnit as TurbineUnitModel
from app.models.windfarm import Windfarm
from app.schemas.turbine_unit import TurbineUnit as TurbineUnitSchema
from app.schemas.turbine_unit import TurbineUnitBulkCreate, TurbineUnitCreate
from app.services.turbine_unit import TurbineUnitBulkValidationError, TurbineUnitService


@pytest_asyncio.fixture
async def test_session():
    """In-memory async SQLite session with only the turbine tables created."""
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
    windfarm = Windfarm(
        code="WF_BULK",
        name="Bulk Test Windfarm",
        country_id=1,  # SQLite test engine does not enforce FKs
        status="operational",
        is_deleted=False,
        permits_obtained=False,
    )
    model = TurbineModel(model="MOD_BULK", supplier="Acme", original_supplier="Acme")
    session.add_all([windfarm, model])
    await session.commit()
    await session.refresh(windfarm)
    await session.refresh(model)
    return windfarm.id, model.id


def _row(code: str, windfarm_id: int, model_id: int, **overrides) -> TurbineUnitCreate:
    data = dict(
        code=code,
        windfarm_id=windfarm_id,
        turbine_model_id=model_id,
        lat=55.0,
        lng=8.0,
        status="operational",
    )
    data.update(overrides)
    return TurbineUnitCreate(**data)


async def _count_units(session) -> int:
    return (await session.execute(select(func.count(TurbineUnitModel.id)))).scalar()


@pytest.mark.asyncio
async def test_bulk_create_happy_path(test_session):
    windfarm_id, model_id = await _seed_windfarm_and_model(test_session)
    rows = [_row(f"WF_BULK-{i:03d}", windfarm_id, model_id) for i in range(1, 4)]

    created = await TurbineUnitService.bulk_create_turbine_units(test_session, rows)

    assert len(created) == 3
    assert [u.code for u in created] == ["WF_BULK-001", "WF_BULK-002", "WF_BULK-003"]
    assert await _count_units(test_session) == 3
    for unit in created:
        dumped = TurbineUnitSchema.model_validate(unit).model_dump()
        assert dumped["windfarm"]["code"] == "WF_BULK"
        assert dumped["turbine_model"]["model"] == "MOD_BULK"


@pytest.mark.asyncio
async def test_bulk_create_in_payload_duplicate(test_session):
    windfarm_id, model_id = await _seed_windfarm_and_model(test_session)
    rows = [
        _row("WF_BULK-001", windfarm_id, model_id),
        _row("WF_BULK-001", windfarm_id, model_id),
    ]

    with pytest.raises(TurbineUnitBulkValidationError) as exc_info:
        await TurbineUnitService.bulk_create_turbine_units(test_session, rows)

    errors = exc_info.value.errors
    assert len(errors) == 1
    assert errors[0].index == 1
    assert errors[0].field == "code"
    assert await _count_units(test_session) == 0


@pytest.mark.asyncio
async def test_bulk_create_existing_code_is_all_or_nothing(test_session):
    windfarm_id, model_id = await _seed_windfarm_and_model(test_session)
    await TurbineUnitService.bulk_create_turbine_units(
        test_session, [_row("WF_BULK-001", windfarm_id, model_id)]
    )

    rows = [
        _row("WF_BULK-002", windfarm_id, model_id),  # valid
        _row("WF_BULK-001", windfarm_id, model_id),  # collides with existing
    ]
    with pytest.raises(TurbineUnitBulkValidationError) as exc_info:
        await TurbineUnitService.bulk_create_turbine_units(test_session, rows)

    errors = exc_info.value.errors
    assert len(errors) == 1
    assert errors[0].index == 1
    assert errors[0].message == "TurbineUnit with this code already exists"
    # the valid row must NOT have been created
    assert await _count_units(test_session) == 1


@pytest.mark.asyncio
async def test_bulk_create_missing_fk(test_session):
    windfarm_id, model_id = await _seed_windfarm_and_model(test_session)
    rows = [
        _row("WF_BULK-001", windfarm_id, model_id),
        _row("WF_BULK-002", windfarm_id, model_id + 999),
    ]

    with pytest.raises(TurbineUnitBulkValidationError) as exc_info:
        await TurbineUnitService.bulk_create_turbine_units(test_session, rows)

    errors = exc_info.value.errors
    assert len(errors) == 1
    assert errors[0].index == 1
    assert errors[0].field == "turbine_model_id"
    assert await _count_units(test_session) == 0


def test_bulk_create_schema_rejects_over_cap():
    row = dict(code="X-001", windfarm_id=1, turbine_model_id=1, lat=55.0, lng=8.0)
    rows = [dict(row, code=f"X-{i:03d}") for i in range(501)]
    with pytest.raises(pydantic.ValidationError):
        TurbineUnitBulkCreate(turbines=rows)
    # at the cap it validates fine
    assert len(TurbineUnitBulkCreate(turbines=rows[:500]).turbines) == 500
