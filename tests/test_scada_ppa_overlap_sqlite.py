"""Overlap refusal semantics (EPR-143, Aje 2026-09-28, D-041) against a real in-memory SQLite.

Two Active rows on one farm whose closed date ranges share a calendar day are "conflicting terms"
and the service refuses the write. SQLite renders no ``FOR UPDATE`` so the RACE is not provable
here (see test_scada_ppa_overlap_postgres for that); what this file pins is the rule itself:
adjacent is allowed, overlap is refused, NULL bounds are unbounded, non-Active rows never take
part, a row is never in conflict with itself, activation-by-update and farm moves are judged on
the MERGED row, and a multi-farm create is all-or-nothing.

Only the three tables the service touches are created; ``windfarms`` keeps its FK columns but
SQLite does not enforce them without the pragma, so no parent tables are needed.
"""

from datetime import date

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.database import Base
from app.models.scada_ppa import ScadaPpa
from app.models.user import User
from app.models.windfarm import Windfarm
from app.schemas.scada_ppa import ScadaPpaCreate, ScadaPpaUpdate
from app.services.scada_ppa_service import (
    ActiveTermsConflict,
    ScadaPpaService,
    ScadaPpaTermsError,
)

A, B = 7309, 7197
USER = 42


@pytest.fixture
async def db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all,
            tables=[User.__table__, Windfarm.__table__, ScadaPpa.__table__],
        )
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add_all(
            [
                Windfarm(id=A, code="HOT", name="Hill of Towie", country_id=1),
                Windfarm(id=B, code="LUT", name="Lutelandet", country_id=1),
            ]
        )
        await session.commit()
        yield session
    await engine.dispose()


def _create(code, farms, status="Active", effective=None, expiration=None):
    return ScadaPpaCreate(
        ppa_code=code,
        ppa_buyer="Buyer",
        windfarm_ids=list(farms),
        ppa_status=status,
        effective_date=effective,
        expiration_date=expiration,
    )


async def _seed(db, code, farm, status="Active", effective=None, expiration=None):
    rows = await ScadaPpaService(db).create_ppas(
        _create(code, [farm], status, effective, expiration), user_id=USER
    )
    return rows[0].id


async def _codes_on(db, farm):
    stmt = select(ScadaPpa.ppa_code).where(ScadaPpa.windfarm_id == farm).order_by(ScadaPpa.id)
    return list((await db.execute(stmt)).scalars().all())


# ─── create ──────────────────────────────────────────────────────────────────


async def test_adjacent_active_contracts_are_allowed(db):
    """Aje: compared on dates — 30 June end vs 1 July start is NOT an overlap."""
    await _seed(db, "OLD", A, effective=date(2024, 1, 1), expiration=date(2026, 6, 30))
    await _seed(db, "NEW", A, effective=date(2026, 7, 1), expiration=date(2030, 12, 31))
    assert await _codes_on(db, A) == ["OLD", "NEW"]


async def test_sharing_one_day_is_an_overlap(db):
    """The day a contract ends is still covered by it: 30 June / 30 June conflicts."""
    await _seed(db, "OLD", A, effective=date(2024, 1, 1), expiration=date(2026, 6, 30))
    with pytest.raises(ActiveTermsConflict) as exc:
        await _seed(db, "NEW", A, effective=date(2026, 6, 30), expiration=date(2030, 12, 31))
    assert str(exc.value) == (
        "conflicting terms: an Active PPA (OLD, 2024-01-01 to 2026-06-30) already covers "
        f"windfarm {A}"
    )
    assert exc.value.windfarm_id == A and [c.ppa_code for c in exc.value.conflicts] == ["OLD"]


async def test_overlapping_active_contract_is_refused(db):
    await _seed(db, "OLD", A, effective=date(2024, 1, 1), expiration=date(2027, 12, 31))
    with pytest.raises(ActiveTermsConflict):
        await _seed(db, "NEW", A, effective=date(2026, 1, 1), expiration=date(2028, 12, 31))
    assert await _codes_on(db, A) == ["OLD"]


async def test_null_bounds_are_unbounded(db):
    """An open-ended Active contract overlaps everything after its start; an undated one
    overlaps everything."""
    await _seed(db, "OPEN", A, effective=date(2025, 1, 1), expiration=None)
    with pytest.raises(ActiveTermsConflict):
        await _seed(db, "LATER", A, effective=date(2031, 1, 1), expiration=date(2032, 1, 1))
    # ...but not what ended before it started
    await _seed(db, "EARLIER", A, effective=date(2020, 1, 1), expiration=date(2024, 12, 31))
    await _seed(db, "UNDATED", B, effective=None, expiration=None)
    with pytest.raises(ActiveTermsConflict):
        await _seed(db, "ANY", B, effective=date(1999, 1, 1), expiration=date(1999, 12, 31))


async def test_non_active_rows_never_take_part(db):
    """Draft / Superseded / Terminated / Expired rows are not in force, on either side."""
    for status in ("Draft", "Superseded", "Terminated", "Expired"):
        await _seed(db, f"OLD-{status}", A, status=status, effective=date(2024, 1, 1))
    await _seed(db, "NEW", A, effective=date(2024, 1, 1))  # fine: nothing Active yet
    await _seed(db, "DRAFT2", A, status="Draft", effective=date(2024, 1, 1))  # a draft over Active
    assert len(await _codes_on(db, A)) == 6


async def test_multi_farm_create_is_all_or_nothing(db):
    """One conflicting farm and NO leg is written — the register never holds half a contract."""
    await _seed(db, "OLD", B, effective=date(2024, 1, 1))
    with pytest.raises(ActiveTermsConflict) as exc:
        await ScadaPpaService(db).create_ppas(
            _create("NEW", [A, B], effective=date(2025, 1, 1)), user_id=USER
        )
    assert exc.value.windfarm_id == B
    await db.rollback()
    assert await _codes_on(db, A) == [] and await _codes_on(db, B) == ["OLD"]


async def test_create_refuses_an_unknown_farm_at_the_lock(db):
    with pytest.raises(ScadaPpaTermsError, match=r"Unknown windfarm_id\(s\): \[999\]"):
        await ScadaPpaService(db).create_ppas(_create("X", [A, 999]), user_id=USER)


# ─── update ──────────────────────────────────────────────────────────────────


async def test_a_row_is_never_in_conflict_with_itself(db):
    """Editing an Active row's own dates must not see the row it is editing."""
    ppa_id = await _seed(db, "ONLY", A, effective=date(2024, 1, 1), expiration=date(2026, 12, 31))
    row = await ScadaPpaService(db).update_ppa(
        ppa_id, ScadaPpaUpdate(expiration_date=date(2027, 12, 31))
    )
    assert row.expiration_date == date(2027, 12, 31)


async def test_activation_by_update_is_judged_on_the_merged_row(db):
    await _seed(db, "OLD", A, effective=date(2024, 1, 1), expiration=None)
    draft = await _seed(db, "NEW", A, status="Draft", effective=date(2026, 1, 1))
    with pytest.raises(ActiveTermsConflict):
        await ScadaPpaService(db).update_ppa(draft, ScadaPpaUpdate(ppa_status="Active"))
    await db.rollback()
    row = await ScadaPpaService(db).get_ppa(draft)
    assert row.ppa_status == "Draft"


async def test_farm_move_onto_an_occupied_farm_is_refused(db):
    await _seed(db, "OLD", B, effective=date(2024, 1, 1), expiration=None)
    mover = await _seed(db, "MOVER", A, effective=date(2025, 1, 1), expiration=None)
    with pytest.raises(ActiveTermsConflict) as exc:
        await ScadaPpaService(db).update_ppa(mover, ScadaPpaUpdate(windfarm_id=B))
    assert exc.value.windfarm_id == B
    await db.rollback()
    # ...and onto a free farm it goes, with its own dates re-checked there
    await _seed(db, "EARLY", B, effective=date(2020, 1, 1), expiration=date(2021, 1, 1))
    await ScadaPpaService(db).delete_by_code("OLD")
    row = await ScadaPpaService(db).update_ppa(mover, ScadaPpaUpdate(windfarm_id=B))
    assert row.windfarm_id == B


async def test_widening_dates_into_a_neighbour_is_refused(db):
    await _seed(db, "OLD", A, effective=date(2024, 1, 1), expiration=date(2025, 12, 31))
    later = await _seed(db, "LATER", A, effective=date(2026, 1, 1), expiration=date(2027, 12, 31))
    with pytest.raises(ActiveTermsConflict):
        await ScadaPpaService(db).update_ppa(
            later, ScadaPpaUpdate(effective_date=date(2025, 12, 31))
        )


async def test_update_revalidates_the_merged_row_before_the_overlap_check(db):
    """A half-patch that inverts the stored pair fails on the merged row (the same check the
    endpoint does early, repeated here on the locked row)."""
    ppa_id = await _seed(db, "P", A, effective=date(2026, 1, 1), expiration=date(2026, 12, 31))
    with pytest.raises(ScadaPpaTermsError, match="expiration_date must be after"):
        await ScadaPpaService(db).update_ppa(
            ppa_id, ScadaPpaUpdate(expiration_date=date(2025, 6, 1))
        )
    # the schema catches a pair supplied together; a HALF patch only the merged row can catch
    await ScadaPpaService(db).update_ppa(ppa_id, ScadaPpaUpdate(cap_price=40))
    with pytest.raises(ScadaPpaTermsError, match="floor_price"):
        await ScadaPpaService(db).update_ppa(ppa_id, ScadaPpaUpdate(floor_price=50))


async def test_update_to_an_unknown_farm_is_refused_at_the_lock(db):
    ppa_id = await _seed(db, "P", A)
    with pytest.raises(ScadaPpaTermsError, match=r"Unknown windfarm_id\(s\): \[999\]"):
        await ScadaPpaService(db).update_ppa(ppa_id, ScadaPpaUpdate(windfarm_id=999))


async def test_update_of_a_missing_row_is_none(db):
    assert await ScadaPpaService(db).update_ppa(12345, ScadaPpaUpdate(ppa_buyer="X")) is None
