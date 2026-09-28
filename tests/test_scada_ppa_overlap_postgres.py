"""The overlap-refusal RACE (EPR-143, Aje 2026-09-28, D-041) against an ISOLATED PostgreSQL.

SQLite cannot prove this (no ``FOR UPDATE``); here two real sessions on two connections write
concurrently and the service's lock order — PPA row, then farms in id order, then the overlap
query — must leave exactly one winner and refuse the other, whatever the interleaving.

Skipped unless ``EPR143_PG_ADMIN_URL`` names a local admin URL, e.g. after::

    docker run -d --name epr143-pg -p 55432:5432 -e POSTGRES_PASSWORD=epr143 postgres:16-alpine
    EPR143_PG_ADMIN_URL=postgresql://postgres:epr143@127.0.0.1:55432/postgres \\
        poetry run pytest tests/test_scada_ppa_overlap_postgres.py

Each test gets a throw-away database holding ``windfarms`` and ``scada_ppa`` in their ORM shape
(every column, so ``selectinload`` works) but WITHOUT foreign keys to the rest of the schema, so
no parent tables are needed. ``users`` is not created: ``created_by_id`` is a plain integer here.
"""

import asyncio
import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateTable

from app.models.scada_ppa import ScadaPpa
from app.models.windfarm import Windfarm
from app.schemas.scada_ppa import ScadaPpaCreate, ScadaPpaUpdate
from app.services.scada_ppa_service import (
    ActiveTermsConflict,
    ScadaPpaService,
    ScadaPpaTermsError,
)

ADMIN_URL = os.environ.get("EPR143_PG_ADMIN_URL")
pytestmark = pytest.mark.skipif(
    not ADMIN_URL, reason="EPR143_PG_ADMIN_URL not set (isolated Postgres only)"
)

A, B = 7309, 7197
USER = 42


@pytest.fixture
async def engine():
    url = make_url(ADMIN_URL)
    assert url.host in ("127.0.0.1", "localhost") and url.database == "postgres"
    admin = create_engine(url.set(drivername="postgresql+psycopg2"), isolation_level="AUTOCOMMIT")
    name = "epr143_ovl_" + uuid.uuid4().hex[:12]
    with admin.connect() as c:
        c.execute(text(f'CREATE DATABASE "{name}"'))
    async_url = url.set(drivername="postgresql+asyncpg", database=name).render_as_string(
        hide_password=False
    )
    eng = create_async_engine(async_url, pool_size=5)
    try:
        async with eng.begin() as conn:
            for table in (Windfarm.__table__, ScadaPpa.__table__):
                await conn.execute(CreateTable(table, include_foreign_key_constraints=[]))
            await conn.execute(
                text(
                    "INSERT INTO windfarms (id, code, name, country_id, permits_obtained) VALUES "
                    f"({A}, 'HOT', 'Hill of Towie', 1, false), ({B}, 'LUT', 'Lutelandet', 1, false)"
                )
            )
        yield eng
    finally:
        await eng.dispose()
        with admin.connect() as c:
            c.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture
def sessions(engine):
    return async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


def _create(code, farms, status="Active", effective=None, expiration=None):
    return ScadaPpaCreate(
        ppa_code=code,
        ppa_buyer="Buyer",
        windfarm_ids=list(farms),
        ppa_status=status,
        effective_date=effective,
        expiration_date=expiration,
    )


async def _seed(sessions, code, farm, status="Active", effective=None, expiration=None):
    async with sessions() as s:
        rows = await ScadaPpaService(s).create_ppas(
            _create(code, [farm], status, effective, expiration), user_id=USER
        )
        return rows[0].id


async def _rows(sessions):
    async with sessions() as s:
        res = await s.execute(
            text(
                "SELECT ppa_code, windfarm_id, ppa_status, effective_date, expiration_date "
                "FROM scada_ppa ORDER BY id"
            )
        )
        return [tuple(r) for r in res.all()]


async def _attempt(sessions, work, barrier):
    """One writer: do the stale read the endpoint would do, wait for the other writer to have done
    the same, then write. Returns the exception instead of raising so gather() sees both."""
    async with sessions() as s:
        svc = ScadaPpaService(s)
        try:
            await work(svc, barrier)
            return None
        except (ActiveTermsConflict, ScadaPpaTermsError) as exc:
            await s.rollback()
            return exc


async def test_two_concurrent_creates_on_one_farm_leave_exactly_one_active_row(sessions):
    """Different codes (so the unique key does not fire), same farm, overlapping Active dates:
    the farm lock serialises them, the second sees the first's row and is refused."""
    barrier = asyncio.Barrier(2)

    def make(code):
        async def work(svc, barrier):
            await barrier.wait()  # both are "in flight" before either writes
            await svc.create_ppas(
                _create(code, [A], effective=date(2026, 1, 1), expiration=date(2030, 12, 31)),
                user_id=USER,
            )

        return work

    outcomes = await asyncio.gather(
        _attempt(sessions, make("FIRST"), barrier), _attempt(sessions, make("SECOND"), barrier)
    )
    conflicts = [o for o in outcomes if isinstance(o, ActiveTermsConflict)]
    assert len(conflicts) == 1 and outcomes.count(None) == 1, outcomes
    rows = await _rows(sessions)
    assert len(rows) == 1 and rows[0][2] == "Active"
    # the loser's message names the winner (the rollback expired the ORM rows, so read the
    # message the exception formatted while the session was live, not its .conflicts)
    assert f"an Active PPA ({rows[0][0]}, 2026-01-01 to 2030-12-31)" in str(conflicts[0])


async def test_activation_versus_move_never_ends_active_on_the_occupied_farm(sessions):
    """Codex round 4: a Draft on farm A is read by two requests; one moves it to farm B (which
    holds an Active contract), the other activates it. Whichever commits second must see the
    REFRESHED row (farm B, or Active) and be refused; the row never ends Active on B."""
    await _seed(sessions, "OLD", B, effective=date(2024, 1, 1), expiration=None)
    draft = await _seed(sessions, "D", A, status="Draft", effective=date(2026, 1, 1))
    barrier = asyncio.Barrier(2)

    def make(patch):
        async def work(svc, barrier):
            stale = await svc.get_ppa(draft)  # the endpoint's 404-check read
            assert stale.windfarm_id == A and stale.ppa_status == "Draft"
            await barrier.wait()
            await svc.update_ppa(draft, patch)

        return work

    outcomes = await asyncio.gather(
        _attempt(sessions, make(ScadaPpaUpdate(windfarm_id=B)), barrier),
        _attempt(sessions, make(ScadaPpaUpdate(ppa_status="Active")), barrier),
    )
    assert sum(isinstance(o, ActiveTermsConflict) for o in outcomes) == 1, outcomes
    assert outcomes.count(None) == 1
    rows = {r[0]: r for r in await _rows(sessions)}
    assert rows["OLD"][1:3] == (B, "Active")
    assert rows["D"][1:3] in {(B, "Draft"), (A, "Active")}, rows["D"]


async def test_concurrent_half_patches_cannot_invert_the_dates(sessions):
    """Codex round 5: January–December; one patch moves the start to October, the other the end
    to June. Each passes the endpoint's early check on the stale row; the service re-validates
    the merged row under the lock, so the second one fails and the range is never inverted."""
    ppa_id = await _seed(
        sessions, "P", A, effective=date(2026, 1, 1), expiration=date(2026, 12, 31)
    )
    barrier = asyncio.Barrier(2)

    def make(patch):
        async def work(svc, barrier):
            await svc.get_ppa(ppa_id)
            await barrier.wait()
            await svc.update_ppa(ppa_id, patch)

        return work

    outcomes = await asyncio.gather(
        _attempt(sessions, make(ScadaPpaUpdate(effective_date=date(2026, 10, 1))), barrier),
        _attempt(sessions, make(ScadaPpaUpdate(expiration_date=date(2026, 6, 30))), barrier),
    )
    assert sum(isinstance(o, ScadaPpaTermsError) for o in outcomes) == 1, outcomes
    assert outcomes.count(None) == 1
    (row,) = await _rows(sessions)
    assert row[3] < row[4], row
    assert (row[3], row[4]) in {
        (date(2026, 10, 1), date(2026, 12, 31)),
        (date(2026, 1, 1), date(2026, 6, 30)),
    }
