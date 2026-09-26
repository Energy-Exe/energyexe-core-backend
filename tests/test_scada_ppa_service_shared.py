"""EPR-143: the SCADA PPA register is SHARED per farm — no ScadaPpaService statement filters on
``created_by_id`` any more (EPR-136's per-user predicate is gone), while ``create_ppas`` still stamps
the caller as "entered by".

DB-free by design — conftest's SQLite cannot host ``scada_ppa`` (see test_scada_ppas_endpoints), and
the property under test is structural: a fake session captures each statement, we compile it for
Postgres and assert on the SQL. If someone re-introduces an ownership predicate, or drops the
provenance stamp, this is the test that goes red.
"""

import inspect

import pytest
from sqlalchemy.dialects import postgresql

from app.models.scada_ppa import ScadaPpa
from app.schemas.scada_ppa import ScadaPpaCreate, ScadaPpaUpdate
from app.services.scada_ppa_service import ScadaPpaService

USER = 42
OWNED = "scada_ppa.created_by_id ="


class _Result:
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount

    def scalar(self):
        return len(self._rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    """Records every statement; returns empty results so nothing downstream needs a real row."""

    def __init__(self):
        self.statements = []
        self.added = []

    async def execute(self, stmt):
        self.statements.append(stmt)
        return _Result()

    def add_all(self, rows):
        self.added.extend(rows)

    async def commit(self):
        return None

    async def refresh(self, row):
        return None

    async def delete(self, row):
        return None


def _sql(stmt) -> str:
    return str(stmt.compile(dialect=postgresql.dialect()))


def _touches_scada_ppa(sql: str) -> bool:
    return "scada_ppa" in sql


@pytest.fixture
def db():
    return _FakeSession()


@pytest.mark.asyncio
async def test_no_scada_ppa_statement_carries_an_ownership_predicate(db):
    """Every user-facing method, every statement touching scada_ppa: no created_by_id filter."""
    svc = ScadaPpaService(db)
    await svc.list_ppas(q="stat", windfarm_id=7309)
    await svc.get_ppa(1)
    await svc.get_by_code("X")
    await svc.existing_pairs("X", [7309, 7197])
    await svc.update_ppa(1, ScadaPpaUpdate(ppa_buyer="Y"))
    await svc.delete_ppa(1)
    await svc.delete_by_code("X")
    await svc.missing_windfarm_ids([7309])
    sqls = [_sql(s) for s in db.statements]
    touching = [s for s in sqls if _touches_scada_ppa(s)]
    assert len(touching) >= 8, "sanity: the methods above do query scada_ppa"
    assert not any(OWNED in s for s in touching), [s for s in touching if OWNED in s]
    assert any("windfarms" in s and not _touches_scada_ppa(s) for s in sqls)


@pytest.mark.asyncio
async def test_list_filters_are_the_only_predicates(db):
    await ScadaPpaService(db).list_ppas(windfarm_id=7309, ppa_status="Active", q="stat")
    sqls = [_sql(s) for s in db.statements]
    assert len(sqls) == 2, "count + page"
    for sql in sqls:
        assert "scada_ppa.windfarm_id =" in sql and "scada_ppa.ppa_status =" in sql
        assert OWNED not in sql
    # an unfiltered list is the whole register
    db.statements.clear()
    await ScadaPpaService(db).list_ppas()
    assert "WHERE" not in _sql(db.statements[0])


@pytest.mark.asyncio
async def test_create_stamps_the_caller_as_entered_by_on_every_leg(db):
    payload = ScadaPpaCreate(
        ppa_code="PPA-2026-001", ppa_buyer="Statkraft", windfarm_ids=[7309, 7197]
    )
    await ScadaPpaService(db).create_ppas(payload, user_id=USER)
    assert [r.created_by_id for r in db.added] == [USER, USER]
    assert isinstance(db.added[0], ScadaPpa)
    # the re-read for the response is by code, not by owner
    assert OWNED not in _sql(db.statements[-1])


@pytest.mark.asyncio
async def test_existing_pairs_checks_only_the_requested_farms(db):
    await ScadaPpaService(db).existing_pairs("PPA-2026-001", [7309, 7197])
    sql = _sql(db.statements[-1])
    assert "scada_ppa.ppa_code =" in sql and "scada_ppa.windfarm_id IN" in sql
    assert OWNED not in sql
    assert await ScadaPpaService(db).existing_pairs("PPA-2026-001", []) == []


def test_user_id_is_keyword_only_on_create_and_absent_elsewhere():
    """Provenance is stamped from the token on create; no other method takes a user at all."""
    create = inspect.signature(ScadaPpaService.create_ppas).parameters["user_id"]
    assert create.kind is inspect.Parameter.KEYWORD_ONLY
    assert create.default is inspect.Parameter.empty
    for name in (
        "list_ppas",
        "get_ppa",
        "get_by_code",
        "existing_pairs",
        "update_ppa",
        "delete_ppa",
        "delete_by_code",
        "missing_windfarm_ids",
    ):
        assert "user_id" not in inspect.signature(getattr(ScadaPpaService, name)).parameters, name
