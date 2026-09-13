"""EPR-136: every ScadaPpaService statement carries the ownership predicate.

DB-free by design — conftest's SQLite cannot host ``scada_ppa`` (see test_scada_ppas_endpoints), and
the property under test is structural anyway: *no* query the service emits may touch ``scada_ppa``
without ``created_by_id = :user``. A fake session captures each statement, we compile it for
Postgres and assert on the SQL. If someone adds a method or drops the predicate, this is the test
that goes red.
"""

from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from app.models.scada_ppa import ScadaPpa
from app.schemas.scada_ppa import ScadaPpaCreate, ScadaPpaUpdate
from app.services.scada_ppa_service import ScadaPpaService

USER = 42
OWNED = "scada_ppa.created_by_id = %(created_by_id_1)s"


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
async def test_list_scopes_both_the_count_and_the_page(db):
    await ScadaPpaService(db).list_ppas(user_id=USER, q="stat", windfarm_id=7309)
    sqls = [_sql(s) for s in db.statements]
    assert len(sqls) == 2, "count + page"
    for sql in sqls:
        assert OWNED in sql, sql


@pytest.mark.asyncio
async def test_get_scopes(db):
    await ScadaPpaService(db).get_ppa(1, user_id=USER)
    assert OWNED in _sql(db.statements[-1])


@pytest.mark.asyncio
async def test_get_by_code_scopes(db):
    await ScadaPpaService(db).get_by_code("PPA-2026-001", user_id=USER)
    assert OWNED in _sql(db.statements[-1])


@pytest.mark.asyncio
async def test_update_scopes_the_lookup(db):
    await ScadaPpaService(db).update_ppa(1, ScadaPpaUpdate(ppa_buyer="X"), user_id=USER)
    assert OWNED in _sql(db.statements[-1])


@pytest.mark.asyncio
async def test_delete_scopes_the_lookup(db):
    await ScadaPpaService(db).delete_ppa(1, user_id=USER)
    assert OWNED in _sql(db.statements[-1])


@pytest.mark.asyncio
async def test_delete_by_code_scopes_the_bulk_delete(db):
    await ScadaPpaService(db).delete_by_code("PPA-2026-001", user_id=USER)
    sql = _sql(db.statements[-1])
    assert sql.startswith("DELETE FROM scada_ppa")
    assert OWNED in sql


@pytest.mark.asyncio
async def test_create_stamps_the_caller_on_every_leg(db):
    payload = ScadaPpaCreate(
        ppa_code="PPA-2026-001", ppa_buyer="Statkraft", windfarm_ids=[7309, 7197]
    )
    await ScadaPpaService(db).create_ppas(payload, user_id=USER)
    assert [r.created_by_id for r in db.added] == [USER, USER]
    assert isinstance(db.added[0], ScadaPpa)
    # and the re-read for the response is scoped too
    assert OWNED in _sql(db.statements[-1])


@pytest.mark.asyncio
async def test_no_scada_ppa_statement_escapes_the_predicate(db):
    """Belt and braces: run every user-facing method and check that *every* statement touching
    scada_ppa carries the predicate. missing_windfarm_ids is the one deliberate exception — it
    reads shared reference data (windfarms), never scada_ppa."""
    svc = ScadaPpaService(db)
    await svc.list_ppas(user_id=USER)
    await svc.get_ppa(1, user_id=USER)
    await svc.get_by_code("X", user_id=USER)
    await svc.update_ppa(1, ScadaPpaUpdate(ppa_buyer="Y"), user_id=USER)
    await svc.delete_ppa(1, user_id=USER)
    await svc.delete_by_code("X", user_id=USER)
    await svc.missing_windfarm_ids([7309])
    sqls = [_sql(s) for s in db.statements]
    touching = [s for s in sqls if _touches_scada_ppa(s)]
    assert touching, "sanity: the methods above do query scada_ppa"
    assert all(OWNED in s for s in touching), [s for s in touching if OWNED not in s]
    assert any("windfarms" in s and not _touches_scada_ppa(s) for s in sqls)


def test_user_id_is_keyword_only_everywhere():
    """A positional user_id would let a refactor swap it with ppa_id silently."""
    import inspect

    for name in (
        "list_ppas",
        "get_ppa",
        "get_by_code",
        "create_ppas",
        "update_ppa",
        "delete_ppa",
        "delete_by_code",
    ):
        param = inspect.signature(getattr(ScadaPpaService, name)).parameters["user_id"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY, name
        assert param.default is inspect.Parameter.empty, f"{name}: user_id must be required"
