"""SCADA review fixes, stage 5: each client portal lists its own agent chats."""

from types import SimpleNamespace

import pytest

from app.api.v1.endpoints import brain_agent as ep
from app.models.agent_thread import AgentThread
from app.schemas.brain_agent import ThreadUpsertRequest


class _Db:
    def __init__(self, found=None):
        self.found = found
        self.statements = []
        self.added = []

    async def execute(self, stmt):
        self.statements.append(stmt)
        found = self.found
        return SimpleNamespace(
            scalars=lambda: SimpleNamespace(all=lambda: []),
            scalar_one_or_none=lambda: found,
        )

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        pass

    async def refresh(self, obj):
        pass


USER = SimpleNamespace(id=7)


def _where(db):
    return str(db.statements[0].compile(compile_kwargs={"literal_binds": True}))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "portal, expected, absent",
    [
        ("scada", "agent_threads.portal = 'scada'", "IS NULL"),
        ("perform", "agent_threads.portal IS NULL OR agent_threads.portal = 'perform'", None),
        (None, None, "portal"),
    ],
)
async def test_list_filters_by_portal(portal, expected, absent):
    db = _Db()
    await ep.list_threads(limit=50, offset=0, portal=portal, db=db, current_user=USER)
    sql = _where(db)
    if expected:
        assert expected in sql
    if absent:
        assert absent not in sql.split("WHERE", 1)[1]


def _detail_stub(monkeypatch):
    monkeypatch.setattr(ep.ThreadDetail, "model_validate", classmethod(lambda cls, t: t))


@pytest.mark.asyncio
async def test_a_new_chat_keeps_its_portal(monkeypatch):
    _detail_stub(monkeypatch)
    db = _Db(found=None)
    body = ThreadUpsertRequest(title="q", portal="scada")
    await ep.upsert_thread("t1", body, db=db, current_user=USER)
    assert db.added[0].portal == "scada"


@pytest.mark.asyncio
async def test_a_server_saved_chat_gets_its_portal_once(monkeypatch):
    _detail_stub(monkeypatch)
    thread = AgentThread(id="t1", user_id=7, title="q", messages=[], portal=None)
    await ep.upsert_thread(
        "t1", ThreadUpsertRequest(title="q", portal="scada"), db=_Db(thread), current_user=USER
    )
    assert thread.portal == "scada"
    # Never moved to another portal afterwards.
    await ep.upsert_thread(
        "t1", ThreadUpsertRequest(title="q", portal="perform"), db=_Db(thread), current_user=USER
    )
    assert thread.portal == "scada"
