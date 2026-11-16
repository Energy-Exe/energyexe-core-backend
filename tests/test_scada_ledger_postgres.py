"""The value-ledger service and its status keys against an ISOLATED PostgreSQL (never the shared staging DB).

Skipped unless ``SCADA_LEDGER_PG_TEST_URL`` names a loopback database whose name ends in ``_test``, e.g.::

    createdb -h 127.0.0.1 value_ledger_api_test
    SCADA_LEDGER_PG_TEST_URL=postgresql://127.0.0.1:5432/value_ledger_api_test poetry run pytest tests/test_scada_ledger_postgres.py

Each test rebuilds the tables it needs: users and scada_finding_action from the models, and the pipeline's
scada.opportunity_run / opportunity_register / opportunity_ledger_* in their pipeline shape.
"""

import datetime as dt
import json
import os

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import Settings
from app.core.database import Base
from app.models.scada_finding_action import ScadaFindingAction
from app.models.user import User
from app.services import scada_finding_service, scada_ledger_service
from app.services.scada_finding_service import ScadaFindingService
from app.services.scada_ledger_service import JSON_FIELDS, ScadaLedgerService, scada_ledger_present
from tests.test_scada_ledger_document import close, sample

URL = os.environ.get("SCADA_LEDGER_PG_TEST_URL")
pytestmark = pytest.mark.skipif(
    not URL, reason="SCADA_LEDGER_PG_TEST_URL not set (isolated Postgres only)"
)

REGISTER_DDL = """
CREATE SCHEMA scada;
CREATE TABLE scada.opportunity_run (farm varchar(64), asset_code varchar(16), run_id varchar(64) NOT NULL,
                                    PRIMARY KEY (farm, asset_code));
CREATE TABLE scada.opportunity_register (farm varchar(64), asset_code varchar(16), id integer,
                                         trigger varchar(48), scope varchar(32) NOT NULL, cls varchar(16) NOT NULL,
                                         PRIMARY KEY (farm, asset_code, id))
"""
# as the pipeline's migration c6d1f8a3e9b2 creates them
LEDGER_DDL = """
CREATE TABLE scada.opportunity_ledger_cause (
    farm varchar(64), asset_code varchar(16), run_id varchar(64) NOT NULL, year integer, turbine varchar(16),
    cause varchar(96), value double precision NOT NULL, claimed_mwh double precision, claims integer,
    summed boolean NOT NULL, pipeline_version varchar(16) NOT NULL, computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (farm, asset_code, year, turbine, cause));
CREATE TABLE scada.opportunity_ledger_run (
    farm varchar(64), asset_code varchar(16), run_id varchar(64) NOT NULL, register_run_id varchar(64) NOT NULL,
    method_version varchar(64) NOT NULL, format varchar(32) NOT NULL, currency varchar(3) NOT NULL,
    window_from date NOT NULL, window_to date NOT NULL, years double precision NOT NULL,
    years_basis varchar(32) NOT NULL, years_detail text NOT NULL, headline_year double precision NOT NULL,
    today_same_years_year double precision NOT NULL, register_year double precision NOT NULL,
    realised_year double precision NOT NULL, turbines text NOT NULL, bridge text NOT NULL,
    bridge_start_basis varchar(32) NOT NULL, bridge_basis text NOT NULL, ranges text NOT NULL, coverage text NOT NULL,
    checks text NOT NULL, lines text NOT NULL, realised text NOT NULL, catalog text NOT NULL,
    pipeline_version varchar(16) NOT NULL, computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (farm, asset_code))
"""
FARM, ASSET = "raggovidda", "RG2"


def _url() -> str:
    u = make_url(URL)
    if u.host not in ("127.0.0.1", "localhost", "::1") or not (u.database or "").endswith("_test"):
        pytest.fail("SCADA_LEDGER_PG_TEST_URL must be a loopback database whose name ends in _test")
    return u.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _exec(conn, ddl: str) -> None:
    for stmt in (s for s in ddl.split(";") if s.strip()):
        await conn.execute(text(stmt))


@pytest.fixture
async def db(monkeypatch):
    monkeypatch.setattr(scada_ledger_service, "_tables_seen", False)
    on = Settings(SCADA_LEDGER_ENABLED=True)
    monkeypatch.setattr(scada_finding_service, "get_settings", lambda: on)
    engine = create_async_engine(_url())
    tables = [User.__table__, ScadaFindingAction.__table__]

    async def drop(conn):
        await conn.execute(text("DROP SCHEMA IF EXISTS scada CASCADE"))
        await conn.run_sync(lambda c: Base.metadata.drop_all(c, tables=tables[::-1]))

    async with engine.begin() as conn:
        await drop(conn)
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=tables))
        await _exec(conn, REGISTER_DDL)
    session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)()
    try:
        yield session
    finally:
        await session.close()
        async with engine.begin() as conn:
            await drop(conn)
        await engine.dispose()


async def _publish(db, build="rg2-b", current=None):
    """The ledger tables with one sample, and the register's current run (default: the one it was built beside)."""
    rows, run, _ = sample(build)
    await _exec(await db.connection(), LEDGER_DDL)
    stored = {k: json.dumps(v) if k in JSON_FIELDS else v for k, v in run.items()}
    for k in ("window_from", "window_to"):
        stored[k] = dt.date.fromisoformat(run[k])
    cols = ", ".join(stored)
    await db.execute(
        text(
            f"INSERT INTO scada.opportunity_ledger_run ({cols}, pipeline_version) "
            f"VALUES ({', '.join(':' + k for k in stored)}, 't')"
        ),
        stored,
    )
    await db.execute(
        text(
            "INSERT INTO scada.opportunity_ledger_cause (farm, asset_code, run_id, year, turbine, cause, value, "
            "claimed_mwh, claims, summed, pipeline_version) VALUES (:farm, :asset_code, :run_id, :year, :turbine, "
            ":cause, :value, :claimed_mwh, :claims, :summed, 't')"
        ),
        rows,
    )
    await _set_register_run(db, current or run["register_run_id"])
    return run


async def _set_register_run(db, run_id):
    await db.execute(text("DELETE FROM scada.opportunity_run"))
    await db.execute(
        text("INSERT INTO scada.opportunity_run VALUES (:f, :a, :r)"),
        {"f": FARM, "a": ASSET, "r": run_id},
    )
    await db.commit()


def _vl_key(run):
    c = next(c for c in run["catalog"]["causes"] if c["status_key"]["scope"].startswith("VL:"))
    return c["status_key"]


async def _act(db, key, staff=True, user_id=None):
    return await ScadaFindingService(db).set_action(
        farm=FARM,
        asset_code=ASSET,
        trigger=key["trigger"],
        scope=key["scope"],
        cls=key["cls"],
        status="CONFIRMED",
        notes=None,
        user_id=user_id,
        internal_staff=staff,
    )


async def test_absent_tables_are_absent_and_a_ledger_key_is_not_found(db):
    assert await scada_ledger_present(db) is False
    key = {"trigger": "OPS_01", "scope": "VL:downtime_fault", "cls": "REALIZED"}
    assert await _act(db, key) is None


async def test_a_current_ledger_serves_its_document_and_the_statuses_on_its_causes(db):
    run = await _publish(db)
    assert await scada_ledger_present(db)
    user = User(
        email="ada@x", username="ada", hashed_password="x", first_name="Ada", last_name="Lovelace"
    )
    db.add(user)
    await db.commit()
    vl = _vl_key(run)
    for asset, (trigger, scope, cls) in [
        (ASSET, (vl["trigger"], vl["scope"], vl["cls"])),
        (ASSET, ("MKT_02", "FLEET", "RECOVERABLE")),  # a company-wide line keeps the register key
        (ASSET, ("DET_aux", "T03", "CONDITIONAL")),  # a register key the ledger does not use
        ("RG1", (vl["trigger"], vl["scope"], vl["cls"])),
    ]:
        db.add(
            ScadaFindingAction(
                farm=FARM,
                asset_code=asset,
                trigger=trigger,
                scope=scope,
                cls=cls,
                status="ACKNOWLEDGED",
                updated_by=user.id,
            )
        )
    await db.commit()

    svc = ScadaLedgerService(db)
    assert [a["status"] for a in await svc.assets(FARM)] == ["ok"]
    got = await svc.get(FARM, ASSET)
    assert got["status"] == "ok" and got["current_register_run_id"] == run["register_run_id"]
    assert close(got["document"], sample("rg2-b")[2])
    assert sorted((s["trigger"], s["scope"]) for s in got["statuses"]) == sorted(
        [("MKT_02", "FLEET"), (vl["trigger"], vl["scope"])]
    )
    assert {s["actor"] for s in got["statuses"]} == {"Ada Lovelace"}
    assert await svc.get(FARM, "RG1") is None


async def test_a_ledger_beside_an_older_register_run_or_without_rows_is_stale(db):
    run = await _publish(db, current="a-newer-register-run")
    svc = ScadaLedgerService(db)
    got = await svc.get(FARM, ASSET)
    assert got["status"] == "stale" and got["document"] is None
    assert got["current_register_run_id"] == "a-newer-register-run"
    assert [a["status"] for a in await svc.assets(FARM)] == ["stale"]
    await _set_register_run(db, run["register_run_id"])
    assert (await svc.get(FARM, ASSET))["status"] == "ok"
    await db.execute(text("DELETE FROM scada.opportunity_ledger_cause"))
    await db.commit()
    assert (await svc.get(FARM, ASSET))["status"] == "stale"


async def test_set_action_accepts_a_ledger_key_for_staff_on_a_current_run_only(db, monkeypatch):
    run = await _publish(db)
    vl = _vl_key(run)
    action = await _act(db, vl)
    assert action is not None and (action.scope, action.status) == (vl["scope"], "CONFIRMED")
    assert await _act(db, vl, staff=False) is None
    assert await _act(db, {**vl, "scope": "VL:not_a_cause"}) is None
    assert await _act(db, {**vl, "cls": "CONDITIONAL"}) is None
    for enabled, expected in ((False, None), (True, action)):
        s = Settings(SCADA_LEDGER_ENABLED=enabled)
        monkeypatch.setattr(scada_finding_service, "get_settings", lambda: s)
        assert await _act(db, vl) is expected
    await _set_register_run(db, "a-newer-register-run")
    assert await _act(db, vl) is None


async def test_a_register_key_is_unchanged(db):
    await db.execute(
        text(
            "INSERT INTO scada.opportunity_register VALUES (:f, :a, 1, 'DET_aux', 'T03', 'CONDITIONAL')"
        ),
        {"f": FARM, "a": ASSET},
    )
    await db.commit()
    key = {"trigger": "DET_aux", "scope": "T03", "cls": "CONDITIONAL"}
    assert await _act(db, key, staff=False) is not None  # no ledger tables needed, no staff needed
    assert await _act(db, {**key, "scope": "T04"}) is None
