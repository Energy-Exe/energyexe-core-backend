"""sweep_stuck_import_jobs fails in-process import rows orphaned by a restart."""

from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.dialects import postgresql

from app.models.import_job_execution import ImportJobStatus
from app.services import import_job_service as svc_mod
from app.services.import_job_service import sweep_stuck_import_jobs
from tests.test_import_job_execute_nonblocking import _Ctx, flat_params


def _db(rows):
    db = MagicMock()
    result = MagicMock()
    result.all.return_value = rows
    db.execute = AsyncMock(return_value=result)
    db.commit = AsyncMock()
    return db


async def test_predicate_targets_in_process_rows_with_no_time_cutoff(monkeypatch):
    db = _db([(1, "entsoe-scheduled"), (2, "opportunity-detection")])
    monkeypatch.setattr(svc_mod, "get_session_factory", lambda: lambda: _Ctx(db))

    count = await sweep_stuck_import_jobs()

    assert count == 2
    db.commit.assert_awaited_once()
    stmt = db.execute.await_args.args[0]
    compiled = stmt.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    values = flat_params(compiled)

    assert sql.startswith("UPDATE import_job_executions")
    assert "->>" in sql and "RETURNING" in sql
    assert "trigger" in values and "external" in values
    assert "opportunity-detection" in values
    assert ImportJobStatus.PENDING in values and ImportJobStatus.RUNNING in values
    assert ImportJobStatus.FAILED in values
    assert "Orphaned by service restart" in values
    # No cutoff: with one task and a stop-then-start deploy every in-process
    # row is an orphan at boot; a cutoff would leave the guard returning it.
    assert "created_at <" not in sql and "started_at" not in sql


async def test_no_orphans_returns_zero(monkeypatch):
    db = _db([])
    monkeypatch.setattr(svc_mod, "get_session_factory", lambda: lambda: _Ctx(db))

    assert await sweep_stuck_import_jobs() == 0
    db.commit.assert_awaited_once()
