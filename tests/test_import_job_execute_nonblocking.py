"""execute_job waits for the import subprocess OFF the event loop.

Until 2026-09-05 it awaited subprocess.run inline, freezing the single uvicorn
worker for the whole import: /health timed out, the ALB marked the task
unhealthy after 150 s and ECS restarted it twice a day. No DB: the job row is a
real ImportJobExecution instance served by a fake session.
"""

import asyncio
import subprocess
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from app.models.import_job_execution import ImportJobExecution, ImportJobStatus
from app.services import import_job_service as svc_mod
from app.services.import_job_service import INFLIGHT_GUARD_WINDOW, ImportJobService


class _Ctx:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def make_job(**overrides) -> ImportJobExecution:
    fields = dict(
        id=1,
        job_name="entsoe-scheduled",
        source="ENTSOE",
        job_type="scheduled",
        import_start_date=datetime(2026, 9, 1),
        import_end_date=datetime(2026, 9, 1, 23, 59, 59),
        status=ImportJobStatus.PENDING,
        records_imported=0,
        records_updated=0,
        api_calls_made=0,
        retry_count=0,
        max_retries=3,
        job_metadata={"job_config": "entsoe-daily", "trigger": "external"},
        created_at=datetime(2026, 9, 4, 22, 10),
        updated_at=datetime(2026, 9, 4, 22, 10),
    )
    fields.update(overrides)
    return ImportJobExecution(**fields)


def fake_db(job) -> MagicMock:
    db = MagicMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = job
    db.execute = AsyncMock(return_value=result)
    db.commit = AsyncMock()
    db.close = AsyncMock()
    db.refresh = AsyncMock()
    return db


def flat_params(compiled) -> list:
    out = []
    for value in compiled.params.values():
        if isinstance(value, (list, tuple)):
            out.extend(value)
        else:
            out.append(value)
    return out


def _completed(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args="echo fake", returncode=returncode, stdout=stdout, stderr=stderr
    )


@pytest.fixture
def harness(monkeypatch):
    job = make_job()
    db = fake_db(job)
    monkeypatch.setattr(svc_mod, "get_session_factory", lambda: lambda: _Ctx(db))
    monkeypatch.setattr(ImportJobService, "_build_import_command", lambda self, j: "echo fake")
    return ImportJobService(db), job, db


async def test_execute_job_does_not_block_the_event_loop(harness, monkeypatch):
    service, _, _ = harness

    def _slow_run(*_args, **_kwargs):
        time.sleep(0.5)
        return _completed(stdout="Total Records Stored: 3\nTotal API Calls: 2\n")

    monkeypatch.setattr(svc_mod.subprocess, "run", _slow_run)

    stop = asyncio.Event()
    ticks = 0

    async def _ticker():
        nonlocal ticks
        while not stop.is_set():
            await asyncio.sleep(0.05)
            ticks += 1

    ticker = asyncio.create_task(_ticker())
    result = await service.execute_job(1)
    stop.set()
    await ticker

    assert result.status == ImportJobStatus.SUCCESS
    assert result.records_imported == 3
    assert result.api_calls_made == 2
    # Blocking code yields 0-1 ticks here; a free loop turns ~10 times.
    assert ticks >= 4


async def test_request_session_is_closed_before_the_import_runs(harness, monkeypatch):
    service, _, db = harness
    order = []

    async def _close():
        order.append("close")

    db.close = _close
    monkeypatch.setattr(
        svc_mod.subprocess, "run", lambda *a, **k: (order.append("run"), _completed())[1]
    )

    await service.execute_job(1)

    assert order[:2] == ["close", "run"]


async def test_nonzero_exit_marks_failed_with_stderr(harness, monkeypatch):
    service, _, _ = harness
    monkeypatch.setattr(
        svc_mod.subprocess, "run", lambda *a, **k: _completed(returncode=1, stderr="boom " * 300)
    )

    result = await service.execute_job(1)

    assert result.status == ImportJobStatus.FAILED
    assert result.error_message.startswith("boom")
    assert len(result.error_message) == 1000


async def test_subprocess_timeout_marks_failed(harness, monkeypatch):
    service, _, _ = harness

    def _raise(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="echo fake", timeout=3600)

    monkeypatch.setattr(svc_mod.subprocess, "run", _raise)

    result = await service.execute_job(1)

    assert result.status == ImportJobStatus.FAILED
    assert result.error_message == "Job timeout after 1 hour"


async def test_already_running_row_is_rejected(harness):
    service, job, _ = harness
    job.status = ImportJobStatus.RUNNING

    with pytest.raises(ValueError, match="already running"):
        await service.execute_job(1)


async def test_find_inflight_scheduled_predicate(harness):
    service, _, db = harness
    before = datetime.now(timezone.utc).replace(tzinfo=None)

    await service.find_inflight_scheduled("entsoe-daily")

    stmt = db.execute.await_args.args[0]
    compiled = stmt.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    values = flat_params(compiled)

    assert "->>" in sql, sql
    assert "job_config" in values and "entsoe-daily" in values
    assert ImportJobStatus.PENDING in values and ImportJobStatus.RUNNING in values
    assert ImportJobStatus.SUCCESS not in values
    assert "ORDER BY" in sql and "LIMIT" in sql
    cutoff = next(v for v in values if isinstance(v, datetime))
    assert abs((before - INFLIGHT_GUARD_WINDOW) - cutoff) < timedelta(seconds=5)
