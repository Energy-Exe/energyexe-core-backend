"""POST /import-jobs/trigger/{job} dedups concurrent triggers per job key.

Once execute_job stopped blocking the event loop, a Lambda retry landing while
an import is still running would have started a second one. The endpoint now
serialises the in-flight check with the row creation under a per-job lock and
hands back the live row (200) for a scheduled re-trigger, or 409 for a backfill
with explicit dates. The endpoint function is called directly; the service
methods are stubbed.
"""

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from app.api.v1.endpoints import import_jobs as endpoint
from app.models.import_job_execution import ImportJobStatus
from app.services.import_job_service import INFLIGHT_GUARD_WINDOW, ImportJobService
from tests.test_import_job_execute_nonblocking import make_job


@pytest.fixture(autouse=True)
def _reset_locks():
    endpoint._trigger_locks.clear()
    yield
    endpoint._trigger_locks.clear()


@pytest.fixture
def stubs(monkeypatch):
    find = AsyncMock(return_value=None)
    create = AsyncMock()
    execute = AsyncMock()
    monkeypatch.setattr(ImportJobService, "find_inflight_scheduled", find)
    monkeypatch.setattr(ImportJobService, "create_job", create)
    monkeypatch.setattr(ImportJobService, "execute_job", execute)
    return find, create, execute


async def _trigger(job_name, **kwargs):
    kwargs.setdefault("start", None)
    kwargs.setdefault("end", None)
    return await endpoint.trigger_scheduled_job(job_name, db=MagicMock(), **kwargs)


async def test_inflight_row_is_returned_without_starting_a_new_import(stubs):
    find, create, execute = stubs
    find.return_value = make_job(id=7, status=ImportJobStatus.RUNNING)

    response = await _trigger("entsoe-daily")

    assert response.id == 7
    assert response.status == "running"
    find.assert_awaited_once_with("entsoe-daily")
    create.assert_not_awaited()
    execute.assert_not_awaited()


async def test_backfill_while_a_run_is_in_flight_is_409(stubs):
    find, create, execute = stubs
    find.return_value = make_job(id=7, status=ImportJobStatus.RUNNING)

    with pytest.raises(HTTPException) as excinfo:
        await _trigger("entsoe-daily", start=date(2026, 9, 1), end=date(2026, 9, 2))

    assert excinfo.value.status_code == 409
    assert "job 7" in excinfo.value.detail
    create.assert_not_awaited()


async def test_no_inflight_row_creates_and_executes_exactly_once(stubs):
    find, create, execute = stubs
    create.return_value = make_job(id=8)
    execute.return_value = make_job(id=8, status=ImportJobStatus.SUCCESS)

    response = await _trigger("taipower-hourly")

    assert response.status == "success"
    create.assert_awaited_once()
    execute.assert_awaited_once_with(8)
    request = create.await_args.args[0]
    assert request.job_metadata == {"job_config": "taipower-hourly", "trigger": "external"}


async def test_concurrent_triggers_serialise_on_the_lock(monkeypatch):
    created = make_job(id=9, status=ImportJobStatus.PENDING)
    state = {"row": None, "creates": 0}

    async def _find(self, job_config, window=INFLIGHT_GUARD_WINDOW):
        return state["row"]

    async def _create(self, request, user_id=None, job_type=None):
        state["creates"] += 1
        await asyncio.sleep(0.01)  # the window a second request must not slip into
        state["row"] = created
        return created

    async def _execute(self, job_id):
        return make_job(id=job_id, status=ImportJobStatus.SUCCESS)

    monkeypatch.setattr(ImportJobService, "find_inflight_scheduled", _find)
    monkeypatch.setattr(ImportJobService, "create_job", _create)
    monkeypatch.setattr(ImportJobService, "execute_job", _execute)

    first, second = await asyncio.gather(_trigger("entsoe-daily"), _trigger("entsoe-daily"))

    assert state["creates"] == 1
    assert sorted([first.status, second.status]) == ["pending", "success"]


async def test_unknown_job_is_still_400(stubs):
    with pytest.raises(HTTPException) as excinfo:
        await _trigger("nope-daily")

    assert excinfo.value.status_code == 400
