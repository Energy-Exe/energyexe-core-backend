"""Tests for the nightly pipeline job body (issue #113).

Verifies that `run_pipeline_job`:
  * runs the performance batch and THEN opportunity detection (exactly once),
  * skips detection when the batch raises, so detection cannot mask a batch
    failure,
  * returns a truthful **exit code** in each case.

That last point is the contract the ECS task-failure alarm depends on. The job
previously returned `None` on every path — including both failure paths — so a
failed nightly run exited 0 and looked like a success.

No database required — the service classes and session factory are fully mocked.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.cron import pipeline_daily


def _fake_session_factory():
    """Return a session-factory mock whose `()` yields an async ctx manager.

    `session_factory()` is used as `async with session_factory() as db:`.
    """

    @asynccontextmanager
    async def _ctx():
        yield MagicMock(name="db_session")

    factory = MagicMock(name="session_factory", side_effect=lambda: _ctx())
    return factory


@pytest.mark.asyncio
async def test_detection_invoked_after_pipeline_batch():
    """Detection runs exactly once, AFTER the pipeline batch, and the job passes."""
    calls = []

    async def fake_batch(*args, **kwargs):
        calls.append("batch")
        return {"windfarms_processed": 3}

    async def fake_detection(*args, **kwargs):
        calls.append("detection")
        return {"job_id": 1, "windfarms_scanned": 3, "opportunities_created": 2}

    batch_mock = AsyncMock(side_effect=fake_batch)
    detection_mock = AsyncMock(side_effect=fake_detection)

    with patch("app.core.database.get_session_factory", _fake_session_factory), patch(
        "app.services.performance_pipeline_service.PerformancePipelineService.run_pipeline_batch",
        batch_mock,
    ), patch(
        "app.services.opportunity_detection_service.OpportunityDetectionService.run_detection_job",
        detection_mock,
    ):
        exit_code = await pipeline_daily.run_pipeline_job()

    assert exit_code == pipeline_daily.EXIT_OK
    batch_mock.assert_called_once()
    detection_mock.assert_called_once()
    # Ordering: batch before detection.
    assert calls == ["batch", "detection"]


@pytest.mark.asyncio
async def test_batch_failure_skips_detection_and_fails_the_job():
    """A batch failure returns EXIT_BATCH_FAILED and detection is NOT run."""
    batch_mock = AsyncMock(side_effect=RuntimeError("boom"))
    detection_mock = AsyncMock(return_value={})

    with patch("app.core.database.get_session_factory", _fake_session_factory), patch(
        "app.services.performance_pipeline_service.PerformancePipelineService.run_pipeline_batch",
        batch_mock,
    ), patch(
        "app.services.opportunity_detection_service.OpportunityDetectionService.run_detection_job",
        detection_mock,
    ):
        # Must not propagate — the job reports failure through its return value.
        exit_code = await pipeline_daily.run_pipeline_job()

    assert exit_code == pipeline_daily.EXIT_BATCH_FAILED
    batch_mock.assert_called_once()
    # Detection skipped — it cannot mask the batch failure.
    detection_mock.assert_not_called()


@pytest.mark.asyncio
async def test_detection_failure_fails_the_job_but_batch_stands():
    """A detection failure is its own exit code — the batch still ran."""
    batch_mock = AsyncMock(return_value={"windfarms_processed": 3})
    detection_mock = AsyncMock(side_effect=RuntimeError("detection boom"))

    with patch("app.core.database.get_session_factory", _fake_session_factory), patch(
        "app.services.performance_pipeline_service.PerformancePipelineService.run_pipeline_batch",
        batch_mock,
    ), patch(
        "app.services.opportunity_detection_service.OpportunityDetectionService.run_detection_job",
        detection_mock,
    ):
        exit_code = await pipeline_daily.run_pipeline_job()

    assert exit_code == pipeline_daily.EXIT_DETECTION_FAILED
    batch_mock.assert_called_once()
    detection_mock.assert_called_once()


@pytest.mark.asyncio
async def test_windfarm_ids_scope_both_phases():
    """--windfarm-ids reaches both services — this is what makes a smoke test fast."""
    batch_mock = AsyncMock(return_value={"windfarms_processed": 2})
    detection_mock = AsyncMock(return_value={})

    with patch("app.core.database.get_session_factory", _fake_session_factory), patch(
        "app.services.performance_pipeline_service.PerformancePipelineService.run_pipeline_batch",
        batch_mock,
    ), patch(
        "app.services.opportunity_detection_service.OpportunityDetectionService.run_detection_job",
        detection_mock,
    ):
        exit_code = await pipeline_daily.run_pipeline_job(windfarm_ids=[7404, 7200])

    assert exit_code == pipeline_daily.EXIT_OK
    assert batch_mock.call_args.kwargs["windfarm_ids"] == [7404, 7200]
    assert detection_mock.call_args.kwargs["windfarm_ids"] == [7404, 7200]


@pytest.mark.asyncio
async def test_skip_detection():
    """--skip-detection runs the batch alone and still passes."""
    batch_mock = AsyncMock(return_value={"windfarms_processed": 1})
    detection_mock = AsyncMock(return_value={})

    with patch("app.core.database.get_session_factory", _fake_session_factory), patch(
        "app.services.performance_pipeline_service.PerformancePipelineService.run_pipeline_batch",
        batch_mock,
    ), patch(
        "app.services.opportunity_detection_service.OpportunityDetectionService.run_detection_job",
        detection_mock,
    ):
        exit_code = await pipeline_daily.run_pipeline_job(skip_detection=True)

    assert exit_code == pipeline_daily.EXIT_OK
    batch_mock.assert_called_once()
    detection_mock.assert_not_called()
