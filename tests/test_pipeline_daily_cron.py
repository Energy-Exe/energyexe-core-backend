"""Tests for the daily pipeline cron job wiring (issue #113).

Verifies that `run_pipeline_job`:
  * runs the performance batch and THEN opportunity detection (exactly once),
  * still fires the failure alert when the batch raises, and in that case
    does NOT run detection (so detection cannot mask a batch failure).

No database required — the service classes, alert service and session factory
are fully mocked.
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
    """Detection runs exactly once, AFTER the pipeline batch."""
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
        await pipeline_daily.run_pipeline_job()

    batch_mock.assert_called_once()
    detection_mock.assert_called_once()
    # Ordering: batch before detection.
    assert calls == ["batch", "detection"]


@pytest.mark.asyncio
async def test_pipeline_failure_still_alerts():
    """A batch failure fires the alert path and detection is NOT run."""
    batch_mock = AsyncMock(side_effect=RuntimeError("boom"))
    detection_mock = AsyncMock(return_value={})
    create_alert_mock = AsyncMock(return_value=None)

    alert_instance = MagicMock()
    alert_instance.create_system_alert = create_alert_mock

    with patch("app.core.database.get_session_factory", _fake_session_factory), patch(
        "app.services.performance_pipeline_service.PerformancePipelineService.run_pipeline_batch",
        batch_mock,
    ), patch(
        "app.services.opportunity_detection_service.OpportunityDetectionService.run_detection_job",
        detection_mock,
    ), patch(
        "app.services.alert_service.AlertService", return_value=alert_instance
    ):
        # Must not propagate — the cron job swallows + alerts.
        await pipeline_daily.run_pipeline_job()

    batch_mock.assert_called_once()
    # Alert fired for the batch failure.
    create_alert_mock.assert_called_once()
    _, alert_kwargs = create_alert_mock.call_args
    assert alert_kwargs["title"] == "Pipeline daily job failed"
    assert alert_kwargs["severity"] == "HIGH"
    # Detection skipped — it cannot mask the batch failure.
    detection_mock.assert_not_called()
