"""Tests for per-windfarm failure reporting in `run_pipeline_batch`.

Context: the first EventBridge-scheduled nightly run (2026-08-18) reported
1478 succeeded / 136 failed and logged **nothing** about which windfarms failed
or why — the batch emitted zero error-level lines. A windfarm can fail without
raising: `run_pipeline` returns an error dict for data-coverage conditions, and
only the raising path was logged. A farm could therefore fail every night
unnoticed.

These tests pin:
  * every failure is logged with the windfarm id and a machine-readable code,
  * the aggregate summary carries a reason histogram and the failed ids,
  * a failed **power-curve** build counts as a FAILURE rather than a success.

That last one was a real miscount: `run_pipeline` returned `result` with the
error nested under `result["power_curves"]`, while the batch classifies on the
*top-level* "error" key — so those runs were tallied as successes and the true
failure count was understated.

No database required — the session factory and job row are mocked.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.performance_pipeline_service import PerformancePipelineService

WF_OK = 1
WF_NO_DATA = 2
WF_NO_CAPACITY = 3
WF_CURVE_FAILED = 4
WF_RAISES = 5

# What run_pipeline returns for each windfarm, mirroring its real exit shapes.
OUTCOMES = {
    WF_OK: {"windfarm_id": WF_OK, "power_curves": {"years": [2024]}},
    WF_NO_DATA: {
        "windfarm_id": WF_NO_DATA,
        "error": "No hourly data",
        "error_code": "no_hourly_data",
    },
    WF_NO_CAPACITY: {
        "windfarm_id": WF_NO_CAPACITY,
        "error": "No rated capacity",
        "error_code": "no_rated_capacity",
    },
    WF_CURVE_FAILED: {
        "windfarm_id": WF_CURVE_FAILED,
        "power_curves": {"error": "no bins"},
        "error": "no bins",
        "error_code": "power_curve_failed",
    },
}


def _fake_factory():
    @asynccontextmanager
    async def _ctx():
        yield MagicMock(name="wf_db", commit=AsyncMock())

    return MagicMock(side_effect=lambda: _ctx())


async def _run_batch(windfarm_ids):
    """Drive run_pipeline_batch with a stubbed run_pipeline and mocked job row."""

    async def fake_run_pipeline(self, wf_id, **kwargs):
        if wf_id == WF_RAISES:
            raise RuntimeError("connection died")
        return OUTCOMES[wf_id]

    db = MagicMock(name="batch_db")
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    job = MagicMock(id=99)
    db.get = AsyncMock(return_value=job)

    svc = PerformancePipelineService(db)

    with patch("app.core.database.get_session_factory", _fake_factory), patch.object(
        PerformancePipelineService, "run_pipeline", fake_run_pipeline
    ):
        return await svc.run_pipeline_batch(windfarm_ids=windfarm_ids)


@pytest.mark.asyncio
async def test_failures_are_counted_and_broken_down_by_reason():
    result = await _run_batch([WF_OK, WF_NO_DATA, WF_NO_CAPACITY, WF_RAISES])

    assert result["windfarms_processed"] == 4
    assert result["succeeded"] == 1
    assert result["failed"] == 3
    assert result["failure_reasons"] == {
        "no_hourly_data": 1,
        "no_rated_capacity": 1,
        "exception": 1,
    }


@pytest.mark.asyncio
async def test_power_curve_failure_counts_as_a_failure_not_a_success():
    """Regression guard: this used to be tallied as a success.

    The error lived only at result["power_curves"]["error"], and the batch
    classifies on the top-level key.
    """
    result = await _run_batch([WF_OK, WF_CURVE_FAILED])

    assert result["succeeded"] == 1
    assert result["failed"] == 1
    assert result["failure_reasons"] == {"power_curve_failed": 1}


@pytest.mark.asyncio
async def test_every_failure_names_its_windfarm_in_the_logs():
    """The whole point: a failed farm must be identifiable from the logs."""
    with patch("app.services.performance_pipeline_service.logger") as log:
        await _run_batch([WF_OK, WF_NO_DATA, WF_CURVE_FAILED, WF_RAISES])

    # Non-raising failures -> warning, one per failed windfarm, with the id.
    warned = {
        c.kwargs["windfarm_id"]: c.kwargs["error_code"]
        for c in log.warning.call_args_list
        if c.args and c.args[0] == "pipeline_windfarm_failed"
    }
    assert warned == {
        WF_NO_DATA: "no_hourly_data",
        WF_CURVE_FAILED: "power_curve_failed",
    }

    # The raising path stays at error level and keeps its traceback.
    errored = [c for c in log.error.call_args_list if c.args[0] == "pipeline_windfarm_error"]
    assert len(errored) == 1
    assert errored[0].kwargs["windfarm_id"] == WF_RAISES
    assert errored[0].kwargs["exc_info"] is True

    # A single summary line answers "what failed tonight and why".
    summary = [c for c in log.info.call_args_list if c.args[0] == "performance_pipeline_complete"]
    assert len(summary) == 1
    assert summary[0].kwargs["failed_windfarm_ids"] == [WF_NO_DATA, WF_CURVE_FAILED, WF_RAISES]
    assert summary[0].kwargs["failure_reasons"] == {
        "no_hourly_data": 1,
        "power_curve_failed": 1,
        "exception": 1,
    }


@pytest.mark.asyncio
async def test_clean_run_reports_no_failures():
    result = await _run_batch([WF_OK])

    assert result["succeeded"] == 1
    assert result["failed"] == 0
    assert result["failure_reasons"] == {}
