"""The API lifespan runs every startup sweeper and never fails on one."""

import pytest
from fastapi import FastAPI

pytest.importorskip("claude_agent_sdk")

import app.services.brain_agent_repo_manager as repo_manager  # noqa: E402
import app.services.brain_agent_service as brain_agent_service  # noqa: E402
import app.services.import_job_service as import_job_service  # noqa: E402
import app.services.reports.orchestrator as orchestrator  # noqa: E402
from app.main import lifespan  # noqa: E402


async def test_lifespan_runs_all_sweepers_and_tolerates_a_failing_one(monkeypatch):
    monkeypatch.setenv("TESTING", "true")  # skips init_db
    calls = []

    async def _reports():
        calls.append("reports")
        return 0

    async def _imports():
        calls.append("imports")
        raise RuntimeError("db down")

    async def _threads():
        calls.append("threads")
        return 0

    monkeypatch.setattr(repo_manager, "ensure_repos", lambda: calls.append("repos"))
    monkeypatch.setattr(orchestrator, "sweep_stuck_reports", _reports)
    monkeypatch.setattr(import_job_service, "sweep_stuck_import_jobs", _imports)
    monkeypatch.setattr(brain_agent_service, "sweep_stuck_agent_threads", _threads)

    async with lifespan(FastAPI()):
        pass

    assert calls == ["repos", "reports", "imports", "threads"]
