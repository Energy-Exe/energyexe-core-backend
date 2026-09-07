"""The EventBridge -> API trigger Lambda (infra/lambda/trigger_import/index.py).

Loaded by path: it is a stdlib-only handler, not part of the app package.
"""

import importlib.util
import urllib.error
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "infra" / "lambda" / "trigger_import" / "index.py"
)


class _Context:
    def __init__(self, remaining_ms):
        self._remaining_ms = remaining_ms

    def get_remaining_time_in_millis(self):
        return self._remaining_ms


@pytest.fixture
def lam(monkeypatch):
    monkeypatch.setenv("API_URL", "https://api.example.test")
    monkeypatch.delenv("READ_TIMEOUT_SECONDS", raising=False)
    spec = importlib.util.spec_from_file_location("trigger_import_index", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.time, "sleep", lambda _s: None)
    return module


def test_default_read_timeout_fits_under_alb_and_function_timeouts(lam):
    assert lam.READ_TIMEOUT_SECONDS == 270


def test_read_timeout_stops_in_invocation_retries_and_raises(lam, monkeypatch):
    timeouts = []

    def _post(url, timeout=None):
        timeouts.append(timeout)
        raise urllib.error.URLError(TimeoutError("timed out"))

    monkeypatch.setattr(lam, "_post", _post)

    with pytest.raises(RuntimeError, match="timed out"):
        lam.handler({"job_name": "entsoe-daily"}, _Context(300_000))

    assert timeouts == [270]  # one attempt, then hand over to the async retry


def test_in_flight_row_counts_as_success(lam, monkeypatch):
    monkeypatch.setattr(lam, "_post", lambda url, timeout=None: {"id": 5, "status": "running"})

    out = lam.handler({"job_name": "entsoe-daily"}, _Context(300_000))

    assert out == {"job_name": "entsoe-daily", "job_id": 5, "status": "running"}


def test_fast_transport_errors_retry_up_to_attempts(lam, monkeypatch):
    calls = []

    def _post(url, timeout=None):
        calls.append(url)
        raise urllib.error.URLError(ConnectionRefusedError())

    monkeypatch.setattr(lam, "_post", _post)

    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        lam.handler({"job_name": "taipower-hourly"}, _Context(300_000))

    assert len(calls) == 3
    assert calls[0] == "https://api.example.test/api/v1/import-jobs/trigger/taipower-hourly"


def test_failed_job_row_retries_then_raises(lam, monkeypatch):
    calls = []

    def _post(url, timeout=None):
        calls.append(1)
        return {"id": 1, "status": "failed", "error_message": "API 500"}

    monkeypatch.setattr(lam, "_post", _post)

    with pytest.raises(RuntimeError, match="API 500"):
        lam.handler({"job_name": "elexon-daily"}, _Context(300_000))

    assert len(calls) == 3


def test_attempt_is_skipped_when_the_budget_cannot_fit_one(lam, monkeypatch):
    calls = []
    monkeypatch.setattr(lam, "_post", lambda url, timeout=None: calls.append(timeout))

    with pytest.raises(RuntimeError):
        lam.handler({"job_name": "elexon-daily"}, _Context(20_000))

    assert calls == []


def test_attempt_timeout_is_capped_to_the_remaining_budget(lam, monkeypatch):
    timeouts = []

    def _post(url, timeout=None):
        timeouts.append(timeout)
        return {"id": 2, "status": "success", "records_imported": 10}

    monkeypatch.setattr(lam, "_post", _post)

    out = lam.handler({"job_name": "ecb-rates-daily"}, _Context(100_000))

    assert timeouts == [90]
    assert out["status"] == "success" and out["records_imported"] == 10


def test_missing_job_name_is_rejected(lam):
    with pytest.raises(ValueError):
        lam.handler({}, _Context(300_000))
