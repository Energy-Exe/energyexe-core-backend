"""scripts/jobs/run_weather_daily.py — window resolution and CLI shape (EPR-121)."""

import argparse
import importlib.util
from datetime import date
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "run_weather_daily",
    Path(__file__).parent.parent / "scripts" / "jobs" / "run_weather_daily.py",
)
job = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(job)

TODAY = date(2026, 8, 28)


def test_default_window_is_today_minus_lag():
    assert job.resolve_window(TODAY) == (date(2026, 8, 22), date(2026, 8, 22))
    assert job.resolve_window(TODAY, lag_days=1) == (date(2026, 8, 27), date(2026, 8, 27))


def test_explicit_day_and_range():
    assert job.resolve_window(TODAY, single=date(2026, 1, 2)) == (
        date(2026, 1, 2),
        date(2026, 1, 2),
    )
    assert job.resolve_window(TODAY, start=date(2026, 1, 1), end=date(2026, 3, 1)) == (
        date(2026, 1, 1),
        date(2026, 3, 1),
    )
    # --start alone is a one-day window
    assert job.resolve_window(TODAY, start=date(2026, 1, 1)) == (date(2026, 1, 1), date(2026, 1, 1))


def test_invalid_windows_are_rejected():
    with pytest.raises(ValueError):
        job.resolve_window(TODAY, start=date(2026, 3, 1), end=date(2026, 1, 1))
    with pytest.raises(ValueError):
        job.resolve_window(TODAY, end=date(2026, 1, 1))
    with pytest.raises(ValueError):
        job.resolve_window(TODAY, single=date(2026, 1, 1), start=date(2026, 1, 1))


def test_windfarm_ids_parse_dedupe_and_sort():
    assert job.parse_windfarm_ids("8806, 7204,8806,") == [7204, 8806]
    assert job.parse_windfarm_ids("") is None
    assert job.parse_windfarm_ids(None) is None


def test_only_the_bare_invocation_is_the_scheduled_shape():
    bare = argparse.Namespace(date=None, start=None, end=None)
    dated = argparse.Namespace(date=date(2026, 1, 2), start=None, end=None)
    ranged = argparse.Namespace(date=None, start=date(2026, 1, 1), end=date(2026, 1, 3))
    assert job.is_scheduled_shape(bare) is True
    assert job.is_scheduled_shape(dated) is False
    assert job.is_scheduled_shape(ranged) is False


def test_cron_monitor_config_follows_env(monkeypatch):
    monkeypatch.setenv("WEATHER_DAILY_HOUR", "2")
    monkeypatch.setenv("WEATHER_DAILY_MINUTE", "15")
    assert job._cron_monitor_config()["schedule"] == {"type": "crontab", "value": "15 2 * * *"}
