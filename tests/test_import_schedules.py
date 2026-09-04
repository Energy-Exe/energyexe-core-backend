"""IMPORT_SCHEDULES mirrors infra/scheduled_imports.tf and computes next runs.

The admin /import-jobs "next run" column is derived from IMPORT_SCHEDULES, and
the real firing times live in the Terraform map. The first test parses the .tf
file so the two cannot drift silently (they had: the previous implementation
keyed on the DB job_name, which never matched, so the column was always null).
"""

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.services.import_job_service import (
    IMPORT_SCHEDULES,
    ImportSchedule,
    next_scheduled_run,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
TF_FILE = REPO_ROOT / "infra" / "scheduled_imports.tf"

_TF_ENTRY = re.compile(
    r'"(?P<key>[a-z0-9-]+)"\s*=\s*\{\s*expression\s*=\s*"(?P<expr>cron\([^)]*\))"',
    re.MULTILINE,
)


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _tf_schedules() -> dict:
    text = TF_FILE.read_text()
    found = {m.group("key"): m.group("expr") for m in _TF_ENTRY.finditer(text)}
    assert found, f"no cron entries parsed from {TF_FILE}"
    return found


class TestMirrorsTerraform:
    def test_same_job_keys(self):
        assert set(_tf_schedules()) == set(IMPORT_SCHEDULES)

    def test_same_cron_expressions(self):
        tf = _tf_schedules()
        mismatches = {
            key: (tf[key], sched.to_cron())
            for key, sched in IMPORT_SCHEDULES.items()
            if tf.get(key) != sched.to_cron()
        }
        assert not mismatches, f"IMPORT_SCHEDULES drifted from {TF_FILE.name}: {mismatches}"

    def test_daily_batch_stays_inside_one_utc_day_and_clear_of_taipower(self):
        """Date windows are derived from the UTC date (import_jobs.py), and the
        hourly Taipower run at :05 blocks the worker for up to ~2 min."""
        for key, sched in IMPORT_SCHEDULES.items():
            if sched.hour is None:
                continue
            assert sched.hour == 22, key
            assert 10 <= sched.minute <= 57, key


class TestCronRendering:
    @pytest.mark.parametrize(
        "sched, expected",
        [
            (ImportSchedule(minute=5), "cron(5 * * * ? *)"),
            (ImportSchedule(minute=10, hour=22), "cron(10 22 * * ? *)"),
            (ImportSchedule(minute=50, hour=22, weekdays_only=True), "cron(50 22 ? * MON-FRI *)"),
            (ImportSchedule(minute=55, hour=22, day_of_month=1), "cron(55 22 1 * ? *)"),
        ],
    )
    def test_to_cron(self, sched, expected):
        assert sched.to_cron() == expected


class TestNextRun:
    def test_hourly_before_and_after_minute(self):
        s = ImportSchedule(minute=5)
        assert s.next_run(_utc(2026, 9, 4, 13, 4)) == _utc(2026, 9, 4, 13, 5)
        assert s.next_run(_utc(2026, 9, 4, 13, 5)) == _utc(2026, 9, 4, 14, 5)
        assert s.next_run(_utc(2026, 9, 4, 23, 30)) == _utc(2026, 9, 5, 0, 5)

    def test_daily_today_then_tomorrow(self):
        s = ImportSchedule(minute=10, hour=22)
        assert s.next_run(_utc(2026, 9, 4, 9, 0)) == _utc(2026, 9, 4, 22, 10)
        assert s.next_run(_utc(2026, 9, 4, 22, 10)) == _utc(2026, 9, 5, 22, 10)
        assert s.next_run(_utc(2026, 9, 4, 23, 59)) == _utc(2026, 9, 5, 22, 10)

    def test_weekdays_skip_the_weekend(self):
        s = ImportSchedule(minute=50, hour=22, weekdays_only=True)
        friday_late = _utc(2026, 9, 4, 23, 0)  # 2026-09-04 is a Friday
        assert s.next_run(friday_late) == _utc(2026, 9, 7, 22, 50)  # Monday
        saturday = _utc(2026, 9, 5, 10, 0)
        assert s.next_run(saturday) == _utc(2026, 9, 7, 22, 50)
        friday_morning = _utc(2026, 9, 4, 8, 0)
        assert s.next_run(friday_morning) == _utc(2026, 9, 4, 22, 50)

    def test_monthly_rolls_to_next_month_and_year(self):
        s = ImportSchedule(minute=55, hour=22, day_of_month=1)
        assert s.next_run(_utc(2026, 9, 1, 22, 0)) == _utc(2026, 9, 1, 22, 55)
        assert s.next_run(_utc(2026, 9, 1, 22, 55)) == _utc(2026, 10, 1, 22, 55)
        assert s.next_run(_utc(2026, 9, 15, 0, 0)) == _utc(2026, 10, 1, 22, 55)
        assert s.next_run(_utc(2026, 12, 20, 0, 0)) == _utc(2027, 1, 1, 22, 55)

    def test_naive_now_is_treated_as_utc(self):
        s = ImportSchedule(minute=10, hour=22)
        assert s.next_run(datetime(2026, 9, 4, 9, 0)) == _utc(2026, 9, 4, 22, 10)


class TestNextScheduledRun:
    def test_known_key(self):
        assert next_scheduled_run("entsoe-daily", _utc(2026, 9, 4, 9, 0)) == _utc(
            2026, 9, 4, 22, 10
        )

    @pytest.mark.parametrize("key", [None, "", "entsoe-scheduled", "opportunity-detection"])
    def test_unscheduled_keys_have_no_next_run(self, key):
        assert next_scheduled_run(key, _utc(2026, 9, 4, 9, 0)) is None
