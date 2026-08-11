"""Regression tests for the price-import commands built by ImportJobService.

Background: `entsoe-prices-daily` / `elexon-prices-daily` fetched raw prices
correctly but wrote ZERO rows to `price_data`, silently, for as long as they
existed. `_build_import_command` passed the job's inclusive end date straight
into `process_to_hourly.py --end-date`, but the processing filter is
`period_start >= start AND period_start < end` (exclusive — see
PriceProcessingService._get_raw_prices_for_bidzone). For a single-day job that
is `>= D AND < D`, which matches nothing. Both scripts exit 0 regardless, so the
job still reported success.

These tests pin the exclusive-end contract so the off-by-one cannot come back.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest

from app.services.import_job_service import ImportJobService


def build(source: str, start: datetime, end: datetime) -> str:
    """Build the shell command for a job window without touching the DB."""
    job = SimpleNamespace(source=source, import_start_date=start, import_end_date=end)
    return ImportJobService(None)._build_import_command(job)


def process_step(command: str) -> str:
    """The `process_to_hourly.py` half of the `&&`-chained command."""
    steps = [s for s in command.split(" && ") if "process_to_hourly.py" in s]
    assert len(steps) == 1, f"expected exactly one process step, got {steps}"
    return steps[0]


# A daily job's window is D 00:00:00 -> D 23:59:59 (import_jobs.py:294-297).
DAY_START = datetime(2026, 8, 9, 0, 0, 0)
DAY_END = datetime(2026, 8, 9, 23, 59, 59)


class TestExclusiveEndDate:
    @pytest.mark.parametrize("source", ["ENTSOE_PRICES", "ELEXON_PRICES"])
    def test_single_day_process_window_is_not_empty(self, source):
        """The regression itself: --end-date must be the day AFTER the last day."""
        step = process_step(build(source, DAY_START, DAY_END))

        assert "--start-date 2026-08-09" in step
        assert "--end-date 2026-08-10" in step
        # `--start-date X --end-date X` is the empty window that caused the bug.
        assert "--end-date 2026-08-09" not in step

    @pytest.mark.parametrize("source", ["ENTSOE_PRICES", "ELEXON_PRICES"])
    def test_bound_is_a_calendar_day_not_a_microsecond(self, source):
        """Guards against 'refactoring' this to app.utils.date_bounds.exclusive_end().

        That helper returns end + 1 microsecond for a timestamped end, which is
        correct for a SQL bound but vanishes under %Y-%m-%d formatting — the
        value here is serialized to a DATE and re-parsed as midnight downstream,
        so anything short of the next calendar day reinstates the empty window.
        """
        from app.utils.date_bounds import exclusive_end

        assert exclusive_end(DAY_END).strftime("%Y-%m-%d") == "2026-08-09"
        assert f"--end-date {exclusive_end(DAY_END):%Y-%m-%d}" not in process_step(
            build(source, DAY_START, DAY_END)
        )

    @pytest.mark.parametrize("source", ["ENTSOE_PRICES", "ELEXON_PRICES"])
    def test_backfill_includes_its_final_day(self, source):
        """?start=A&end=B must process through B inclusive, not B-1."""
        step = process_step(
            build(source, datetime(2026, 2, 1, 0, 0, 0), datetime(2026, 2, 7, 23, 59, 59))
        )

        assert "--start-date 2026-02-01" in step
        assert "--end-date 2026-02-08" in step

    @pytest.mark.parametrize("source", ["ENTSOE_PRICES", "ELEXON_PRICES"])
    def test_month_and_year_boundaries_roll_over(self, source):
        """The +1 day is real date arithmetic, not string munging."""
        eoy = process_step(
            build(source, datetime(2026, 12, 31, 0, 0, 0), datetime(2026, 12, 31, 23, 59, 59))
        )
        assert "--end-date 2027-01-01" in eoy

        eom = process_step(
            build(source, datetime(2026, 2, 28, 0, 0, 0), datetime(2026, 2, 28, 23, 59, 59))
        )
        assert "--end-date 2026-03-01" in eom


class TestFetchStepUnaffected:
    """The fetch scripts pad --end to 23:59:59 themselves, so they keep the
    inclusive end. Only the process step needed the +1 day."""

    def test_entsoe_fetch_keeps_inclusive_end(self):
        command = build("ENTSOE_PRICES", DAY_START, DAY_END)
        fetch = command.split(" && ")[0]

        assert "import_prices_from_api.py" in fetch
        assert "--start 2026-08-09 --end 2026-08-09" in fetch

    def test_elexon_fetch_keeps_inclusive_end(self):
        command = build("ELEXON_PRICES", DAY_START, DAY_END)
        fetch = command.split(" && ")[0]

        assert "import_elexon_prices.py" in fetch
        assert "--start 2026-08-09 --end 2026-08-09" in fetch


class TestElexonBidzoneFilter:
    def test_elexon_process_is_scoped_to_gb(self):
        """Without this, --force walks every windfarm with a bidzone_id and
        queries ELEXON raw prices for non-GB zones that never have any."""
        step = process_step(build("ELEXON_PRICES", DAY_START, DAY_END))

        assert "--source ELEXON" in step
        assert "--bidzone-codes 10YGB----------A" in step

    def test_entsoe_process_is_not_bidzone_scoped(self):
        """ENTSOE covers all 11 mapped bidzones, so it must stay unscoped."""
        step = process_step(build("ENTSOE_PRICES", DAY_START, DAY_END))

        assert "--bidzone-codes" not in step


class TestGenerationJobsUntouched:
    """The price fix added a variable to a shared method; the five working
    jobs must still build exactly the windows they did before."""

    @pytest.mark.parametrize(
        "source,script",
        [
            ("ENTSOE", "entsoe/import_from_api.py"),
            ("ELEXON", "elexon/import_from_api.py"),
            ("ECB_RATES", "import_ecb_rates.py"),
        ],
    )
    def test_inclusive_window_preserved(self, source, script):
        command = build(source, DAY_START, DAY_END)

        assert script in command
        assert "--start 2026-08-09 --end 2026-08-09" in command
        assert "2026-08-10" not in command
