"""EPR-126: the shared "where does this windfarm's generation data end" probe.

One definition serves both the Opportunity report's ``effective_window`` and
the nightly detection window clip, so both agree on a farm's data horizon.
"""

from datetime import date, datetime, timezone

import pytest

from app.services.generation_coverage import (
    GenerationCoverage,
    as_utc,
    data_through_day,
    generation_data_through,
    month_end,
)

UTC = timezone.utc


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    async def execute(self, statement, *args, **kwargs):
        self.statements.append(statement)
        return _Result(self.rows)


@pytest.mark.asyncio
async def test_hourly_source_runs_through_its_last_day():
    session = _Session([(datetime(2025, 12, 31, 23, tzinfo=UTC), "NVE")])
    cov = await generation_data_through(
        session, 7213, datetime(2024, 9, 5), datetime(2026, 8, 27, 2)
    )
    assert cov == GenerationCoverage(
        last_hour=datetime(2025, 12, 31, 23, tzinfo=UTC),
        last_day=date(2025, 12, 31),
        source="NVE",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row, expected_day",
    [
        ((datetime(2026, 6, 1, 0, tzinfo=UTC), "EIA"), date(2026, 6, 30)),
        ((datetime(2024, 2, 1, 0, tzinfo=UTC), "ENERGISTYRELSEN"), date(2024, 2, 29)),
    ],
)
async def test_monthly_sources_run_through_month_end(row, expected_day):
    """EIA / ENERGISTYRELSEN store a whole month at its first hour."""
    session = _Session([row])
    cov = await generation_data_through(session, 1, datetime(2024, 1, 1), datetime(2026, 8, 27))
    assert cov is not None
    assert cov.last_day == expected_day
    assert cov.last_hour == row[0]


@pytest.mark.asyncio
async def test_no_rows_means_no_coverage():
    session = _Session([])
    assert (
        await generation_data_through(session, 1, datetime(2024, 1, 1), datetime(2026, 1, 1))
        is None
    )


@pytest.mark.asyncio
async def test_query_shape_is_a_single_indexed_backward_scan():
    """ORDER BY hour DESC LIMIT 1 on the (windfarm_id, hour) index — never a
    grouped MAX — filtered to rows that carry a reading, bound tz-aware."""
    session = _Session([])
    await generation_data_through(session, 42, datetime(2024, 9, 5, 2), datetime(2026, 8, 27, 2))
    stmt = session.statements[0]
    sql = str(stmt)
    assert "ORDER BY generation_data.hour DESC" in sql
    assert "LIMIT" in sql
    assert "generation_data.windfarm_id = " in sql
    assert (
        "coalesce(generation_data.metered_mwh, generation_data.generation_mwh) IS NOT NULL" in sql
    )
    assert "max(" not in sql.lower()
    bound = [v for v in stmt.compile().params.values() if isinstance(v, datetime)]
    assert bound and all(v.tzinfo == UTC for v in bound)
    assert sorted(bound) == [
        datetime(2024, 9, 5, 2, tzinfo=UTC),
        datetime(2026, 8, 27, 2, tzinfo=UTC),
    ]


def test_as_utc_declares_naive_as_utc_and_converts_aware():
    # Naive input is DECLARED UTC, never shifted through the local zone.
    assert as_utc(datetime(2026, 8, 26, 3)) == datetime(2026, 8, 26, 3, tzinfo=UTC)
    plus_two = datetime(2026, 8, 26, 5, tzinfo=timezone(__import__("datetime").timedelta(hours=2)))
    assert as_utc(plus_two) == datetime(2026, 8, 26, 3, tzinfo=UTC)


def test_data_through_day_and_month_end():
    assert data_through_day(datetime(2025, 12, 31, 23), "NVE") == date(2025, 12, 31)
    assert data_through_day(datetime(2025, 12, 31, 23), None) == date(2025, 12, 31)
    assert data_through_day(datetime(2025, 11, 1, 0), "EIA") == date(2025, 11, 30)
    assert month_end(date(2024, 2, 10)) == date(2024, 2, 29)
    assert month_end(date(2025, 12, 1)) == date(2025, 12, 31)
