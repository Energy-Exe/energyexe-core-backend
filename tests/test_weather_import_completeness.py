"""WeatherImportCore._is_date_complete (EPR-121).

The day-complete check decides whether a CDS download is skipped. It must be
(a) a half-open range on `hour` — `date(hour) = :d` was a full scan of the
~270M-row table — and (b) scoped to the farms in play, so a newly added farm's
empty history counts as incomplete even though the fleet's rows for that day
already exist (the legacy script's hard-coded 38,184 could never see that).
"""

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.weather_import import WeatherImportCore


class _Ctx:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _patch(monkeypatch, count):
    fake_result = MagicMock()
    fake_result.scalar.return_value = count
    fake_session = MagicMock()
    fake_session.execute = AsyncMock(return_value=fake_result)
    monkeypatch.setattr("app.core.database.get_session_factory", lambda: lambda: _Ctx(fake_session))
    return fake_session


def _farms(*ids):
    return [MagicMock(id=i) for i in ids]


def _compiled(fake_session):
    stmt = fake_session.execute.await_args.args[0]
    compiled = stmt.compile()
    return str(compiled), compiled.params


@pytest.mark.asyncio
async def test_complete_only_when_every_farm_has_its_24_rows(monkeypatch):
    core = WeatherImportCore()
    _patch(monkeypatch, 48)
    assert await core._is_date_complete(date(2026, 1, 2), _farms(8806, 7204)) is True
    _patch(monkeypatch, 47)
    assert await core._is_date_complete(date(2026, 1, 2), _farms(8806, 7204)) is False


@pytest.mark.asyncio
async def test_predicate_is_a_half_open_range_scoped_to_the_farms(monkeypatch):
    fake_session = _patch(monkeypatch, 0)

    await WeatherImportCore()._is_date_complete(date(2026, 1, 2), _farms(8806))

    sql, params = _compiled(fake_session)
    assert "date(" not in sql.lower()
    assert "weather_data.hour >= " in sql and "weather_data.hour < " in sql
    assert "weather_data.windfarm_id IN" in sql
    assert params["hour_1"] == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert params["hour_2"] == datetime(2026, 1, 3, tzinfo=timezone.utc)
    assert params["windfarm_id_1"] == [8806]


@pytest.mark.asyncio
async def test_none_count_is_incomplete(monkeypatch):
    _patch(monkeypatch, None)
    assert await WeatherImportCore()._is_date_complete(date(2026, 1, 2), _farms(1)) is False


@pytest.mark.asyncio
async def test_fleet_threshold_grows_with_the_fleet(monkeypatch):
    """Adding a farm makes a previously complete day incomplete again — the self-healing property."""
    core = WeatherImportCore()
    _patch(monkeypatch, 48)
    assert await core._is_date_complete(date(2026, 1, 2), _farms(1, 2)) is True
    _patch(monkeypatch, 48)
    assert await core._is_date_complete(date(2026, 1, 2), _farms(1, 2, 3)) is False
