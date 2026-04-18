"""Unit tests for WeatherImportCore._compute_windfarm_bbox.

The bbox helper replaces a previously hardcoded Europe-only bbox that caused
xarray.interp to silently produce NaN wind speeds for ~1,168 non-EU windfarms.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.weather_import import WeatherImportCore


class _AsyncSessionCtx:
    """Async-context-manager wrapper around a fake session."""

    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _patch_session_factory(monkeypatch, fetchall_return):
    """Make get_session_factory() return a factory that yields a fake session.

    The fake session's `execute()` returns an object whose `fetchall()` returns
    the provided value, mimicking SQLAlchemy's Result interface.
    """
    fake_result = MagicMock()
    fake_result.fetchall.return_value = fetchall_return

    fake_session = MagicMock()
    fake_session.execute = AsyncMock(return_value=fake_result)

    monkeypatch.setattr(
        "app.core.database.get_session_factory",
        lambda: lambda: _AsyncSessionCtx(fake_session),
    )


@pytest.mark.asyncio
async def test_bbox_includes_us_taiwan_and_europe(monkeypatch):
    """USA, Taiwan, and Denmark coords all fall inside the returned bbox."""
    rows = [
        MagicMock(lat=41.0, lng=-71.5),   # Block Island (USA)
        MagicMock(lat=23.7, lng=120.4),   # Formosa (Taiwan)
        MagicMock(lat=55.5, lng=8.0),     # Danish coast
    ]
    _patch_session_factory(monkeypatch, rows)

    bbox = await WeatherImportCore()._compute_windfarm_bbox()
    n, w, s, e = bbox

    # All input coords must lie strictly inside the bbox (with the 0.5° buffer)
    assert s <= 23.7 - 0.4, f"south {s} should be <= 23.3"
    assert n >= 55.5 + 0.4, f"north {n} should be >= 55.9"
    assert w <= -71.5 - 0.4, f"west {w} should be <= -71.9"
    assert e >= 120.4 + 0.4, f"east {e} should be >= 120.8"


@pytest.mark.asyncio
async def test_bbox_empty_db_returns_global_default(monkeypatch):
    """No windfarms with coords → global bbox, not Europe-only, no crash."""
    _patch_session_factory(monkeypatch, [])

    bbox = await WeatherImportCore()._compute_windfarm_bbox()

    assert bbox == [85.0, -180.0, -85.0, 180.0]


@pytest.mark.asyncio
async def test_bbox_buffer_applied(monkeypatch):
    """Buffer is added in the correct direction on each side."""
    rows = [MagicMock(lat=50.0, lng=10.0)]
    _patch_session_factory(monkeypatch, rows)

    bbox = await WeatherImportCore()._compute_windfarm_bbox(buffer_deg=1.0)

    # Single point with 1° buffer: N=51, W=9, S=49, E=11
    assert bbox == [51.0, 9.0, 49.0, 11.0]
