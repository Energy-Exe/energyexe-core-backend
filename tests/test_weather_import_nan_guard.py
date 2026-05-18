"""Unit tests for the NaN guard in WeatherImportCore._extract_windfarm_data.

Without this guard, NaN floats from xarray.interp (returned for out-of-bbox
or masked grid cells) propagate through `float(NaN)` -> `math.sqrt(NaN)` and
land in the database as NaN rows, blocking the downstream pipeline.
"""

import math
from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from app.core.weather_import import WeatherImportCore


def _fake_dataset(u100_vals, v100_vals, t2m_vals):
    """Build a minimal mock that mimics the xarray.Dataset surface used by
    `_extract_windfarm_data` (variable indexing + .interp(...).values + .time)."""
    n = len(u100_vals)

    def _make_var(arr):
        var = MagicMock()
        var.interp.return_value.values = np.array(arr, dtype=float)
        return var

    ds = MagicMock()
    ds.__getitem__.side_effect = lambda key: {
        "u100": _make_var(u100_vals),
        "v100": _make_var(v100_vals),
        "t2m": _make_var(t2m_vals),
    }[key]

    # Real ERA5 GRIB timestamps are naive numpy.datetime64 (UTC by convention,
    # but no tzinfo). The production code wraps each with `pd.Timestamp(..., tz='UTC')`.
    times = pd.date_range("2024-06-01", periods=n, freq="h").to_numpy()
    ds.time = MagicMock()
    ds.time.values = times
    ds.time.__len__.return_value = n

    # `len(ds.latitude)` / `len(ds.longitude)` are referenced for log output
    ds.latitude = MagicMock()
    ds.latitude.__len__.return_value = 10
    ds.longitude = MagicMock()
    ds.longitude.__len__.return_value = 10

    return ds


def _windfarm(wf_id=7361, lat=41.0, lng=-71.5):
    wf = MagicMock()
    wf.id = wf_id
    wf.lat = lat
    wf.lng = lng
    return wf


def test_all_nan_hours_skipped():
    """When ERA5 returns NaN for u100/v100/t2m, no records are produced."""
    nan_arr = [math.nan] * 24
    finite = [5.0] * 24
    ds = _fake_dataset(nan_arr, finite, finite)

    records = WeatherImportCore()._extract_windfarm_data(
        ds, [_windfarm()], target_date=date(2024, 6, 1)
    )

    assert records == []


def test_partial_nan_hours_skipped():
    """Only the NaN hours are skipped; finite hours produce records."""
    u100 = [math.nan if i < 12 else 5.0 for i in range(24)]
    v100 = [3.0] * 24
    t2m = [283.15] * 24
    ds = _fake_dataset(u100, v100, t2m)

    records = WeatherImportCore()._extract_windfarm_data(
        ds, [_windfarm()], target_date=date(2024, 6, 1)
    )

    assert len(records) == 12
    for r in records:
        assert not math.isnan(r["wind_speed_100m"])
        assert not math.isnan(r["wind_direction_deg"])


def test_finite_hours_produce_records():
    """Healthy ERA5 data: 24 records per windfarm with finite numeric values."""
    finite_u = [4.0] * 24
    finite_v = [3.0] * 24  # sqrt(16+9) = 5
    t2m = [283.15] * 24    # 10 °C

    ds = _fake_dataset(finite_u, finite_v, t2m)

    records = WeatherImportCore()._extract_windfarm_data(
        ds, [_windfarm()], target_date=date(2024, 6, 1)
    )

    assert len(records) == 24
    for r in records:
        assert r["windfarm_id"] == 7361
        assert r["source"] == "ERA5"
        assert r["wind_speed_100m"] == 5.0
        assert not math.isnan(r["temperature_2m_c"])
        # 283.15 K -> 10 °C
        assert r["temperature_2m_c"] == 10.0
