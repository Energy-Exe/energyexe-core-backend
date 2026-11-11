"""The hourly chart points project the normalised value onto capacity in force."""

import pandas as pd

from app.api.v1.endpoints.performance_pipeline import build_wind_normalised_points
from app.utils.capacity import BASIS_IN_FORCE, CAPACITY_BASIS_ATTR


def _hourly(with_capacity: bool) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "hour": pd.date_range("2023-01-01", periods=3, freq="h", tz="UTC"),
            "actual_mw": [10.0, 20.0, 30.0],
            "expected_mw": [12.0, 22.0, 28.0],
            "norm_ratio": [0.8, 0.9, 1.1],
            "wind_speed": [6.0, 7.0, 8.0],
        }
    )
    if with_capacity:
        df["capacity_mw"] = [150.4, 150.4, 145.8]
        df.attrs[CAPACITY_BASIS_ATTR] = BASIS_IN_FORCE
    return df


def test_points_use_per_hour_capacity_in_force():
    pts = build_wind_normalised_points(_hourly(True), 0.5, 145.8)
    assert [round(p.wind_normalised_mwh, 3) for p in pts] == [
        round(0.8 * 0.5 * 150.4, 3),
        round(0.9 * 0.5 * 150.4, 3),
        round(1.1 * 0.5 * 145.8, 3),
    ]
    assert pts[0].actual_mwh == 10.0 and pts[2].wind_speed == 8.0


def test_points_fall_back_to_rated_for_plain_frames():
    pts = build_wind_normalised_points(_hourly(False), 0.5, 100.0)
    assert [round(p.wind_normalised_mwh, 3) for p in pts] == [40.0, 45.0, 55.0]
