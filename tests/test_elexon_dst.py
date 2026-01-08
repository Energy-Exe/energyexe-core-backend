"""Tests for Elexon client DST handling."""

import pandas as pd
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo


def convert_settlement_to_utc(settlement_date: str, settlement_period: int) -> str:
    """
    Convert Elexon settlement date and period to UTC timestamp.

    This is a standalone function that mirrors the logic in ElexonClient.fetch_physical_data
    for testing purposes.
    """
    df = pd.DataFrame({
        "settlement_date": [settlement_date],
        "settlement_period": [settlement_period]
    })

    uk_dates = pd.to_datetime(df["settlement_date"]).dt.tz_localize(
        "Europe/London", ambiguous="infer", nonexistent="shift_forward"
    )
    utc_dates = uk_dates.dt.tz_convert("UTC")
    timestamp = utc_dates + pd.to_timedelta(
        (df["settlement_period"] - 1) * 30, unit="minutes"
    )
    timestamp = timestamp.dt.tz_localize(None)

    return timestamp.dt.strftime("%Y-%m-%dT%H:%M:%S").iloc[0]


class TestElexonDSTHandling:
    """Test DST handling in Elexon settlement period conversion."""

    def test_normal_summer_day_period_1(self):
        """Test period 1 on a normal summer day (BST = UTC+1)."""
        # June 15, 2024 - normal BST day
        # Period 1 starts at 00:00 BST = 23:00 UTC (previous day)
        result = convert_settlement_to_utc("2024-06-15", 1)
        assert result == "2024-06-14T23:00:00"

    def test_normal_summer_day_period_48(self):
        """Test period 48 on a normal summer day."""
        # Period 48 starts at 23:30 BST = 22:30 UTC
        result = convert_settlement_to_utc("2024-06-15", 48)
        assert result == "2024-06-15T22:30:00"

    def test_normal_winter_day_period_1(self):
        """Test period 1 on a normal winter day (GMT = UTC+0)."""
        # January 15, 2024 - normal GMT day
        # Period 1 starts at 00:00 GMT = 00:00 UTC
        result = convert_settlement_to_utc("2024-01-15", 1)
        assert result == "2024-01-15T00:00:00"

    def test_normal_winter_day_period_48(self):
        """Test period 48 on a normal winter day."""
        # Period 48 starts at 23:30 GMT = 23:30 UTC
        result = convert_settlement_to_utc("2024-01-15", 48)
        assert result == "2024-01-15T23:30:00"

    def test_spring_forward_day_period_1(self):
        """Test period 1 on spring forward day (March 31, 2024)."""
        # March 31, 2024 - clocks go forward at 01:00 GMT -> 02:00 BST
        # Period 1 starts at 00:00 GMT = 00:00 UTC
        result = convert_settlement_to_utc("2024-03-31", 1)
        assert result == "2024-03-31T00:00:00"

    def test_spring_forward_day_period_46(self):
        """Test period 46 on spring forward day (last period of the day)."""
        # On spring forward, there are only 46 periods (23 hours)
        # Day starts at 00:00 GMT = 00:00 UTC
        # Period 46 = 00:00 UTC + (45 * 30 min) = 22:30 UTC
        # This ends at 23:00 UTC = 00:00 BST (next day), which is correct
        result = convert_settlement_to_utc("2024-03-31", 46)
        assert result == "2024-03-31T22:30:00"

    def test_fall_back_day_period_1(self):
        """Test period 1 on fall back day (October 27, 2024)."""
        # October 27, 2024 - clocks go back at 02:00 BST -> 01:00 GMT
        # Period 1 starts at 00:00 BST = 23:00 UTC (Oct 26)
        result = convert_settlement_to_utc("2024-10-27", 1)
        assert result == "2024-10-26T23:00:00"

    def test_fall_back_day_period_50(self):
        """Test period 50 on fall back day (last period of the day)."""
        # On fall back, there are 50 periods (25 hours)
        # Period 50 starts at 23:30 GMT = 23:30 UTC
        result = convert_settlement_to_utc("2024-10-27", 50)
        assert result == "2024-10-27T23:30:00"

    def test_fall_back_day_periods_around_transition(self):
        """Test periods around the DST transition on fall back day."""
        # Period 5 (2 hours after midnight BST): 02:00 BST = 01:00 UTC
        result = convert_settlement_to_utc("2024-10-27", 5)
        assert result == "2024-10-27T01:00:00"

        # Period 6: 02:30 BST = 01:30 UTC
        result = convert_settlement_to_utc("2024-10-27", 6)
        assert result == "2024-10-27T01:30:00"

    def test_vectorized_conversion(self):
        """Test that vectorized conversion works correctly with multiple rows."""
        df = pd.DataFrame({
            "settlement_date": ["2024-06-15", "2024-01-15", "2024-10-27"],
            "settlement_period": [1, 1, 50]
        })

        uk_dates = pd.to_datetime(df["settlement_date"]).dt.tz_localize(
            "Europe/London", ambiguous="infer", nonexistent="shift_forward"
        )
        utc_dates = uk_dates.dt.tz_convert("UTC")
        timestamps = utc_dates + pd.to_timedelta(
            (df["settlement_period"] - 1) * 30, unit="minutes"
        )
        timestamps = timestamps.dt.tz_localize(None)
        results = timestamps.dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()

        assert results[0] == "2024-06-14T23:00:00"  # Summer period 1
        assert results[1] == "2024-01-15T00:00:00"  # Winter period 1
        assert results[2] == "2024-10-27T23:30:00"  # Fall back period 50


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
