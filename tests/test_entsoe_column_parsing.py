"""Unit tests for ENTSOE per-plant column parsing and empty-response retry (EPR-108).

entsoe-py 0.7.1 builds MultiIndex column names as (plantname, psrtype, metric, eic)
when a TimeSeries has <psrType>, but (plantname, metric, eic) when it lacks one,
and pandas pads mixed-length tuples with NaN floats. The old parser assumed fixed
positions and used a bare `"W" in level` check, which could capture "Wind Offshore"
as an EIC code and silently drop real data.
"""

import math
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from app.services.entsoe_client import ENTSOEClient, _parse_plant_column

BALTIC_EIC = "19WBALTICPOWERPO"
FR_EIC = "17W100P100P0987B"
SOLAR_EIC = "19W000000000SOLR"


class TestParsePlantColumn:
    def test_clean_four_tuple_generation(self):
        parsed = _parse_plant_column(
            ("MFW Baltic Power", "Wind Offshore", "Actual Aggregated", BALTIC_EIC)
        )
        assert parsed == {
            "unit_name": "MFW Baltic Power",
            "eic_code": BALTIC_EIC,
            "data_direction": "generation",
            "production_type": "Wind Offshore",
        }

    def test_clean_four_tuple_consumption(self):
        parsed = _parse_plant_column(
            ("Ferme FR", "Wind Onshore", "Actual Consumption", FR_EIC)
        )
        assert parsed["data_direction"] == "consumption"
        assert parsed["eic_code"] == FR_EIC

    def test_three_tuple_with_eic_no_psrtype(self):
        # entsoe-py shape when the TimeSeries lacks <psrType>
        parsed = _parse_plant_column(("Plant", "Actual Aggregated", BALTIC_EIC))
        assert parsed["eic_code"] == BALTIC_EIC
        assert parsed["production_type"] is None
        assert parsed["data_direction"] == "generation"

    def test_three_tuple_consumption_metric_position_varies(self):
        parsed = _parse_plant_column(("Plant", "Actual Consumption", BALTIC_EIC))
        assert parsed["data_direction"] == "consumption"

    def test_three_tuple_psrtype_without_eic_is_skipped(self):
        # The old bug: "Wind Offshore" contains "W" and was captured as the EIC.
        assert _parse_plant_column(("Plant", "Wind Offshore", "Actual Aggregated")) is None

    def test_nan_padded_tuples(self):
        nan = float("nan")
        parsed = _parse_plant_column(("Plant", "Actual Aggregated", BALTIC_EIC, nan))
        assert parsed["eic_code"] == BALTIC_EIC
        parsed = _parse_plant_column(("Plant", nan, "Actual Aggregated", BALTIC_EIC))
        assert parsed["eic_code"] == BALTIC_EIC
        assert parsed["data_direction"] == "generation"

    def test_eic_shaped_plant_name_not_captured(self):
        # Position 0 is the plant name and must never be used as the EIC.
        assert _parse_plant_column(("19W000000000FAKE", "Wind Offshore", "Actual Aggregated")) is None

    def test_non_tuple_column(self):
        assert _parse_plant_column("Actual Aggregated") is None


def _synthetic_df(columns):
    index = pd.date_range("2026-07-15", periods=4, freq="15min", tz="Europe/Warsaw")
    data = {col: [1.0, 2.0, 3.0, 4.0] for col in columns}
    df = pd.DataFrame(data, index=index)
    df.columns = pd.MultiIndex.from_tuples(columns)
    return df


class TestFetchGenerationPerUnit:
    def _client_with_response(self, df):
        client = ENTSOEClient(api_key="test")
        client.client.query_generation_per_plant = MagicMock(return_value=df)
        return client

    async def test_end_to_end_schema_filter_and_split(self):
        df = _synthetic_df([
            ("MFW Baltic Power", "Wind Offshore", "Actual Aggregated", BALTIC_EIC),
            ("Ferme FR", "Wind Onshore", "Actual Aggregated", FR_EIC),
            ("Ferme FR", "Wind Onshore", "Actual Consumption", FR_EIC),
            ("Solar Plant", "Solar", "Actual Aggregated", SOLAR_EIC),
        ])
        client = self._client_with_response(df)

        result, metadata = await client.fetch_generation_per_unit(
            start=datetime(2026, 7, 15),
            end=datetime(2026, 7, 16),
            area_code="10YPL-AREA-----S",
            eic_codes=None,
            production_types=["wind"],
        )

        assert metadata["success"] is True
        assert set(result.columns) == {
            "timestamp", "value", "unit_name", "eic_code",
            "area_code", "data_direction", "unit",
        }
        # Solar dropped by the wind filter; FR split into generation+consumption
        found = {(u["eic_code"], u["data_direction"]) for u in metadata["units_found"]}
        assert found == {
            (BALTIC_EIC, "generation"),
            (FR_EIC, "generation"),
            (FR_EIC, "consumption"),
        }
        # Timestamps converted from Europe/Warsaw to UTC (July: UTC+2)
        assert result["timestamp"].iloc[0] == "2026-07-14T22:00:00"

    async def test_eic_filter_keeps_only_requested_unit(self):
        df = _synthetic_df([
            ("MFW Baltic Power", "Wind Offshore", "Actual Aggregated", BALTIC_EIC),
            ("Ferme FR", "Wind Onshore", "Actual Aggregated", FR_EIC),
        ])
        client = self._client_with_response(df)

        result, metadata = await client.fetch_generation_per_unit(
            start=datetime(2026, 7, 15),
            end=datetime(2026, 7, 16),
            area_code="10YPL-AREA-----S",
            eic_codes=[BALTIC_EIC],
            production_types=["wind"],
        )

        assert set(result["eic_code"]) == {BALTIC_EIC}
        assert metadata["diagnostic"]["matched_eic_codes"] == [BALTIC_EIC]
        assert metadata["diagnostic"]["missing_eic_codes"] == []

    async def test_three_tuple_column_still_matched(self):
        # psrType-less TimeSeries: (plantname, metric, eic)
        df = _synthetic_df([("MFW Baltic Power", "Actual Aggregated", BALTIC_EIC)])
        client = self._client_with_response(df)

        result, metadata = await client.fetch_generation_per_unit(
            start=datetime(2026, 7, 15),
            end=datetime(2026, 7, 16),
            area_code="10YPL-AREA-----S",
            eic_codes=[BALTIC_EIC],
            production_types=["wind"],
        )

        assert not result.empty
        assert metadata["units_found"][0]["eic_code"] == BALTIC_EIC

    async def test_no_matching_units_reports_error(self):
        df = _synthetic_df([("Ferme FR", "Wind Onshore", "Actual Aggregated", FR_EIC)])
        client = self._client_with_response(df)

        result, metadata = await client.fetch_generation_per_unit(
            start=datetime(2026, 7, 15),
            end=datetime(2026, 7, 16),
            area_code="10YPL-AREA-----S",
            eic_codes=[BALTIC_EIC],
            production_types=["wind"],
        )

        assert result.empty
        assert metadata["success"] is False
        assert "No matching units found" in metadata["errors"]
        assert metadata["diagnostic"]["missing_eic_codes"] == [BALTIC_EIC]


class TestEmptyResponseRetry:
    """The entsoe-py RangeIndex.set_levels artifact (empty response) must be
    retried — Aug 13 prod logs showed transient empties for PL while an
    identical query succeeded 22s earlier."""

    RANGE_INDEX_ERROR = TypeError("'RangeIndex' object has no attribute 'set_levels'")

    async def test_transient_empty_then_success(self):
        df = _synthetic_df([
            ("MFW Baltic Power", "Wind Offshore", "Actual Aggregated", BALTIC_EIC),
        ])
        client = ENTSOEClient(api_key="test")
        mock_query = MagicMock(side_effect=[self.RANGE_INDEX_ERROR, self.RANGE_INDEX_ERROR, df])
        client.client.query_generation_per_plant = mock_query

        with patch("app.services.entsoe_client.asyncio.sleep", new=AsyncMock()):
            result, metadata = await client.fetch_generation_per_unit(
                start=datetime(2026, 7, 15),
                end=datetime(2026, 7, 16),
                area_code="10YPL-AREA-----S",
                eic_codes=[BALTIC_EIC],
                production_types=["wind"],
            )

        assert mock_query.call_count == 3
        assert not result.empty
        assert metadata["success"] is True

    async def test_persistent_empty_returns_no_data(self):
        client = ENTSOEClient(api_key="test")
        mock_query = MagicMock(side_effect=[self.RANGE_INDEX_ERROR] * 3)
        client.client.query_generation_per_plant = mock_query

        with patch("app.services.entsoe_client.asyncio.sleep", new=AsyncMock()):
            result, metadata = await client.fetch_generation_per_unit(
                start=datetime(2026, 7, 15),
                end=datetime(2026, 7, 16),
                area_code="10YGB----------A",
                eic_codes=None,
                production_types=["wind"],
            )

        assert mock_query.call_count == 3
        assert result.empty
        assert metadata["success"] is False
        assert "No data available for the specified parameters" in metadata["errors"]
