"""Unit tests for ECB Exchange Rate API client."""

from datetime import date
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from app.services.ecb_client import ECBExchangeRateClient


SAMPLE_ECB_CSV = """KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK,OBS_COM,TIME_FORMAT,COLLECTION,COMPILING_ORG,DISS_ORG,DOM_SER_IDS,PUBL_ECB,PUBL_MU,PUBL_PUBLIC,UNIT_INDEX_BASE,COMPILATION_METH,COVERAGE,DECIMALS,NAT_TITLE,SOURCE_AGENCY,SOURCE_PUB,TITLE,TITLE_COMPL,UNIT,UNIT_MULT
EXR.D.NOK.EUR.SP00.A,D,NOK,EUR,SP00,A,2023-01-02,10.5138,A,F,,,,A,,,,0,,,,4,,4,Norwegian krone,4F0,,Norwegian krone/Euro,ECB reference exchange rate  Norwegian krone/Euro,NOK,0
EXR.D.NOK.EUR.SP00.A,D,NOK,EUR,SP00,A,2023-01-03,10.4916,A,F,,,,A,,,,0,,,,4,,4,Norwegian krone,4F0,,Norwegian krone/Euro,ECB reference exchange rate  Norwegian krone/Euro,NOK,0
EXR.D.NOK.EUR.SP00.A,D,NOK,EUR,SP00,A,2023-01-04,10.6295,A,F,,,,A,,,,0,,,,4,,4,Norwegian krone,4F0,,Norwegian krone/Euro,ECB reference exchange rate  Norwegian krone/Euro,NOK,0
"""

EMPTY_CSV = ""


class TestFetchDailyRates:

    @pytest.mark.asyncio
    async def test_parse_ecb_csv(self):
        """Test that sample ECB CSV is parsed correctly."""
        client = ECBExchangeRateClient()

        mock_response = MagicMock()
        mock_response.text = SAMPLE_ECB_CSV
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.get.return_value = mock_response
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            df, metadata = await client.fetch_daily_rates("NOK", date(2023, 1, 2), date(2023, 1, 4))

        assert df is not None
        assert len(df) == 3
        assert list(df.columns) == ["rate_date", "rate"]
        assert df.iloc[0]["rate"] == pytest.approx(10.5138)
        assert df.iloc[2]["rate"] == pytest.approx(10.6295)
        assert metadata["records"] == 3

    @pytest.mark.asyncio
    async def test_empty_response(self):
        """Test handling of empty API response."""
        client = ECBExchangeRateClient()

        mock_response = MagicMock()
        mock_response.text = EMPTY_CSV
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.get.return_value = mock_response
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            df, metadata = await client.fetch_daily_rates("NOK", date(2023, 1, 2), date(2023, 1, 4))

        assert df is None
        assert "error" in metadata

    @pytest.mark.asyncio
    async def test_network_error(self):
        """Test handling of network failure."""
        client = ECBExchangeRateClient()

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.get.side_effect = Exception("Connection refused")
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            df, metadata = await client.fetch_daily_rates("NOK", date(2023, 1, 2), date(2023, 1, 4))

        assert df is None
        assert "error" in metadata


class TestFetchAllRates:

    @pytest.mark.asyncio
    async def test_combines_all_currencies(self):
        """Test that fetch_all_rates combines results from all currencies."""
        client = ECBExchangeRateClient()

        async def mock_fetch(currency, start, end):
            import pandas as pd
            df = pd.DataFrame({
                "rate_date": [date(2023, 1, 2)],
                "rate": [10.5 if currency == "NOK" else 0.88],
            })
            return df, {"currency": currency, "records": 1}

        with patch.object(client, "fetch_daily_rates", side_effect=mock_fetch):
            combined, metadata = await client.fetch_all_rates(date(2023, 1, 2), date(2023, 1, 2))

        assert combined is not None
        assert len(combined) == 4  # NOK, GBP, DKK, USD
        assert set(combined["currency"].unique()) == {"NOK", "GBP", "DKK", "USD"}
        assert len(metadata) == 4

    @pytest.mark.asyncio
    async def test_partial_failure(self):
        """Test that partial API failures don't block other currencies."""
        client = ECBExchangeRateClient()

        call_count = 0

        async def mock_fetch(currency, start, end):
            nonlocal call_count
            call_count += 1
            if currency == "GBP":
                return None, {"currency": currency, "error": "timeout"}
            import pandas as pd
            df = pd.DataFrame({
                "rate_date": [date(2023, 1, 2)],
                "rate": [10.5],
            })
            return df, {"currency": currency, "records": 1}

        with patch.object(client, "fetch_daily_rates", side_effect=mock_fetch):
            combined, metadata = await client.fetch_all_rates(date(2023, 1, 2), date(2023, 1, 2))

        assert combined is not None
        assert len(combined) == 3  # GBP failed, 3 succeeded
        assert "GBP" not in combined["currency"].values
