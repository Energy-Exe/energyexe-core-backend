"""Unit tests for ExchangeRateService conversion logic."""

import asyncio
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.exchange_rate_service import ExchangeRateService


@pytest.fixture
def mock_db():
    """Create a mock AsyncSession."""
    db = AsyncMock()
    return db


@pytest.fixture
def service(mock_db):
    return ExchangeRateService(mock_db)


class TestGetRateForPeriod:

    @pytest.mark.asyncio
    async def test_same_currency_returns_one(self, service):
        rate = await service.get_rate_for_period("EUR", "EUR", date(2023, 1, 1), date(2023, 12, 31))
        assert rate == Decimal("1")

    @pytest.mark.asyncio
    async def test_same_non_eur_currency_returns_one(self, service):
        rate = await service.get_rate_for_period("NOK", "NOK", date(2023, 1, 1), date(2023, 12, 31))
        assert rate == Decimal("1")

    @pytest.mark.asyncio
    async def test_to_eur_uses_inverse_rate(self, service, mock_db):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = Decimal("0.088700")
        mock_db.execute.return_value = mock_result

        rate = await service.get_rate_for_period("NOK", "EUR", date(2023, 6, 1), date(2023, 6, 30))

        assert rate is not None
        assert rate == Decimal("0.088700")
        mock_db.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_from_eur_uses_rate(self, service, mock_db):
        # 1 EUR = 11.28 NOK
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = Decimal("11.280000")
        mock_db.execute.return_value = mock_result

        rate = await service.get_rate_for_period("EUR", "NOK", date(2023, 6, 1), date(2023, 6, 30))

        assert rate is not None
        assert rate == Decimal("11.280000")

    @pytest.mark.asyncio
    async def test_cross_currency_nok_to_gbp(self, service, mock_db):
        # Cross rate: NOK→EUR→GBP via joined daily rates
        mock_result = MagicMock()
        # AVG(nok_inverse * gbp_rate) ≈ 0.0887 * 0.86 ≈ 0.076282
        mock_result.scalar_one_or_none.return_value = Decimal("0.076282")
        mock_db.execute.return_value = mock_result

        rate = await service.get_rate_for_period("NOK", "GBP", date(2023, 1, 1), date(2023, 12, 31))

        assert rate is not None
        assert rate == Decimal("0.076282")

    @pytest.mark.asyncio
    async def test_multi_month_period(self, service, mock_db):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = Decimal("0.090123")
        mock_db.execute.return_value = mock_result

        rate = await service.get_rate_for_period("NOK", "EUR", date(2023, 1, 1), date(2023, 12, 31))

        assert rate is not None
        assert rate == Decimal("0.090123")

    @pytest.mark.asyncio
    async def test_no_rates_returns_none(self, service, mock_db):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute.return_value = mock_result

        rate = await service.get_rate_for_period("NOK", "EUR", date(2050, 1, 1), date(2050, 12, 31))

        assert rate is None


class TestConvertAmount:

    @pytest.mark.asyncio
    async def test_convert_nok_to_eur(self, service, mock_db):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = Decimal("0.088700")
        mock_db.execute.return_value = mock_result

        converted = await service.convert_amount(
            Decimal("10000000"), "NOK", "EUR", date(2023, 1, 1), date(2023, 12, 31)
        )

        assert converted is not None
        assert converted == Decimal("887000.00")

    @pytest.mark.asyncio
    async def test_convert_same_currency(self, service):
        converted = await service.convert_amount(
            Decimal("1000"), "EUR", "EUR", date(2023, 1, 1), date(2023, 12, 31)
        )
        assert converted == Decimal("1000.00")

    @pytest.mark.asyncio
    async def test_convert_missing_rate_returns_none(self, service, mock_db):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute.return_value = mock_result

        converted = await service.convert_amount(
            Decimal("1000"), "NOK", "EUR", date(2050, 1, 1), date(2050, 12, 31)
        )

        assert converted is None

    @pytest.mark.asyncio
    async def test_convert_gbp_to_eur(self, service, mock_db):
        # GBP inverse rate ~1.16 (1 GBP = 1.16 EUR)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = Decimal("1.160000")
        mock_db.execute.return_value = mock_result

        converted = await service.convert_amount(
            Decimal("5000000"), "GBP", "EUR", date(2023, 1, 1), date(2023, 12, 31)
        )

        assert converted == Decimal("5800000.00")

    @pytest.mark.asyncio
    async def test_convert_eur_to_nok(self, service, mock_db):
        # 1 EUR = 11.28 NOK
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = Decimal("11.280000")
        mock_db.execute.return_value = mock_result

        converted = await service.convert_amount(
            Decimal("100000"), "EUR", "NOK", date(2023, 1, 1), date(2023, 12, 31)
        )

        assert converted == Decimal("1128000.00")

    @pytest.mark.asyncio
    async def test_convert_nok_to_gbp_cross(self, service, mock_db):
        # Cross: NOK→EUR→GBP, combined rate ≈ 0.076282
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = Decimal("0.076282")
        mock_db.execute.return_value = mock_result

        converted = await service.convert_amount(
            Decimal("10000000"), "NOK", "GBP", date(2023, 1, 1), date(2023, 12, 31)
        )

        assert converted == Decimal("762820.00")
