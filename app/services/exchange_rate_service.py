"""Service for exchange rate lookups and currency conversion."""

from datetime import date
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.exchange_rate import ExchangeRate

logger = structlog.get_logger()


class ExchangeRateService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_rate_for_period(
        self,
        from_currency: str,
        to_currency: str,
        period_start: date,
        period_end: date,
    ) -> Optional[Decimal]:
        """
        Returns the average daily conversion rate for converting from_currency to to_currency
        across a financial period.

        Simple average of daily rates is used because:
        - Financial data is period totals (no daily breakdown of revenue/expenses)
        - Industry standard per IAS 21 for P&L translation
        - Daily granularity gives more accurate averages than monthly

        Args:
            from_currency: Source currency (e.g., "NOK")
            to_currency: Target currency (must be "EUR")
            period_start: Start of the financial period
            period_end: End of the financial period

        Returns:
            Average inverse_rate for the period, or None if no rates found
        """
        if from_currency == to_currency:
            return Decimal("1")

        if to_currency != "EUR":
            raise ValueError("Only conversion to EUR is supported")

        result = await self.db.execute(
            select(func.avg(ExchangeRate.inverse_rate)).where(
                and_(
                    ExchangeRate.base_currency == "EUR",
                    ExchangeRate.quote_currency == from_currency,
                    ExchangeRate.rate_date >= period_start,
                    ExchangeRate.rate_date <= period_end,
                )
            )
        )
        avg_rate = result.scalar_one_or_none()

        if avg_rate is None:
            logger.warning(
                "No exchange rate data found",
                from_currency=from_currency,
                period_start=str(period_start),
                period_end=str(period_end),
            )
            return None

        return Decimal(str(avg_rate)).quantize(Decimal("0.000001"))

    async def convert_amount(
        self,
        amount: Decimal,
        from_currency: str,
        to_currency: str,
        period_start: date,
        period_end: date,
    ) -> Optional[Decimal]:
        """
        Convert a monetary amount using the average period rate.

        Returns:
            Converted amount rounded to 2 decimal places, or None if no rate available
        """
        rate = await self.get_rate_for_period(from_currency, to_currency, period_start, period_end)
        if rate is None:
            return None
        return round(amount * rate, 2)
