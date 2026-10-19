"""Service for exchange rate lookups and currency conversion."""

from datetime import date
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

import structlog
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.exchange_rate import ExchangeRate

logger = structlog.get_logger()

# Currencies with ECB rate data
SUPPORTED_CURRENCIES = {"NOK", "GBP", "DKK", "USD"}

# Valid values for display_currency query params (EUR is the rate base)
ALLOWED_DISPLAY_CURRENCIES = {"EUR"} | SUPPORTED_CURRENCIES


class ExchangeRateService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _avg_inverse_rate(
        self, quote_currency: str, period_start: date, period_end: date
    ) -> Optional[Decimal]:
        """AVG(inverse_rate) for quote_currency in the period — gives EUR per 1 unit."""
        result = await self.db.execute(
            select(func.avg(ExchangeRate.inverse_rate)).where(
                and_(
                    ExchangeRate.base_currency == "EUR",
                    ExchangeRate.quote_currency == quote_currency,
                    ExchangeRate.rate_date >= period_start,
                    ExchangeRate.rate_date <= period_end,
                )
            )
        )
        val = result.scalar_one_or_none()
        return Decimal(str(val)).quantize(Decimal("0.000001")) if val is not None else None

    async def _avg_rate(
        self, quote_currency: str, period_start: date, period_end: date
    ) -> Optional[Decimal]:
        """AVG(rate) for quote_currency in the period — gives units of quote per 1 EUR."""
        result = await self.db.execute(
            select(func.avg(ExchangeRate.rate)).where(
                and_(
                    ExchangeRate.base_currency == "EUR",
                    ExchangeRate.quote_currency == quote_currency,
                    ExchangeRate.rate_date >= period_start,
                    ExchangeRate.rate_date <= period_end,
                )
            )
        )
        val = result.scalar_one_or_none()
        return Decimal(str(val)).quantize(Decimal("0.000001")) if val is not None else None

    async def _avg_cross_rate(
        self,
        from_currency: str,
        to_currency: str,
        period_start: date,
        period_end: date,
    ) -> Optional[Decimal]:
        """
        Cross-rate via EUR: AVG(from.inverse_rate * to.rate) joined on rate_date.

        from.inverse_rate = EUR per 1 unit of from_currency
        to.rate = units of to_currency per 1 EUR
        Product = units of to_currency per 1 unit of from_currency
        """
        from_tbl = ExchangeRate.__table__.alias("f")
        to_tbl = ExchangeRate.__table__.alias("t")

        result = await self.db.execute(
            select(func.avg(from_tbl.c.inverse_rate * to_tbl.c.rate)).where(
                and_(
                    from_tbl.c.base_currency == "EUR",
                    from_tbl.c.quote_currency == from_currency,
                    to_tbl.c.base_currency == "EUR",
                    to_tbl.c.quote_currency == to_currency,
                    from_tbl.c.rate_date == to_tbl.c.rate_date,
                    from_tbl.c.rate_date >= period_start,
                    from_tbl.c.rate_date <= period_end,
                )
            )
        )
        val = result.scalar_one_or_none()
        return Decimal(str(val)).quantize(Decimal("0.000001")) if val is not None else None

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

        Supports:
        - Same currency (rate = 1)
        - X → EUR (uses inverse_rate)
        - EUR → X (uses rate)
        - X → Y cross-currency (via EUR intermediary, joined on matching dates)

        Simple average of daily rates per IAS 21 for P&L translation.
        """
        if from_currency == to_currency:
            return Decimal("1")

        rate = None

        if from_currency == "EUR":
            # EUR → X: multiply by rate (units of X per 1 EUR)
            rate = await self._avg_rate(to_currency, period_start, period_end)
        elif to_currency == "EUR":
            # X → EUR: multiply by inverse_rate (EUR per 1 unit of X)
            rate = await self._avg_inverse_rate(from_currency, period_start, period_end)
        else:
            # Cross: X → EUR → Y
            rate = await self._avg_cross_rate(from_currency, to_currency, period_start, period_end)

        if rate is None:
            logger.warning(
                "No exchange rate data found",
                from_currency=from_currency,
                to_currency=to_currency,
                period_start=str(period_start),
                period_end=str(period_end),
            )

        return rate

    async def rates_for_periods(
        self,
        periods: Iterable[Tuple[str, date, date]],
        to_currencies: Iterable[str],
    ) -> Dict[Tuple[str, date, date, str], Optional[Decimal]]:
        """``get_rate_for_period`` for many ``(from, start, end)`` periods and targets
        from one read of the daily rates (same averages, same rounding).

        Keyed ``(from, start, end, to)``; ``None`` where a period has no rates.
        """
        periods = set(periods)
        targets = set(to_currencies)
        out: Dict[Tuple[str, date, date, str], Optional[Decimal]] = {}
        if not periods or not targets:
            return out
        quotes = ({c for c, _, _ in periods} | targets) - {"EUR"}
        daily: Dict[str, Dict[date, Tuple[Decimal, Decimal]]] = {c: {} for c in quotes}
        if quotes:
            result = await self.db.execute(
                select(
                    ExchangeRate.quote_currency,
                    ExchangeRate.rate_date,
                    ExchangeRate.rate,
                    ExchangeRate.inverse_rate,
                ).where(
                    and_(
                        ExchangeRate.base_currency == "EUR",
                        ExchangeRate.quote_currency.in_(sorted(quotes)),
                        ExchangeRate.rate_date >= min(s for _, s, _ in periods),
                        ExchangeRate.rate_date <= max(e for _, _, e in periods),
                    )
                )
            )
            for q, d, rate, inverse in result.all():
                daily[q][d] = (Decimal(str(rate)), Decimal(str(inverse)))

        def avg(values: List[Decimal]) -> Optional[Decimal]:
            if not values:
                return None
            return (sum(values, Decimal("0")) / len(values)).quantize(Decimal("0.000001"))

        for frm, start, end in periods:
            for to in targets:
                if frm == to:
                    rate: Optional[Decimal] = Decimal("1")
                elif frm == "EUR":
                    rate = avg([r for d, (r, _) in daily[to].items() if start <= d <= end])
                elif to == "EUR":
                    rate = avg([i for d, (_, i) in daily[frm].items() if start <= d <= end])
                else:
                    to_days = daily[to]
                    rate = avg(
                        [
                            i * to_days[d][0]
                            for d, (_, i) in daily[frm].items()
                            if start <= d <= end and d in to_days
                        ]
                    )
                out[(frm, start, end, to)] = rate
        return out

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
