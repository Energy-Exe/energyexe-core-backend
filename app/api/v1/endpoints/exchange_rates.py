"""API endpoints for exchange rate data."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.exchange_rate import ExchangeRate
from app.schemas.exchange_rate import ExchangeRateListResponse, ExchangeRateResponse

router = APIRouter()


@router.get("", response_model=ExchangeRateListResponse)
async def list_exchange_rates(
    currency: Optional[str] = Query(None, pattern="^[A-Z]{3}$"),
    year: Optional[int] = Query(None, ge=2000, le=2100),
    db: AsyncSession = Depends(get_db),
):
    """List exchange rates with optional currency and year filters."""
    query = select(ExchangeRate)
    count_query = select(func.count(ExchangeRate.id))

    conditions = []
    if currency:
        conditions.append(ExchangeRate.quote_currency == currency)
    if year:
        conditions.append(ExchangeRate.rate_date >= date(year, 1, 1))
        conditions.append(ExchangeRate.rate_date <= date(year, 12, 31))

    if conditions:
        query = query.where(and_(*conditions))
        count_query = count_query.where(and_(*conditions))

    count_result = await db.execute(count_query)
    total = count_result.scalar() or 0

    result = await db.execute(
        query.order_by(ExchangeRate.quote_currency, ExchangeRate.rate_date.desc()).limit(1000)
    )
    items = list(result.scalars().all())

    return ExchangeRateListResponse(
        items=items,
        total=total,
        currency=currency,
        year=year,
    )
