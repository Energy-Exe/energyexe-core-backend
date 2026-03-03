"""Schemas for exchange rate API responses."""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel


class ExchangeRateResponse(BaseModel):
    id: int
    base_currency: str
    quote_currency: str
    rate_date: date
    rate: Decimal
    inverse_rate: Decimal
    source: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ExchangeRateListResponse(BaseModel):
    items: List[ExchangeRateResponse]
    total: int
    currency: Optional[str] = None
    year: Optional[int] = None
