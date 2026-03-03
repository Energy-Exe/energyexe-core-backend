"""Exchange rate model for ECB daily rates."""

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from app.core.database import Base


class ExchangeRate(Base):
    __tablename__ = "exchange_rates"
    __table_args__ = (
        UniqueConstraint(
            "base_currency", "quote_currency", "rate_date",
            name="uq_exchange_rate_pair_date",
        ),
        Index("ix_exchange_rate_quote_date", "quote_currency", "rate_date"),
    )

    id = Column(Integer, primary_key=True, index=True)
    base_currency = Column(String(3), nullable=False)       # Always "EUR"
    quote_currency = Column(String(3), nullable=False)      # NOK, GBP, DKK, USD
    rate_date = Column(Date, nullable=False)                # Business day date
    rate = Column(Numeric(12, 6), nullable=False)           # Units of quote per 1 EUR
    inverse_rate = Column(Numeric(12, 6), nullable=False)   # 1 / rate (EUR per 1 quote)
    source = Column(String(50), nullable=False, default="ECB")

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self) -> str:
        return f"<ExchangeRate({self.base_currency}/{self.quote_currency} {self.rate_date}: {self.rate})>"
