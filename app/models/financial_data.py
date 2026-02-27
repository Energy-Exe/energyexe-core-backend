from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class FinancialData(Base):
    __tablename__ = "financial_data"
    __table_args__ = (
        UniqueConstraint(
            "financial_entity_id", "period_start", name="uq_financial_data_entity_period"
        ),
        Index("ix_financial_data_entity_period", "financial_entity_id", "period_start"),
        Index("ix_financial_data_period_start", "period_start"),
    )

    id = Column(Integer, primary_key=True, index=True)
    financial_entity_id = Column(
        Integer, ForeignKey("financial_entities.id"), nullable=False, index=True
    )
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    period_length_months = Column(Numeric(4, 1), nullable=True)
    currency = Column(String(3), nullable=False)  # ISO 4217: EUR, GBP, NOK, DKK
    is_synthetic = Column(Boolean, nullable=False, default=False)

    # Reported generation
    reported_generation_gwh = Column(Numeric(12, 3), nullable=True)

    # Revenue
    revenue = Column(Numeric(15, 2), nullable=True)
    other_revenue = Column(Numeric(15, 2), nullable=True)
    total_revenue = Column(Numeric(15, 2), nullable=True)

    # Operating expenses
    cost_of_goods = Column(Numeric(15, 2), nullable=True)
    grid_cost = Column(Numeric(15, 2), nullable=True)
    land_cost = Column(Numeric(15, 2), nullable=True)
    payroll_expenses = Column(Numeric(15, 2), nullable=True)
    service_agreements = Column(Numeric(15, 2), nullable=True)
    insurance = Column(Numeric(15, 2), nullable=True)
    other_operating_expenses = Column(Numeric(15, 2), nullable=True)
    total_operating_expenses = Column(Numeric(15, 2), nullable=True)

    # Profitability
    ebitda = Column(Numeric(15, 2), nullable=True)
    depreciation = Column(Numeric(15, 2), nullable=True)
    ebit = Column(Numeric(15, 2), nullable=True)
    net_interest = Column(Numeric(15, 2), nullable=True)
    net_other_financial = Column(Numeric(15, 2), nullable=True)
    earnings_before_tax = Column(Numeric(15, 2), nullable=True)
    tax = Column(Numeric(15, 2), nullable=True)
    net_income = Column(Numeric(15, 2), nullable=True)

    # Flexible / metadata
    extra_line_items = Column(JSONB, nullable=True)
    comment = Column(Text, nullable=True)
    source = Column(String(100), nullable=True)  # seed_import / excel_import / manual
    import_job_id = Column(
        Integer, ForeignKey("import_job_executions.id"), nullable=True
    )

    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    financial_entity = relationship("FinancialEntity", back_populates="financial_data")
