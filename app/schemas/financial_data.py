from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class FinancialDataBase(BaseModel):
    period_start: date
    period_end: date
    period_length_months: Optional[Decimal] = None
    currency: str = Field(..., min_length=3, max_length=3)
    is_synthetic: bool = False

    # Reported generation
    reported_generation_gwh: Optional[Decimal] = None

    # Revenue
    revenue: Optional[Decimal] = None
    other_revenue: Optional[Decimal] = None
    total_revenue: Optional[Decimal] = None

    # Operating expenses
    cost_of_goods: Optional[Decimal] = None
    grid_cost: Optional[Decimal] = None
    land_cost: Optional[Decimal] = None
    payroll_expenses: Optional[Decimal] = None
    service_agreements: Optional[Decimal] = None
    insurance: Optional[Decimal] = None
    other_operating_expenses: Optional[Decimal] = None
    total_operating_expenses: Optional[Decimal] = None

    # Profitability
    ebitda: Optional[Decimal] = None
    depreciation: Optional[Decimal] = None
    ebit: Optional[Decimal] = None
    net_interest: Optional[Decimal] = None
    net_other_financial: Optional[Decimal] = None
    earnings_before_tax: Optional[Decimal] = None
    tax: Optional[Decimal] = None
    net_income: Optional[Decimal] = None

    # Flexible
    extra_line_items: Optional[Dict[str, Any]] = None
    comment: Optional[str] = None
    source: Optional[str] = Field(None, max_length=100)


class FinancialDataCreate(FinancialDataBase):
    financial_entity_id: int


class FinancialDataUpdate(BaseModel):
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    period_length_months: Optional[Decimal] = None
    currency: Optional[str] = Field(None, min_length=3, max_length=3)
    is_synthetic: Optional[bool] = None

    reported_generation_gwh: Optional[Decimal] = None

    revenue: Optional[Decimal] = None
    other_revenue: Optional[Decimal] = None
    total_revenue: Optional[Decimal] = None

    cost_of_goods: Optional[Decimal] = None
    grid_cost: Optional[Decimal] = None
    land_cost: Optional[Decimal] = None
    payroll_expenses: Optional[Decimal] = None
    service_agreements: Optional[Decimal] = None
    insurance: Optional[Decimal] = None
    other_operating_expenses: Optional[Decimal] = None
    total_operating_expenses: Optional[Decimal] = None

    ebitda: Optional[Decimal] = None
    depreciation: Optional[Decimal] = None
    ebit: Optional[Decimal] = None
    net_interest: Optional[Decimal] = None
    net_other_financial: Optional[Decimal] = None
    earnings_before_tax: Optional[Decimal] = None
    tax: Optional[Decimal] = None
    net_income: Optional[Decimal] = None

    extra_line_items: Optional[Dict[str, Any]] = None
    comment: Optional[str] = None
    source: Optional[str] = Field(None, max_length=100)


class FinancialData(FinancialDataBase):
    id: int
    financial_entity_id: int
    import_job_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class FinancialEntityBasic(BaseModel):
    id: int
    code: str
    name: str
    entity_type: str

    model_config = ConfigDict(from_attributes=True)


class FinancialDataWithEntity(FinancialData):
    financial_entity: Optional[FinancialEntityBasic] = None

    model_config = ConfigDict(from_attributes=True)


class FinancialDataImportError(BaseModel):
    row: int
    field: Optional[str] = None
    value: Optional[str] = None
    message: str


class FinancialDataImportResult(BaseModel):
    success: bool
    total_rows: int
    created: int
    updated: int
    skipped: int
    errors: List[FinancialDataImportError] = []
    unmatched_entities: List[str] = []


class FinancialDataListResponse(BaseModel):
    items: List[FinancialDataWithEntity]
    total: int
    limit: int
    offset: int
    has_more: bool


class FinancialDataSummary(BaseModel):
    financial_entity_id: int
    financial_entity_name: str
    financial_entity_code: str
    entity_type: str
    currency: Optional[str] = None
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    revenue: Optional[Decimal] = None
    total_revenue: Optional[Decimal] = None
    total_operating_expenses: Optional[Decimal] = None
    ebitda: Optional[Decimal] = None
    net_income: Optional[Decimal] = None
    reported_generation_gwh: Optional[Decimal] = None


class FinancialRatioPeriod(BaseModel):
    financial_data_id: int
    period_start: date
    period_end: date
    currency: str
    # Currency conversion metadata
    display_currency: str = ""
    original_currency: str = ""
    exchange_rate_used: Optional[Decimal] = None
    # Source financials
    total_revenue: Optional[Decimal] = None
    total_operating_expenses: Optional[Decimal] = None
    ebitda: Optional[Decimal] = None
    # Generation
    generation_mwh: Optional[Decimal] = None
    generation_hours_count: int = 0
    # Computed ratios
    revenue_per_mwh: Optional[Decimal] = None
    opex_per_mwh: Optional[Decimal] = None
    ebitda_margin_pct: Optional[Decimal] = None
    # Exclusion metadata
    is_ramp_up_excluded: bool = False
    ramp_up_exclusion_reason: Optional[str] = None
    generation_data_available: bool = True
    period_coverage_pct: Optional[Decimal] = None


class FinancialRatiosResponse(BaseModel):
    windfarm_id: int
    windfarm_name: str
    financial_entity_id: int
    financial_entity_name: str
    entity_type: str
    cod: Optional[date] = None
    linked_windfarm_ids: List[int]
    display_currency: Optional[str] = None
    periods: List[FinancialRatioPeriod]
