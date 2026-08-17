"""Pydantic schemas for the report generation API (EPR-81/82)."""

from datetime import date, datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ReportCreate(BaseModel):
    report_type: str = Field(..., max_length=32)
    windfarm_id: Optional[int] = None
    portfolio_id: Optional[int] = None
    period_start: date
    period_end: date
    # Optional whitelist of section keys to generate (default: all in registry).
    sections: Optional[List[str]] = None
    generate_narratives: bool = True
    # Digest cadence hint (EPR-87): how the period was chosen, for labelling.
    period_type: Optional[str] = None

    @model_validator(mode="after")
    def _check_scope_and_period(self) -> "ReportCreate":
        if (self.windfarm_id is None) == (self.portfolio_id is None):
            raise ValueError("Provide exactly one of windfarm_id or portfolio_id")
        if self.period_start > self.period_end:
            raise ValueError("period_start must be on or before period_end")
        if self.period_type is not None and self.period_type not in (
            "monthly",
            "quarterly",
            "annual",
        ):
            raise ValueError("period_type must be one of monthly, quarterly, annual")
        return self


class ReportSectionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    section_key: str
    title: str = ""  # filled from registry metadata
    kind: str = ""  # payload kind, filled from registry metadata
    ai_enabled: bool = False  # filled from registry metadata
    pass_number: int
    display_order: int
    layout: str
    status: str
    data: Optional[dict] = None
    narrative_json: Optional[dict] = None
    narrative_text: Optional[str] = None
    error: Optional[str] = None
    generated_at: Optional[datetime] = None
    cost_usd: Optional[float] = None


class ScopeMeta(BaseModel):
    name: str
    capacity_mw: Optional[float] = None
    bidzone: Optional[str] = None
    country: Optional[str] = None
    owner: Optional[str] = None


class ReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    report_type: str
    scope_type: str
    windfarm_id: Optional[int] = None
    portfolio_id: Optional[int] = None
    scope_meta: Optional[ScopeMeta] = None
    period_start: date
    period_end: date
    version: int
    status: str
    title: str
    pdf_available: bool = False
    locked: bool = False
    frozen: bool = False  # exported (or locked) — content and PDF are immutable
    pdf_downloaded_at: Optional[datetime] = None
    requested_by_id: int
    requested_by_name: Optional[str] = None
    total_cost_usd: float = 0
    generation_started_at: Optional[datetime] = None
    generation_completed_at: Optional[datetime] = None
    error: Optional[str] = None
    created_at: datetime
    sections: List[ReportSectionResponse] = []


class ReportSummary(BaseModel):
    """Library row — no section payloads."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    report_type: str
    scope_type: str
    windfarm_id: Optional[int] = None
    portfolio_id: Optional[int] = None
    scope_meta: Optional[ScopeMeta] = None
    period_start: date
    period_end: date
    version: int
    status: str
    title: str
    pdf_available: bool = False
    locked: bool = False
    frozen: bool = False  # exported (or locked) — content and PDF are immutable
    pdf_downloaded_at: Optional[datetime] = None
    requested_by_id: int
    created_at: datetime


class SectionStatusItem(BaseModel):
    key: str
    status: str
    error: Optional[str] = None


class ReportStatusResponse(BaseModel):
    """Lightweight poll target while a report generates."""

    id: int
    status: str
    progress_pct: int
    sections: List[SectionStatusItem]
    total_cost_usd: float = 0


class SectionSpecMeta(BaseModel):
    key: str
    title: str
    kind: str
    pass_number: int
    layout: str
    ai_enabled: bool


class ReportTypeMeta(BaseModel):
    code: str
    title: str
    scope: str
    sections: List[SectionSpecMeta]


class ReportListResponse(BaseModel):
    items: List[ReportSummary]
    total: int


# Generic payload alias for section data (documented shape lives in the registry).
SectionData = dict[str, Any]
