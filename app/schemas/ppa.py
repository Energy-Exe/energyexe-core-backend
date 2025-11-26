from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class PPABase(BaseModel):
    """Base schema for PPA with common fields."""

    windfarm_id: int
    ppa_buyer: str = Field(..., min_length=1, max_length=255)
    ppa_size_mw: Optional[Decimal] = Field(None, gt=0, decimal_places=2)
    ppa_duration_years: Optional[int] = Field(None, ge=0)
    ppa_start_date: Optional[date] = None
    ppa_end_date: Optional[date] = None
    ppa_notes: Optional[str] = Field(None, max_length=200)


class PPACreate(PPABase):
    """Schema for creating a new PPA."""

    pass


class PPAUpdate(BaseModel):
    """Schema for updating a PPA. All fields are optional."""

    windfarm_id: Optional[int] = None
    ppa_buyer: Optional[str] = Field(None, min_length=1, max_length=255)
    ppa_size_mw: Optional[Decimal] = Field(None, gt=0, decimal_places=2)
    ppa_duration_years: Optional[int] = Field(None, ge=0)
    ppa_start_date: Optional[date] = None
    ppa_end_date: Optional[date] = None
    ppa_notes: Optional[str] = Field(None, max_length=200)


class PPA(PPABase):
    """Schema for PPA response with database fields."""

    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Embedded schema to avoid circular imports
class WindfarmBasic(BaseModel):
    """Basic windfarm info for embedding in PPA responses."""

    id: int
    code: str
    name: str

    model_config = ConfigDict(from_attributes=True)


class PPAWithWindfarm(PPA):
    """PPA response with nested windfarm details."""

    windfarm: Optional[WindfarmBasic] = None

    model_config = ConfigDict(from_attributes=True)


class PPAImportRow(BaseModel):
    """Schema for a single row in PPA Excel import."""

    windfarm_name: str = Field(..., min_length=1, max_length=255)
    ppa_buyer: str = Field(..., min_length=1, max_length=255)
    ppa_size_mw: Optional[Decimal] = Field(None, gt=0)
    ppa_duration_years: Optional[int] = Field(None, ge=0)
    ppa_start_date: Optional[date] = None
    ppa_end_date: Optional[date] = None
    ppa_notes: Optional[str] = Field(None, max_length=200)


class PPAImportError(BaseModel):
    """Schema for import error details."""

    row: int
    field: Optional[str] = None
    value: Optional[str] = None
    message: str


class PPAImportResult(BaseModel):
    """Schema for PPA import operation result."""

    success: bool
    total_rows: int
    created: int
    updated: int
    skipped: int
    errors: List[PPAImportError] = []
    unmatched_windfarms: List[str] = []


class PPAListResponse(BaseModel):
    """Schema for paginated PPA list response."""

    items: List[PPAWithWindfarm]
    total: int
    limit: int
    offset: int
    has_more: bool
