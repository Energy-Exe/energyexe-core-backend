"""Generation unit schemas."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field

from app.schemas.windfarm import Windfarm


class GenerationUnitBase(BaseModel):
    """Base schema for generation unit."""

    code: str = Field(..., min_length=1, max_length=50, description="Internal unique identifier")
    name: str = Field(..., min_length=1, max_length=255, description="Name of the generation unit")
    source: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Data source (ENTSOE, ELEXON, NORDPOOL, OTHER)",
    )
    fuel_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Fuel type (wind, solar, gas, coal, nuclear, hydro, biomass, oil, other)",
    )
    technology_type: Optional[str] = Field(
        None, max_length=100, description="Specific technology type"
    )
    capacity_mw: Optional[Decimal] = Field(None, description="Installed capacity in MW")
    windfarm_id: Optional[int] = Field(
        None, description="Link to windfarm table if this is a wind generation unit"
    )
    start_date: Optional[date] = Field(None, description="Start/commissioning date of the generation unit")
    end_date: Optional[date] = Field(None, description="End/decommissioning date of the generation unit")
    notes: Optional[str] = Field(None, description="Additional notes")


class GenerationUnitCreate(GenerationUnitBase):
    """Schema for creating a generation unit."""

    pass


class GenerationUnitUpdate(BaseModel):
    """Schema for updating a generation unit."""

    code: Optional[str] = Field(
        None, min_length=1, max_length=50, description="Internal unique identifier"
    )
    name: Optional[str] = Field(
        None, min_length=1, max_length=255, description="Name of the generation unit"
    )
    source: Optional[str] = Field(None, min_length=1, max_length=50, description="Data source")
    fuel_type: Optional[str] = Field(None, min_length=1, max_length=100, description="Fuel type")
    technology_type: Optional[str] = Field(
        None, max_length=100, description="Specific technology type"
    )
    capacity_mw: Optional[Decimal] = Field(None, description="Installed capacity in MW")
    windfarm_id: Optional[int] = Field(None, description="Link to windfarm table")
    start_date: Optional[date] = Field(None, description="Start/commissioning date of the generation unit")
    end_date: Optional[date] = Field(None, description="End/decommissioning date of the generation unit")
    notes: Optional[str] = Field(None, description="Additional notes")


class GenerationUnitResponse(GenerationUnitBase):
    """Schema for generation unit response."""

    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class GenerationUnitSearchParams(BaseModel):
    """Schema for generation unit search parameters."""

    search: Optional[str] = Field(None, description="Search term for name or code")
    source: Optional[str] = Field(None, description="Filter by data source")
    fuel_type: Optional[str] = Field(None, description="Filter by fuel type")
    technology_type: Optional[str] = Field(None, description="Filter by technology type")
    is_active: Optional[bool] = Field(True, description="Filter by active status")
    limit: int = Field(100, ge=1, le=10000, description="Maximum number of results")
    offset: int = Field(0, ge=0, description="Number of results to skip")


class GenerationUnitWithWindfarm(GenerationUnitResponse):
    """Schema for generation unit response with windfarm details."""

    windfarm: Optional[Windfarm] = None
