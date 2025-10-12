"""Schemas for external data sources API."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


# EIA Schemas
class EIAFetchRequest(BaseModel):
    """Request schema for fetching EIA data."""
    windfarm_ids: List[int] = Field(..., description="List of windfarm IDs")
    start_year: int = Field(..., description="Start year")
    start_month: int = Field(..., ge=1, le=12, description="Start month (1-12)")
    end_year: int = Field(..., description="End year")
    end_month: int = Field(..., ge=1, le=12, description="End month (1-12)")


class EIAFetchResponse(BaseModel):
    """Response schema for EIA data fetch."""
    success: bool
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    message: Optional[str] = None


# ENTSOE Schemas
class ENTSOEFetchRequest(BaseModel):
    """Request schema for fetching ENTSOE data."""
    windfarm_ids: List[int] = Field(..., description="List of windfarm IDs")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")
    production_types: List[str] = Field(default=["wind"], description="Production types (wind, solar)")
    eic_codes: Optional[List[str]] = Field(None, description="Optional EIC codes to filter specific units")
    # area_code is no longer needed - automatically detected from windfarm bidzones


class ENTSOEFetchResponse(BaseModel):
    """Response schema for ENTSOE data fetch."""
    success: bool
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    message: Optional[str] = None


# ELEXON Schemas
class ELEXONFetchRequest(BaseModel):
    """Request schema for fetching ELEXON data."""
    windfarm_ids: List[int] = Field(..., description="List of windfarm IDs")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")


class ELEXONFetchResponse(BaseModel):
    """Response schema for ELEXON data fetch."""
    success: bool
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    message: Optional[str] = None


# TAIPOWER Schemas
class TAIPOWERFetchRequest(BaseModel):
    """Request schema for fetching TAIPOWER data."""
    windfarm_ids: List[int] = Field(..., description="List of windfarm IDs")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")


class TAIPOWERFetchResponse(BaseModel):
    """Response schema for TAIPOWER data fetch."""
    success: bool
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    message: Optional[str] = None


# NVE Schemas
class NVEFetchRequest(BaseModel):
    """Request schema for fetching NVE data."""
    windfarm_ids: List[int] = Field(..., description="List of windfarm IDs")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")


class NVEFetchResponse(BaseModel):
    """Response schema for NVE data fetch."""
    success: bool
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    message: Optional[str] = None


# ENERGISTYRELSEN Schemas
class ENERGISTYRELSENFetchRequest(BaseModel):
    """Request schema for fetching ENERGISTYRELSEN data."""
    windfarm_ids: List[int] = Field(..., description="List of windfarm IDs")
    start_date: datetime = Field(..., description="Start datetime")
    end_date: datetime = Field(..., description="End datetime")


class ENERGISTYRELSENFetchResponse(BaseModel):
    """Response schema for ENERGISTYRELSEN data fetch."""
    success: bool
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    message: Optional[str] = None


# Source metadata schemas
class SourceInfo(BaseModel):
    """Information about a data source."""
    code: str
    name: str
    description: str
    country: Optional[str] = None
    status: str = "active"
    requires_api_key: bool = True


class SourcesListResponse(BaseModel):
    """Response schema for listing available sources."""
    sources: List[SourceInfo]
