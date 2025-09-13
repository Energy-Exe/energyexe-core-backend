"""Schemas for generation data API."""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class GenerationDataResponse(BaseModel):
    """Response schema for generation data."""
    
    hour: datetime
    generation_mwh: float
    generation_unit_id: Optional[int] = None
    windfarm_id: Optional[int] = None
    source: str
    source_resolution: Optional[str] = None
    quality_score: Optional[float] = None
    quality_flag: Optional[str] = None
    is_manual_override: bool = False
    
    class Config:
        from_attributes = True


class RawDataResponse(BaseModel):
    """Response schema for raw generation data."""
    
    id: int
    source: str
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    period_type: Optional[str] = None
    identifier: Optional[str] = None
    value: Optional[float] = None
    unit: Optional[str] = None
    data: Dict[str, Any]
    created_at: datetime
    
    class Config:
        from_attributes = True


class ProcessingRequest(BaseModel):
    """Request schema for processing generation data."""
    
    source: str = Field(..., description="Data source: ENTSOE, ELEXON, EIA, TAIPOWER")
    identifier: Optional[str] = Field(None, description="Source-specific identifier")
    generation_unit_id: Optional[int] = Field(None, description="Generation unit ID to map to")
    windfarm_id: Optional[int] = Field(None, description="Windfarm ID to map to")
    start_date: Optional[datetime] = Field(None, description="Start date for processing")
    end_date: Optional[datetime] = Field(None, description="End date for processing")


class ProcessingResponse(BaseModel):
    """Response schema for processing results."""
    
    success: bool
    raw_records_processed: int
    hourly_records_created: int
    message: Optional[str] = None
    period_range: Optional[Dict[str, datetime]] = None


class ManualOverrideRequest(BaseModel):
    """Request schema for manual override."""
    
    hour: datetime = Field(..., description="Hour to override")
    generation_unit_id: int = Field(..., description="Generation unit ID")
    source: str = Field(..., description="Data source")
    new_value: float = Field(..., description="New generation value in MWh")
    reason: str = Field(..., description="Reason for override")


class ManualOverrideResponse(BaseModel):
    """Response schema for manual override."""
    
    success: bool
    original_value: Optional[float] = None
    new_value: float
    hour: datetime
    message: Optional[str] = None


class ImportCSVResponse(BaseModel):
    """Response schema for CSV import."""
    
    success: bool
    records_imported: int
    source: str
    period_range: Optional[Dict[str, datetime]] = None
    message: Optional[str] = None


class StoreRawDataRequest(BaseModel):
    """Request schema for storing raw data."""
    
    source: str = Field(..., description="Data source: ENTSOE, ELEXON, EIA, TAIPOWER")
    data: List[Dict[str, Any]] = Field(..., description="Raw data records")
    source_type: str = Field("api", description="Source type: api, excel, csv")


class StoreRawDataResponse(BaseModel):
    """Response schema for storing raw data."""
    
    success: bool
    records_stored: int
    source: str
    message: Optional[str] = None