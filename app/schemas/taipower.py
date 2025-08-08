"""Pydantic schemas for Taipower integration."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


from pydantic import validator

class TaipowerGenerationUnit(BaseModel):
    """Schema for a single Taipower generation unit."""
    
    generation_type: str = Field(..., alias="機組類型")
    unit_name: str = Field(..., alias="機組名稱")
    installed_capacity_mw: float = Field(..., alias="裝置容量(MW)")
    net_generation_mw: float = Field(..., alias="淨發電量(MW)")
    capacity_utilization_percent: float = Field(..., alias="淨發電量/裝置容量比(%)")
    notes: Optional[str] = Field(None, alias="備註")
    
    @validator('installed_capacity_mw', 'net_generation_mw', pre=True)
    def parse_mw_value(cls, v):
        """Parse MW values, handling subtotal format like '951.0(1.644%)'."""
        if isinstance(v, str):
            # Remove parenthetical percentage if present
            if '(' in v:
                v = v.split('(')[0]
            try:
                return float(v)
            except ValueError:
                return 0.0
        return float(v)
    
    @validator('capacity_utilization_percent', pre=True)
    def parse_percent_value(cls, v):
        """Parse percentage values like '94.162%'."""
        if isinstance(v, str):
            # Remove % sign if present
            v = v.replace('%', '').strip()
            if not v:  # Empty string for subtotals
                return 0.0
            try:
                return float(v)
            except ValueError:
                return 0.0
        return float(v)
    
    class Config:
        populate_by_name = True


class TaipowerDataResponse(BaseModel):
    """Schema for Taipower API response."""
    
    datetime: datetime
    generation_units: List[TaipowerGenerationUnit] = Field(..., alias="aaData")
    
    class Config:
        populate_by_name = True


class TaipowerGenerationDataRequest(BaseModel):
    """Request schema for fetching Taipower generation data."""
    
    windfarm_id: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class TaipowerGenerationDataPoint(BaseModel):
    """Schema for a single data point in the response."""
    
    timestamp: datetime
    generation_type: str
    unit_name: str
    installed_capacity_mw: float
    net_generation_mw: float
    capacity_utilization_percent: float
    notes: Optional[str] = None
    generation_unit_id: Optional[int] = None
    generation_unit_code: Optional[str] = None


class TaipowerGenerationDataResponse(BaseModel):
    """Response schema for Taipower generation data."""
    
    success: bool
    data: List[TaipowerGenerationDataPoint]
    metadata: Dict[str, Any]
    windfarm_id: Optional[int] = None
    windfarm_name: Optional[str] = None


class TaipowerLiveDataResponse(BaseModel):
    """Response schema for live Taipower data."""
    
    success: bool
    timestamp: datetime
    total_generation_mw: float
    generation_by_type: Dict[str, float]
    units: List[TaipowerGenerationDataPoint]
    metadata: Dict[str, Any]