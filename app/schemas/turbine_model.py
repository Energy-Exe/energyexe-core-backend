from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class TurbineModelBase(BaseModel):
    model: str = Field(..., min_length=1, max_length=50)
    supplier: str = Field(..., min_length=1, max_length=50)
    original_supplier: str = Field(..., min_length=1, max_length=50)
    rated_power_kw: Optional[int] = None
    rotor_diameter_m: Optional[Decimal] = None
    cut_in_wind_speed_ms: Optional[Decimal] = None
    cut_out_wind_speed_ms: Optional[Decimal] = None
    rated_wind_speed_ms: Optional[Decimal] = None
    blade_length_m: Optional[Decimal] = None
    generator_type: Optional[str] = Field(None, pattern="^(direct_drive|geared|hybrid)$")


class TurbineModelCreate(TurbineModelBase):
    pass


class TurbineModelUpdate(BaseModel):
    model: Optional[str] = Field(None, min_length=1, max_length=50)
    supplier: Optional[str] = Field(None, min_length=1, max_length=50)
    original_supplier: Optional[str] = Field(None, min_length=1, max_length=50)
    rated_power_kw: Optional[int] = None
    rotor_diameter_m: Optional[Decimal] = None
    cut_in_wind_speed_ms: Optional[Decimal] = None
    cut_out_wind_speed_ms: Optional[Decimal] = None
    rated_wind_speed_ms: Optional[Decimal] = None
    blade_length_m: Optional[Decimal] = None
    generator_type: Optional[str] = Field(None, pattern="^(direct_drive|geared|hybrid)$")


class TurbineModel(TurbineModelBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
