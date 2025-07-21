from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class SubstationBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    owner_id: Optional[int] = None
    substation_type: Optional[str] = Field(None, pattern="^(substation|converter)$")
    lat: float
    lng: float
    current_type: Optional[str] = Field(None, pattern="^(ac|dc)$")
    array_cable_voltage_kv: Optional[int] = None
    export_cable_voltage_kv: Optional[int] = None
    transformer_capacity_mva: Optional[int] = None
    commissioning_date: Optional[date] = None
    operational_date: Optional[date] = None
    notes: Optional[str] = Field(None, max_length=300)
    address: Optional[str] = None
    postal_code: Optional[str] = Field(None, max_length=20)


class SubstationCreate(SubstationBase):
    pass


class SubstationUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    owner_id: Optional[int] = None
    substation_type: Optional[str] = Field(None, pattern="^(substation|converter)$")
    lat: Optional[float] = None
    lng: Optional[float] = None
    current_type: Optional[str] = Field(None, pattern="^(ac|dc)$")
    array_cable_voltage_kv: Optional[int] = None
    export_cable_voltage_kv: Optional[int] = None
    transformer_capacity_mva: Optional[int] = None
    commissioning_date: Optional[date] = None
    operational_date: Optional[date] = None
    notes: Optional[str] = Field(None, max_length=300)
    address: Optional[str] = None
    postal_code: Optional[str] = Field(None, max_length=20)


class Substation(SubstationBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
