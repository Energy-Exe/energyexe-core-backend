from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class SubstationBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
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


class LinkedWindfarmSummary(BaseModel):
    """Summary of a windfarm linked to a substation."""
    id: int
    code: str
    name: str
    nameplate_capacity_mw: Optional[float] = None
    status: Optional[str] = None
    location_type: Optional[str] = None

    class Config:
        from_attributes = True


class SubstationWithOwners(Substation):
    substation_owners: List[dict] = []

    class Config:
        from_attributes = True


class SubstationWithWindfarms(Substation):
    """Substation with linked windfarms."""
    substation_owners: List[dict] = []
    windfarms: List[LinkedWindfarmSummary] = []

    class Config:
        from_attributes = True


class SubstationCreateWithOwners(BaseModel):
    substation: SubstationCreate
    owners: List[dict] = []
