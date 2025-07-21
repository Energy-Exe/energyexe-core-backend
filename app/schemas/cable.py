from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class CableBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    type: Optional[str] = Field(None, pattern="^(export|array|land|interconnector)$")
    owner_id: Optional[int] = None
    from_type: str = Field(..., pattern="^(turbine|substation|windfarm)$")
    from_id: int
    to_type: str = Field(..., pattern="^(turbine|substation|windfarm)$")
    to_id: int
    current_type: Optional[str] = Field(None, pattern="^(ac|dc)$")
    voltage_kv: Optional[int] = None
    length_km: Optional[Decimal] = None
    landing_point_lat: Optional[float] = None
    landing_point_lng: Optional[float] = None
    route_wkt: Optional[str] = None
    notes: Optional[str] = None


class CableCreate(CableBase):
    pass


class CableUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    type: Optional[str] = Field(None, pattern="^(export|array|land|interconnector)$")
    owner_id: Optional[int] = None
    from_type: Optional[str] = Field(None, pattern="^(turbine|substation|windfarm)$")
    from_id: Optional[int] = None
    to_type: Optional[str] = Field(None, pattern="^(turbine|substation|windfarm)$")
    to_id: Optional[int] = None
    current_type: Optional[str] = Field(None, pattern="^(ac|dc)$")
    voltage_kv: Optional[int] = None
    length_km: Optional[Decimal] = None
    landing_point_lat: Optional[float] = None
    landing_point_lng: Optional[float] = None
    route_wkt: Optional[str] = None
    notes: Optional[str] = None


class Cable(CableBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
