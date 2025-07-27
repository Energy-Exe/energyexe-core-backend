from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


# Embedded schemas to avoid circular imports
class WindfarmBasic(BaseModel):
    id: int
    code: str
    name: str

    class Config:
        from_attributes = True


class TurbineModelBasic(BaseModel):
    id: int
    model: str
    supplier: str
    rated_power_kw: Optional[int] = None

    class Config:
        from_attributes = True


class TurbineUnitBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    windfarm_id: int
    turbine_model_id: int
    lat: float
    lng: float
    status: Optional[str] = Field(None, pattern="^(operational|installing|decommissioned)$")
    hub_height_m: Optional[Decimal] = None


class TurbineUnitCreate(TurbineUnitBase):
    pass


class TurbineUnitUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    windfarm_id: Optional[int] = None
    turbine_model_id: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    status: Optional[str] = Field(None, pattern="^(operational|installing|decommissioned)$")
    hub_height_m: Optional[Decimal] = None


class TurbineUnit(TurbineUnitBase):
    id: int
    created_at: datetime
    updated_at: datetime
    windfarm: Optional[WindfarmBasic] = None
    turbine_model: Optional[TurbineModelBasic] = None

    class Config:
        from_attributes = True
