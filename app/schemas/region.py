from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class RegionBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    location_type: str = Field(..., pattern="^(sea|land|combined)$")
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class RegionCreate(RegionBase):
    pass


class RegionUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    location_type: Optional[str] = Field(None, pattern="^(sea|land|combined)$")
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class Region(RegionBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
