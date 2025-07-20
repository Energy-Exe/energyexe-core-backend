from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class ControlAreaBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class ControlAreaCreate(ControlAreaBase):
    pass


class ControlAreaUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class ControlArea(ControlAreaBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True