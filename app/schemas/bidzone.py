from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class BidzoneBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class BidzoneCreate(BidzoneBase):
    pass


class BidzoneUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class Bidzone(BidzoneBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True