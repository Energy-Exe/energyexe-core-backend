from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class MarketBalanceAreaBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class MarketBalanceAreaCreate(MarketBalanceAreaBase):
    pass


class MarketBalanceAreaUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class MarketBalanceArea(MarketBalanceAreaBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True