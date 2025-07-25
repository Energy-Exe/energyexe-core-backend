from datetime import datetime
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from app.schemas.country import Country


class MarketBalanceAreaBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    country_id: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class MarketBalanceAreaCreate(MarketBalanceAreaBase):
    pass


class MarketBalanceAreaUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    country_id: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class MarketBalanceArea(MarketBalanceAreaBase):
    id: int
    created_at: datetime
    updated_at: datetime
    country: Optional["Country"] = None

    model_config = ConfigDict(from_attributes=True)
