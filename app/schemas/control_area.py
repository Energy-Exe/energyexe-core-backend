from datetime import datetime
from typing import Optional, TYPE_CHECKING
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from app.schemas.country import Country


class ControlAreaBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    country_id: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class ControlAreaCreate(ControlAreaBase):
    pass


class ControlAreaUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    country_id: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class ControlArea(ControlAreaBase):
    id: int
    created_at: datetime
    updated_at: datetime
    country: Optional["Country"] = None

    model_config = ConfigDict(from_attributes=True)