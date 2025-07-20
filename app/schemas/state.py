from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class StateBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    country_id: int = Field(..., description="ID of the country this state belongs to")
    lat: Optional[float] = Field(None, description="Latitude of state centroid")
    lng: Optional[float] = Field(None, description="Longitude of state centroid")
    polygon_wkt: Optional[str] = Field(None, description="State boundary as WKT polygon string")


class StateCreate(StateBase):
    pass


class StateUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    country_id: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class StateInDBBase(StateBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class State(StateInDBBase):
    pass


class StateInDB(StateInDBBase):
    pass


# State with country information
class StateWithCountry(StateInDBBase):
    country: Optional["Country"] = None

    model_config = ConfigDict(from_attributes=True)


# Forward reference resolution
from app.schemas.country import Country

StateWithCountry.model_rebuild()