from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class CountryBase(BaseModel):
    code: str = Field(
        ..., min_length=3, max_length=3, description="ISO 3166-1 alpha-3 country code"
    )
    name: str = Field(..., min_length=1, max_length=255)
    lat: Optional[float] = Field(None, description="Latitude of country centroid")
    lng: Optional[float] = Field(None, description="Longitude of country centroid")
    polygon_wkt: Optional[str] = Field(None, description="Country boundary as WKT polygon string")


class CountryCreate(CountryBase):
    pass


class CountryUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=3, max_length=3)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class CountryInDBBase(CountryBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class Country(CountryInDBBase):
    pass


class CountryInDB(CountryInDBBase):
    pass
