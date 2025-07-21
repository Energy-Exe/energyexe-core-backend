from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

from app.schemas.country import Country


class BidzoneBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    bidzone_type: Optional[str] = Field(
        None, max_length=50
    )  # "national", "regional", "interconnector", "virtual"
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None


class BidzoneCreate(BidzoneBase):
    country_ids: Optional[List[int]] = []  # List of country IDs to associate with this bidzone


class BidzoneUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    bidzone_type: Optional[str] = Field(None, max_length=50)
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None
    country_ids: Optional[List[int]] = None  # Update country associations


class Bidzone(BidzoneBase):
    id: int
    created_at: datetime
    updated_at: datetime
    countries: List[Country] = []  # List of associated countries

    class Config:
        from_attributes = True
