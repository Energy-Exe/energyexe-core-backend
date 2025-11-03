from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, List, Optional

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from .windfarm_owner import WindfarmOwnerWithDetails


class WindfarmBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    country_id: int
    state_id: Optional[int] = None
    region_id: Optional[int] = None
    bidzone_id: Optional[int] = None
    market_balance_area_id: Optional[int] = None
    control_area_id: Optional[int] = None
    nameplate_capacity_mw: Optional[float] = None
    project_id: Optional[int] = None
    commercial_operational_date: Optional[date] = None
    first_power_date: Optional[date] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None
    foundation_type: Optional[str] = Field(None, max_length=100)
    location_type: Optional[str] = Field(None, pattern="^(onshore|offshore)$")
    status: Optional[str] = Field(None, pattern="^(operational|decommissioned|under_installation|expanded)$")
    notes: Optional[str] = Field(None, max_length=300)
    alternate_name: Optional[str] = Field(None, max_length=255)
    environmental_assessment_status: Optional[str] = Field(None, max_length=100)
    permits_obtained: bool = False
    grid_connection_status: Optional[str] = Field(None, max_length=100)
    total_investment_amount: Optional[Decimal] = None
    investment_currency: Optional[str] = Field(None, max_length=3)
    address: Optional[str] = None
    postal_code: Optional[str] = Field(None, max_length=20)


class WindfarmCreate(WindfarmBase):
    pass


class WindfarmUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    country_id: Optional[int] = None
    state_id: Optional[int] = None
    region_id: Optional[int] = None
    bidzone_id: Optional[int] = None
    market_balance_area_id: Optional[int] = None
    control_area_id: Optional[int] = None
    nameplate_capacity_mw: Optional[float] = None
    project_id: Optional[int] = None
    commercial_operational_date: Optional[date] = None
    first_power_date: Optional[date] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    polygon_wkt: Optional[str] = None
    foundation_type: Optional[str] = Field(None, max_length=100)
    location_type: Optional[str] = Field(None, pattern="^(onshore|offshore)$")
    status: Optional[str] = Field(None, pattern="^(operational|decommissioned|under_installation|expanded)$")
    notes: Optional[str] = Field(None, max_length=300)
    alternate_name: Optional[str] = Field(None, max_length=255)
    environmental_assessment_status: Optional[str] = Field(None, max_length=100)
    permits_obtained: Optional[bool] = None
    grid_connection_status: Optional[str] = Field(None, max_length=100)
    total_investment_amount: Optional[Decimal] = None
    investment_currency: Optional[str] = Field(None, max_length=3)
    address: Optional[str] = None
    postal_code: Optional[str] = Field(None, max_length=20)


class Windfarm(WindfarmBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class OwnerSummary(BaseModel):
    id: int
    name: str
    ownership_percentage: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class CountrySummary(BaseModel):
    id: int
    code: str
    name: str

    model_config = ConfigDict(from_attributes=True)


class WindfarmListItem(Windfarm):
    country: Optional[CountrySummary] = None
    owners: List[OwnerSummary] = []

    model_config = ConfigDict(from_attributes=True)


class WindfarmWithOwners(Windfarm):
    windfarm_owners: List[dict] = []

    model_config = ConfigDict(from_attributes=True)


class WindfarmCreateWithOwners(BaseModel):
    windfarm: WindfarmCreate
    owners: List[dict] = Field(
        ..., description="List of {owner_id: int, ownership_percentage: Decimal}"
    )

    model_config = ConfigDict(from_attributes=True)
