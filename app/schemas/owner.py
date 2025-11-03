from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from .windfarm_owner import WindfarmOwner


class OwnerBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    type: Optional[str] = Field(
        None, pattern="^(energy|institutional_investor|community_investors|municipality|private_individual|supply_chain_oem|other|unknown)$"
    )
    notes: Optional[str] = None


class OwnerCreate(OwnerBase):
    pass


class OwnerUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    type: Optional[str] = Field(
        None, pattern="^(energy|institutional_investor|community_investors|municipality|private_individual|supply_chain_oem|other|unknown)$"
    )
    notes: Optional[str] = None


class Owner(OwnerBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class OwnerWithWindfarms(Owner):
    windfarm_owners: List["WindfarmOwner"] = []

    class Config:
        from_attributes = True
