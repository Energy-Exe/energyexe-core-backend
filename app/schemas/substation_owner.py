from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field

from .owner import Owner


# Base schema with common fields
class SubstationOwnerBase(BaseModel):
    substation_id: int
    owner_id: int
    ownership_percentage: Decimal = Field(..., ge=0, le=100)


# Schema for creating a new substation owner relationship
class SubstationOwnerCreate(BaseModel):
    owner_id: int
    ownership_percentage: Decimal = Field(..., ge=0, le=100)


# Schema for updating a substation owner relationship
class SubstationOwnerUpdate(BaseModel):
    ownership_percentage: Decimal = Field(..., ge=0, le=100)


# Schema for reading a substation owner relationship (from database)
class SubstationOwner(SubstationOwnerBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# Schema for substation owner with owner details
class SubstationOwnerWithDetails(SubstationOwner):
    owner: Optional[Owner] = None

    model_config = {"from_attributes": True}
