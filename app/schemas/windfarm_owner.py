from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class WindfarmOwnerBase(BaseModel):
    windfarm_id: int
    owner_id: int
    ownership_percentage: Decimal = Field(..., ge=0, le=100, decimal_places=2)


class WindfarmOwnerCreate(WindfarmOwnerBase):
    pass


class WindfarmOwnerUpdate(BaseModel):
    windfarm_id: Optional[int] = None
    owner_id: Optional[int] = None
    ownership_percentage: Optional[Decimal] = Field(None, ge=0, le=100, decimal_places=2)


class WindfarmOwner(WindfarmOwnerBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class WindfarmOwnerWithDetails(WindfarmOwner):
    owner: dict

    model_config = ConfigDict(from_attributes=True)