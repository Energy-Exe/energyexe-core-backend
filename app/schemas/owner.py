from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class OwnerBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    type: Optional[str] = Field(None, pattern="^(private_equity|utility|oil_and_gas|investment_fund)$")
    notes: Optional[str] = None


class OwnerCreate(OwnerBase):
    pass


class OwnerUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=50)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    type: Optional[str] = Field(None, pattern="^(private_equity|utility|oil_and_gas|investment_fund)$")
    notes: Optional[str] = None


class Owner(OwnerBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True