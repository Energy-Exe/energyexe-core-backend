from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


ALLOWED_ENTITY_TYPES = {"spv", "holdco", "fund", "joint_venture", "other"}


class FinancialEntityBase(BaseModel):
    code: str = Field(..., min_length=1, max_length=100)
    name: str = Field(..., min_length=1, max_length=255)
    entity_type: str = Field(default="spv", max_length=50)
    registration_number: Optional[str] = Field(None, max_length=100)
    country_of_incorporation: Optional[str] = Field(None, max_length=100)
    parent_entity_id: Optional[int] = None
    notes: Optional[str] = None


class FinancialEntityCreate(FinancialEntityBase):
    pass


class FinancialEntityUpdate(BaseModel):
    code: Optional[str] = Field(None, min_length=1, max_length=100)
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    entity_type: Optional[str] = Field(None, max_length=50)
    registration_number: Optional[str] = Field(None, max_length=100)
    country_of_incorporation: Optional[str] = Field(None, max_length=100)
    parent_entity_id: Optional[int] = None
    notes: Optional[str] = None


class FinancialEntity(FinancialEntityBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class WindfarmBasic(BaseModel):
    id: int
    code: str
    name: str

    model_config = ConfigDict(from_attributes=True)


class WindfarmFinancialEntityLink(BaseModel):
    id: int
    windfarm_id: int
    financial_entity_id: int
    relationship_type: Optional[str] = None
    notes: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class WindfarmFinancialEntityWithWindfarm(WindfarmFinancialEntityLink):
    windfarm: Optional[WindfarmBasic] = None

    model_config = ConfigDict(from_attributes=True)


class FinancialEntityWithWindfarms(FinancialEntity):
    windfarm_financial_entities: List[WindfarmFinancialEntityWithWindfarm] = []

    model_config = ConfigDict(from_attributes=True)


class WindfarmLinkCreate(BaseModel):
    windfarm_id: int
    relationship_type: Optional[str] = None
    notes: Optional[str] = None


class FinancialEntityListResponse(BaseModel):
    items: List[FinancialEntity]
    total: int
    limit: int
    offset: int
    has_more: bool
